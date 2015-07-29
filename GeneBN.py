import random
import os
import multiprocessing
import time
import re
import sys
import getopt

opts,args=getopt.getopt(sys.argv[1:], "i:o:n:v:c:p:r:")
bootstrap_times=100
bootstrap_values=0.6
input_filename=''
output_BN_filename=''
cpus=2
per_data=1.0
removefile=''
for op, value in opts:
    if op == "-i":
        input_filename = value
    elif op == "-o":
        output_BN_filename = value
    elif op == "-n":
        bootstrap_times = int(value)
    elif op == "-v":
        bootstrap_values = int(value)
    elif op == "-c":
        cpus = int(value)
    elif op == "-p":
        per_data = float(value)
    elif op == "-r":
        removefile = value

def get_nodes(input_filename):
    final_list=[]
    for i in open(input_filename):
        if '[' in i and ']' in i:
            for j in re.findall('\[(.*?)\]',i):
                if '|' in j:
                    lin_list=j.split('|')
                    for k in lin_list[1].split(':'):
                        final_list.append(k+'	'+lin_list[0])
    return final_list

def multiple(n):
    fout_data=open(input_filename+'_data_'+str(n)+'.th','w')
    fout_Rfile=open(input_filename+'_r_'+str(n)+'.R','w')
    data_list=[]
    for i in open(input_filename):
        data_list.append(i.rstrip())
    fout_data.write(data_list.pop(0)+'\n')
    random.shuffle(data_list)
    for i in data_list[:int(len(data_list)*per_data)]:
        fout_data.write(i+'\n')
    fout_data.close()
    fout_Rfile.write('''#!/usr/bin/env Rscript
library(bnlearn)
x=read.table("'''+input_filename+'''_data_'''+str(n)+'''.th")
x=t(x)
x=data.frame(x)
res = hc(x, debug = FALSE)
sink("'''+input_filename+'''_result_'''+str(n)+'''.th")
res
sink()''')
    fout_Rfile.close()
    time.sleep(3)
    os.system('Rscript '+input_filename+'_r_'+str(n)+'.R')
    time.sleep(3)
    if 'no' not in removefile:
        os.system("rm "+input_filename+"_data_"+str(n)+".th")
        os.system("rm "+input_filename+"_r_"+str(n)+".R")

if __name__=='__main__':
    p = multiprocessing.Pool(processes=cpus)
    for n in range(bootstrap_times):
        p.apply_async(multiple, args=(n,))
    p.close()
    p.join()
    gene_pair={}
    for i in range(bootstrap_times):
        x_list=get_nodes(input_filename+"_result_"+str(i)+".th")
        for j in x_list:
            if j in gene_pair.keys():
                gene_pair[j]+=1
            else:
                gene_pair[j]=1
    fout_bootstrap_nodes=open(output_BN_filename,'w')
    for i in gene_pair.keys():
        if gene_pair[i]/bootstrap_times >= bootstrap_values:
            fout_bootstrap_nodes.write(i+'\n')
    fout_bootstrap_nodes.close()
    if 'no' not in removefile:
        os.system("rm "+input_filename+"_result_*.th")