# -*- coding: utf-8 -*-
import csv
import glob
import sys
import mincemeat

path = '/home/leandro/Documents/puc/06-solutions/activities/map-reduce-words-higher-occurrence/'
text_files = glob.glob(path + 'data/*')

# Retorna o conteúdo do arquivo.
def file_contents (file_name):
    f = open(file_name)
    try:
        return f.read()
    finally:
        f.close()

# Lê cada linha do arquivo e transforma as palavras em uma estrutura "chave/valor".
# Retorna somente palavras utilizadas por 'Grzegorz Rozenberg' e/ou 'Philip S. Yu'.
def mapfn (k, v):
    # print 'map ' + k
    from stopwords import allStopWords
    for line in v.splitlines():
        fields = line.split(':::')
        authors = fields[1]
        title = fields[2]
        validBook = ''
        for word in title.split():
            word = word.replace('.', '').replace(',', '').replace(':', '').replace('?', '').replace('(', '').replace(')', '').replace('"', '').replace("''", '')
            if (word not in allStopWords):
                for author in authors.split('::'):
                    if author == 'Grzegorz Rozenberg' or author == 'Philip S. Yu':
                        validBook = 'author: ' + author + '; title: ' + title
                        yield word, author
        if validBook != '': print validBook

# Soma a quantidade de ocorrências das palavras utilizadas por 'Grzegorz Rozenberg' e 'Philip S. Yu'.
# Retorna somente palavras utilizadas por mais de 6 vezes.
def reducefn (k, v):
    # print 'reduce ' + k
    authors = ''
    totalPhilip = totalRozenberg = 0
    hasPhilip = hasRozenberg = False

    for index, item in enumerate(v):
        if item == 'Grzegorz Rozenberg':
            hasRozenberg = True
            totalRozenberg += 1
        if item == 'Philip S. Yu':
            hasPhilip = True
            totalPhilip += 1

    if totalPhilip > 5 or totalRozenberg > 5:
        L = list()
        authors += 'Grzegorz Rozenberg: ' + str(totalRozenberg) + '; ' if hasRozenberg else ''
        authors += 'Philip S. Yu: ' + str(totalPhilip) if hasPhilip else ''
        L.append(authors)
        return L
    else:
        return None

# Transforma todos os arquivos em uma estrutura de "chave/valor" (file_name/file_content).
source = dict( (file_name, file_contents(file_name)) for file_name in text_files )

s = mincemeat.Server()

s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="mapreduce")

# Apresenta o resultado em um arquivo CSV.
w = csv.writer( open(path + 'result.csv', 'w') )

for k, v in results.items():
    if v != None:
        w.writerow([k, v])

# Name Node
# python words-higher-occurrence.py

# Data Nodes
# python mincemeat.py -p mapreduce localhost