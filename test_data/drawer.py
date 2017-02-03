from __future__ import print_function

import matplotlib.pyplot as plt
import ast

FILE="test.tsv"
def convert():
	place = dict()
	max_coord = 0
	for line in open(FILE):
		numbers = line.split('\t')
		if len(numbers) <= 2:
			continue 
		name = numbers[5]
		if numbers[0] == '1':
			max_coord += 1
			place[name] = [1, max_coord, 1]
	for line in open(FILE):
		numbers = line.split('\t')
		if len(numbers) <= 2:
			continue
		name = numbers[5]
		if numbers[0] == '2':
			list = ast.literal_eval(numbers[-1])
			coord = [0,0,0]
			for cur_name in list:
				coord[0] = max(place[cur_name][0], coord[0])
				coord[1] += place[cur_name][1] * place[cur_name][2]
				coord[2] += place[cur_name][2]
			coord[1] /= coord[2]
			coord[0] += 1
			place[name] = coord
			for cur_name in list:
				plt.plot([coord[1], place[cur_name][1]],[coord[0], place[cur_name][0]])
	plt.show()	


convert()
			
