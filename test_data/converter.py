from __future__ import print_function

FILE="res_part1.tsv"
def convert():
	for line in open(FILE):
		numbers = line.split('\t')
		numbers2 = numbers[5].split('_')
		if numbers2[-2] == numbers2[-3]:
			numbers2[-2] = str(int(numbers2[-2]) + 1)
			numbers2[-3] = str(int(numbers2[-3]) + 1)
			numbers[5] = '_'.join(numbers2[1:])
			print('\t'.join(numbers), end='')
		else:
			print(line, end='')

convert()
			
