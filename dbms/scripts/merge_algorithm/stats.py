import time
import ast
from datetime import datetime

FILE='data.tsv'

def get_metrix():
	data = []
	time_to_merge = 0
	count_of_parts = 0
	max_count_of_parts = 0
	parts_in_time = []
	last_date = 0
	for line in open(FILE):
		fields = line.split('\t')
		last_date = datetime.strptime(fields[2], '%Y-%m-%d %H:%M:%S')
		break

	for line in open(FILE):
		fields = line.split('\t')
		cur_date = datetime.strptime(fields[2], '%Y-%m-%d %H:%M:%S')
		if fields[0] == '2':
			time_to_merge += int(fields[4])
			list = ast.literal_eval(fields[-1])
			count_of_parts -= len(list) - 1
		else:
			count_of_parts += 1

		if max_count_of_parts < count_of_parts:
			max_count_of_parts = count_of_parts

		parts_in_time.append([(cur_date-last_date).total_seconds(), count_of_parts])
		last_date = cur_date

	stats_parts_in_time = []
	global_time = 0
	average_parts = 0
	for i in range(max_count_of_parts + 1):
		stats_parts_in_time.append(0)

	for elem in parts_in_time:
		stats_parts_in_time[elem[1]] += elem[0]
		global_time += elem[0]
		average_parts += elem[0] * elem[1]

	for i in range(max_count_of_parts):
		stats_parts_in_time[i] /= global_time
	average_parts /= global_time

	return time_to_merge, max_count_of_parts, average_parts, stats_parts_in_time

def main():
   time_to_merge, max_parts, average_parts, stats_parts = get_metrix()
   print('time_to_merge=', time_to_merge)
   print('max_parts=', max_parts)
   print('average_parts=', average_parts)
   print('stats_parts=', stats_parts)


if __name__ == '__main__':
	main()
