from __future__ import print_function

FILE="part_log4.tsv"
def converter():
  for line in open(FILE):
    numbers = line.split('\t')
    if int(numbers[3]) > 100000:
      print(line, end='')

if __name__ == '__main__':
  converter()

