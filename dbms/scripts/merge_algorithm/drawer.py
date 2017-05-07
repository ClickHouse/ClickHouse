from __future__ import print_function

import argparse
import matplotlib.pyplot as plt
import ast

TMP_FILE='tmp.tsv'

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-f', '--file', default='data.tsv')
    cfg = parser.parse_args()
    return cfg

def draw():
    place = dict()
    max_coord = 0
    global_top = 0
    for line in open(TMP_FILE):
        numbers = line.split('\t')
        if len(numbers) <= 2:
            continue
        name = numbers[-2]
        if numbers[0] == '1':
            dx = int(numbers[3])
            max_coord += dx
            place[name] = [1, max_coord, 1, dx]
            max_coord += dx
            plt.plot([max_coord - 2 * dx, max_coord], [1, 1])
    for line in open(TMP_FILE):
        numbers = line.split('\t')
        if len(numbers) <= 2:
            continue
        name = numbers[-2]
        if numbers[0] == '2':
            list = ast.literal_eval(numbers[-1])
            coord = [0,0,0,0]
            for cur_name in list:
                coord[0] = max(place[cur_name][0], coord[0])
                coord[1] += place[cur_name][1] * place[cur_name][2]
                coord[2] += place[cur_name][2]
                coord[3] += place[cur_name][3]
            coord[1] /= coord[2]
            coord[0] += 1
            global_top = max(global_top, coord[0])
            place[name] = coord
            for cur_name in list:
                plt.plot([coord[1], place[cur_name][1]],[coord[0], place[cur_name][0]])
            plt.plot([coord[1] - coord[3], coord[1] + coord[3]], [coord[0], coord[0]])
    plt.plot([0], [global_top + 1])
    plt.plot([0], [-1])
    plt.show()


def convert(input_file):
    print(input_file)
    tmp_file = open(TMP_FILE, "w")
    for line in open(input_file):
        numbers = line.split('\t')
        numbers2 = numbers[-2].split('_')
        if numbers2[-2] == numbers2[-3]:
            numbers2[-2] = str(int(numbers2[-2]) + 1)
            numbers2[-3] = str(int(numbers2[-3]) + 1)
            numbers[-2] = '_'.join(numbers2[1:])
            print('\t'.join(numbers), end='', file=tmp_file)
        else:
            print(line, end='', file=tmp_file)

def main():
    cfg = parse_args()
    convert(cfg.file)
    draw()

if __name__ == '__main__':
    main()

