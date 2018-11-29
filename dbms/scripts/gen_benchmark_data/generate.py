import argparse

from model import Model
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('-n', type=int, default=100000,
                    help='number of objects to generate')
parser.add_argument('--output_file', type=str, default='out.tsv',
                    help='output file name')
parser.add_argument('--weights_path', type=str,
                    help='path to weights')


args = parser.parse_args()

if __name__ == '__main__':
    if not args.weights_path:
        raise Exception('please specify path to model weights with --weights_path')

    gen = Model()
    gen.generate(args.n, args.output_file, args.weights_path)

