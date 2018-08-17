import argparse

from model import Model
parser = argparse.ArgumentParser(
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--n_iter', type=int, default=10000,
                    help='number of iterations')
parser.add_argument('--save_dir', type=str, default='save',
                    help='dir for saving weights')
parser.add_argument('--data_path', type=str,
                    help='path to train data')
parser.add_argument('--learning_rate', type=int, default=0.0001,
                    help='learning rate')
parser.add_argument('--batch_size', type=int, default=64,
                    help='batch size')
parser.add_argument('--restore_from', type=str,
                    help='path to train saved weights')

args = parser.parse_args()

if __name__ == '__main__':
    if not args.data_path:
        raise Exception('please specify path to train data with --data_path')

    gen = Model(args.learning_rate)
    gen.train(args.data_path, args.save_dir, args.n_iter, args.batch_size, args.restore_from)
