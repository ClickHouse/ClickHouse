import numpy as np
import os
import pickle
import tensorflow as tf

from random import sample
from keras.layers import Dense, Embedding
from tqdm import tqdm

RNN_NUM_UNITS = 256
EMB_SIZE = 32
MAX_LENGTH = 1049


with open('tokens', 'rb') as f:
    tokens = pickle.load(f)
n_tokens = len(tokens)

token_to_id = {c: i for i, c in enumerate(tokens)}


def to_matrix(objects, max_len=None, pad=0, dtype='int32'):
    max_len = max_len or max(map(len, objects))
    matrix = np.zeros([len(objects), max_len], dtype) + pad

    for i in range(len(objects)):
        name_ix = list(map(token_to_id.get, objects[i]))
        matrix[i, :len(name_ix)] = name_ix
    return matrix.T


class Model:
    def __init__(self, learning_rate=0.0001):
        # an embedding layer that converts character ids into embeddings
        self.embed_x = Embedding(n_tokens, EMB_SIZE)
        get_h_next = Dense(1024, activation='relu')
        # a dense layer that maps current hidden state
        # to probabilities of characters [h_t+1]->P(x_t+1|h_t+1)
        self.get_probas = Dense(n_tokens, activation='softmax')

        self.input_sequence = tf.placeholder('int32', (MAX_LENGTH, None))
        batch_size = tf.shape(self.input_sequence)[1]

        self.gru_cell_first = tf.nn.rnn_cell.GRUCell(RNN_NUM_UNITS)
        self.lstm_cell_second = tf.nn.rnn_cell.LSTMCell(RNN_NUM_UNITS)

        h_prev_first = self.gru_cell_first.zero_state(batch_size, dtype=tf.float32)
        h_prev_second = tf.nn.rnn_cell.LSTMStateTuple(
            tf.zeros([batch_size, RNN_NUM_UNITS]),  # initial cell state,
            tf.zeros([batch_size, RNN_NUM_UNITS])  # initial hidden state
        )

        predicted_probas = []
        for t in range(MAX_LENGTH):
            x_t = self.input_sequence[t]
            # convert character id into embedding
            x_t_emb = self.embed_x(tf.reshape(x_t, [-1, 1]))[:, 0]

            out_next_first, h_next_first = self.gru_cell_first(x_t_emb, h_prev_first)
            h_prev_first = h_next_first

            out_next_second, h_next_second = self.lstm_cell_second(out_next_first, h_prev_second)
            h_prev_second = h_next_second

            probas_next = self.get_probas(out_next_second)
            predicted_probas.append(probas_next)

        predicted_probas = tf.stack(predicted_probas)

        predictions_matrix = tf.reshape(predicted_probas[:-1], [-1, len(tokens)])
        answers_matrix = tf.one_hot(tf.reshape(self.input_sequence[1:], [-1]), n_tokens)

        self.loss = tf.reduce_mean(tf.reduce_sum(
            -answers_matrix * tf.log(tf.clip_by_value(predictions_matrix, 1e-7, 1.0)),
            reduction_indices=[1]
        ))
        optimizer = tf.train.AdamOptimizer(learning_rate)
        gvs = optimizer.compute_gradients(self.loss)
        capped_gvs = [(gr if gr is None else tf.clip_by_value(gr, -1., 1.), var) for gr, var in gvs]
        self.optimize = optimizer.apply_gradients(capped_gvs)

        self.sess = tf.Session()
        self.sess.run(tf.global_variables_initializer())
        self.saver = tf.train.Saver()

    def train(self, train_data_path, save_dir, num_iters, batch_size=64, restore_from=False):
        history = []
        if restore_from:
            with open(restore_from + '_history') as f:
                history = pickle.load(f)
            self.saver.restore(self.sess, restore_from)
        with open(train_data_path, 'r') as f:
            train_data = f.readlines()

        train_data = filter(lambda a: len(a) < MAX_LENGTH, train_data)

        for i in tqdm(range(num_iters)):
            batch = to_matrix(
                map(lambda a: '\n' + a.rstrip('\n'), sample(train_data, batch_size)),
                max_len=MAX_LENGTH
            )
            loss_i, _ = self.sess.run([self.loss, self.optimize], {self.input_sequence: batch})
            history.append(loss_i)
            if len(history) % 2000 == 0:
                self.saver.save(self.sess, os.path.join(save_dir, '{}_iters'.format(len(history))))
        self.saver.save(self.sess, os.path.join(save_dir, '{}_iters'.format(len(history))))
        with open(os.path.join(save_dir, '{}_iters_history'.format(len(history)))) as f:
            pickle.dump(history, f)

    def generate(self, num_objects, output_file, weights_path):
        self.saver.restore(self.sess, weights_path)
        batch_size = num_objects
        x_t = tf.placeholder('int32', (None, batch_size))
        h_t_first = tf.Variable(tf.zeros([batch_size, RNN_NUM_UNITS]))
        h_t_second = tf.nn.rnn_cell.LSTMStateTuple(
            tf.Variable(tf.zeros([batch_size, RNN_NUM_UNITS])),
            tf.Variable(tf.zeros([batch_size, RNN_NUM_UNITS]))
        )

        x_t_emb = self.embed_x(tf.reshape(x_t, [-1, 1]))[:, 0]
        first_out_next, next_h_first = self.gru_cell_first(x_t_emb, h_t_first)
        second_out_next, next_h_second = self.lstm_cell_second(first_out_next, h_t_second)
        next_probs = self.get_probas(second_out_next)

        x_sequence = np.zeros(shape=(1, batch_size), dtype=int) + token_to_id['\n']
        self.sess.run(
            [tf.assign(h_t_first, h_t_first.initial_value),
             tf.assign(h_t_second[0], h_t_second[0].initial_value),
             tf.assign(h_t_second[1], h_t_second[1].initial_value)]
        )

        for i in tqdm(range(MAX_LENGTH - 1)):
            x_probs, _, _, _ = self.sess.run(
                [next_probs,
                 tf.assign(h_t_second[0], next_h_second[0]),
                 tf.assign(h_t_second[1], next_h_second[1]),
                 tf.assign(h_t_first, next_h_first)],
                {x_t: [x_sequence[-1, :]]}
            )

            next_char = [np.random.choice(n_tokens, p=x_probs[i]) for i in range(batch_size)]
            if sum(next_char) == 0:
                break
            x_sequence = np.append(x_sequence, [next_char], axis=0)

        with open(output_file, 'w') as f:
            f.writelines([''.join([tokens[ix] for ix in x_sequence.T[k]]) + '\n' for k in range(batch_size)])
