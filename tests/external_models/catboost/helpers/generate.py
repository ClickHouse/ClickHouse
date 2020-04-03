import numpy as np


def generate_uniform_int_column(size, low, high, seed=0):
    np.random.seed(seed)
    return np.random.randint(low, high, size)


def generate_uniform_float_column(size, low, high, seed=0):
    np.random.seed(seed)
    return np.random.random(size) * (high - low) + low


def generate_uniform_string_column(size, samples, seed):
    return np.array(samples)[generate_uniform_int_column(size, 0, len(samples), seed)]
