import random
import string
import threading


# By default the exceptions that was throwed in threads will be ignored
# (they will not mark the test as failed, only printed to stderr).
# Wrap thrading.Thread and re-throw exception on join()
class SafeThread(threading.Thread):
    def __init__(self, target):
        super().__init__()
        self.target = target
        self.exception = None

    def run(self):
        try:
            self.target()
        except Exception as e:  # pylint: disable=broad-except
            self.exception = e

    def join(self, timeout=None):
        super().join(timeout)
        if self.exception:
            raise self.exception


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def replace_config(config_path, old, new):
    config = open(config_path, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(config_path, "w")
    config.writelines(config_lines)
    config.close()
