import datetime
import random
import string
import threading
from typing import Any, Callable


# By default the exceptions that was throwed in threads will be ignored
# (they will not mark the test as failed, only printed to stderr).
# Wrap thrading.Thread and re-throw exception on join()
class SafeThread(threading.Thread):
    def __init__(self, target: Callable[[], None]) -> None:
        super().__init__()
        self.target: Callable[[], None] = target
        self.exception: Exception | None = None

    def run(self) -> None:
        try:
            self.target()
        except Exception as e:  # pylint: disable=broad-except
            self.exception = e

    def join(self, timeout: float | None = None) -> None:
        super().join(timeout)
        if self.exception:
            raise self.exception


def random_string(length: int) -> str:
    letters: str = string.ascii_letters
    return "".join(random.choice(letters) for _ in range(length))


def generate_values(date_str: str, count: int, sign: int = 1) -> str:
    data: list[list[str | int]] = [
        [date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def replace_config(config_path: str, old: str, new: str) -> None:
    # pyright: ignore[reportAny, reportExplicitAny]
    config: Any = open(config_path, "r")
    config_lines: list[str] = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(config_path, "w")
    config.writelines(config_lines)
    config.close()


# Prints the current time in UTC with microseconds.
def format_current_time() -> str:
    return datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')
