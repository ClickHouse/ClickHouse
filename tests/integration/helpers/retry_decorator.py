import time
import random
from typing import Type, List


def retry(
    retries: int = 5,
    delay: float = 1,
    backoff: float = 1.5,
    jitter: float = 2,
    log_function=lambda *args, **kwargs: None,
    retriable_expections_list: List[Type[BaseException]] = [Exception],
):
    def inner(func):
        def wrapper(*args, **kwargs):
            current_delay = delay
            for retry in range(retries):
                try:
                    func(*args, **kwargs)
                    break
                except Exception as e:
                    should_retry = False
                    for retriable_exception in retriable_expections_list:
                        if isinstance(e, retriable_exception):
                            should_retry = True
                            break
                    if not should_retry or (retry == retries - 1):
                        raise e
                    log_function(retry=retry, exception=e)
                    sleep_time = current_delay + random.uniform(0, jitter)
                    time.sleep(sleep_time)
                    current_delay *= backoff

        return wrapper

    return inner
