import random
import time
from typing import List, Type


def retry(
    retries: int = 5,
    delay: float = 1,
    backoff: float = 1.5,
    jitter: float = 2,
    log_function=None,  # should take **kwargs or arguments: `retry_number`, `exception` and `sleep_time`
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
                    sleep_time = current_delay + random.uniform(0, jitter)
                    if log_function is not None:
                        log_function(
                            retry_number=retry, exception=e, sleep_time=sleep_time
                        )
                    time.sleep(sleep_time)
                    current_delay *= backoff

        return wrapper

    return inner
