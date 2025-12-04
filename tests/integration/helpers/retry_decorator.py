import random
import time
from typing import Any, Callable


def retry(
    *exceptions: BaseException,
    retries: int = 5,
    delay: float = 1,
    backoff: float = 1.5,
    jitter: float = 2,
    # should take **kwargs or arguments: `retry_number`, `exception` and `sleep_time`
    log_function: Callable[[int, Exception, float], None] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., None]]:
    exceptions_tuple: tuple[type[BaseException], ...] = exceptions or (
        Exception,)

    def inner(func: Callable[..., Any], *args: Any, **kwargs: Any) -> None:
        current_delay: float = delay
        for retry in range(retries):
            try:
                func(*args, **kwargs)
                break
            except Exception as e:
                should_retry: bool = (retry < retries - 1) and any(
                    isinstance(e, re) for re in exceptions_tuple
                )
                if not should_retry:
                    raise e
                sleep_time: float = current_delay + random.uniform(0, jitter)
                if log_function is not None:
                    log_function(retry_number=retry, exception=e, sleep_time=sleep_time)
                time.sleep(sleep_time)
                current_delay *= backoff

    return inner
