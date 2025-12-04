import random
import time
from typing import Any, Callable


def retry(
    *exceptions: type[BaseException],
    retries: int = 5,
    delay: float = 1,
    backoff: float = 1.5,
    jitter: float = 2,
    # should take **kwargs or arguments: `retry_number`, `exception` and `sleep_time`
    log_function: Callable[[int, Exception, float], None] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    exceptions_tuple: tuple[type[BaseException], ...] = exceptions if exceptions else (
        Exception,)

    def inner(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        current_delay: float = delay
        for retry_num in range(retries):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                should_retry: bool = (retry_num < retries - 1) and any(
                    isinstance(e, exc_type) for exc_type in exceptions_tuple
                )
                if not should_retry:
                    raise e
                sleep_time: float = current_delay + random.uniform(0, jitter)
                if log_function is not None:
                    log_function(retry_num, e, sleep_time)
                time.sleep(sleep_time)
                current_delay *= backoff
        return None  # This should never be reached, but type checker needs it

    return inner
