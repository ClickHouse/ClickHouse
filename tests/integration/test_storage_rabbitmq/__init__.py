import logging
import time

import pytest


def poll_direct_select_result(
    instance, query, is_result_complete, timeout, ignore_error=True, sleep_time=0.05
):
    result = ""
    rows = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        # Direct SELECT from RabbitMQ consumes rows, so accumulate all returned batches.
        batch = instance.query(query, ignore_error=ignore_error)
        result += batch
        if is_result_complete(result):
            return result

        new_rows = sum(1 for line in batch.splitlines() if line.strip())
        if new_rows:
            rows += new_rows
            deadline = time.monotonic() + timeout
        logging.debug(f"Collected rows: {rows}. Now {time.monotonic()}, deadline {deadline}")
        time.sleep(sleep_time)

    pytest.fail(
        f"Time limit of {timeout} seconds reached without RabbitMQ consumption progress. "
        "The result did not match the expected value."
    )
