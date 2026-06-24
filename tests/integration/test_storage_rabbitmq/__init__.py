import logging
import time

import pytest


def poll_direct_select_result(
    instance, query, is_result_complete, timeout, ignore_error=True, sleep_time=0.05
):
    result = ""
    previous_rows = 0
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        # Direct SELECT from RabbitMQ consumes rows, so accumulate all returned batches.
        result += instance.query(query, ignore_error=ignore_error)
        if is_result_complete(result):
            return result

        rows = len([line for line in result.splitlines() if line.strip()])
        if rows > previous_rows:
            deadline = time.monotonic() + timeout
        previous_rows = rows
        logging.debug(f"Collected rows: {rows}. Now {time.monotonic()}, deadline {deadline}")
        time.sleep(sleep_time)

    pytest.fail(
        f"Time limit of {timeout} seconds reached without RabbitMQ consumption progress. "
        "The result did not match the expected value."
    )
