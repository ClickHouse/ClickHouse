import logging
import os
import sys
import time
import uuid

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import assert_eq_with_retry, wait_condition


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
FAILPOINT = "s3_read_buffer_pause_before_cancellation_check"
MOCK_PORT = 8081


@pytest.fixture(scope="module")
def cluster():
    cluster = ClickHouseCluster(__file__)
    cluster.add_instance(
        "node",
        with_minio=True,
        with_remote_database_disk=False,
    )

    try:
        cluster.start()
        start_mock_servers(
            cluster,
            os.path.join(SCRIPT_DIR, "s3_mocks"),
            [("unstable_server.py", "resolver", MOCK_PORT)],
        )
        yield cluster
    finally:
        cluster.shutdown()


def test_storage_s3_cancel_during_stream_retry(cluster):
    # Reproduce a failure while consuming a successful `GetObject` response. The mock first blocks
    # after sending a response prefix. Releasing it truncates the body and makes `ReadBufferFromS3`
    # enter `processException`; the failpoint holds that method immediately before its cancellation
    # check. Cancelling and then resuming the query verifies that the outer read loop does not issue
    # another `GetObject` and reports cancellation instead of the stream exception.
    instance = cluster.instances["node"]
    resolver_id = cluster.get_container_id("resolver")
    query_id = f"s3_cancel_during_stream_retry_{uuid.uuid4()}"
    max_read_attempts = 3

    def control(action):
        # The command runs inside the resolver container, where the mock listens on `localhost`.
        # Retry transient connection failures and reject empty or malformed responses.
        deadline = time.monotonic() + 10
        last_response = ""
        while time.monotonic() < deadline:
            last_response = cluster.exec_in_container(
                resolver_id,
                [
                    "curl",
                    "-s",
                    "--connect-timeout",
                    "1",
                    "--max-time",
                    "2",
                    f"http://localhost:{MOCK_PORT}/cancel_test/{action}",
                ],
                nothrow=True,
            ).strip()
            if action == "status" and last_response.isdigit():
                return last_response
            if action != "status" and last_response == "OK":
                return last_response
            time.sleep(0.1)
        raise AssertionError(
            f"Mock control action {action} failed, last response: {last_response!r}"
        )

    request = None
    request_consumed = False

    try:
        control("reset")
        instance.query(f"SYSTEM ENABLE FAILPOINT {FAILPOINT}")
        request = instance.get_query_request(
            f"""
            SELECT count() FROM s3(
                'http://resolver:{MOCK_PORT}/root/cancel_during_retry.csv',
                NOSIGN,
                'CSV',
                'column1 Int64, column2 Int64, column3 Int64, column4 Int64')
            SETTINGS s3_max_single_read_retries={max_read_attempts}
            """,
            query_id=query_id,
            timeout=180,
        )

        def get_request_count():
            nonlocal request_consumed
            if request.process.poll() is not None:
                answer, error = request.get_answer_and_error()
                request_consumed = True
                raise AssertionError(
                    "S3 query finished before contacting the mock: "
                    f"stdout={answer!r}, stderr={error!r}"
                )
            return int(control("status"))

        get_requests = wait_condition(
            get_request_count,
            lambda requests: requests > 0,
            max_attempts=300,
            delay=0.1,
        )
        assert get_requests == 1

        control("release")
        instance.query(f"SYSTEM WAIT FAILPOINT {FAILPOINT} PAUSE", timeout=60)

        assert_eq_with_retry(
            instance,
            f"SELECT count() FROM system.processes WHERE query_id='{query_id}'",
            "1",
        )

        instance.query(f"KILL QUERY WHERE query_id='{query_id}' ASYNC")
        assert_eq_with_retry(
            instance,
            f"SELECT is_cancelled FROM system.processes WHERE query_id='{query_id}'",
            "1",
        )

        instance.query(f"SYSTEM NOTIFY FAILPOINT {FAILPOINT}")
        error = request.get_error()
        request_consumed = True

        assert "QUERY_WAS_CANCELLED" in error, error
        assert control("status") == "1"
    finally:
        had_test_exception = sys.exc_info()[0] is not None
        cleanup_errors = []

        def cleanup(description, action):
            try:
                action()
            except Exception as ex:
                logging.exception("Failed to clean up %s", description)
                cleanup_errors.append(ex)

        # Each cleanup action is independent so one unavailable component cannot leave the mock,
        # failpoint, or query active for subsequent tests in this module-scoped cluster.
        cleanup("mock response", lambda: control("release"))
        cleanup(
            "query",
            lambda: instance.query(
                f"KILL QUERY WHERE query_id='{query_id}' ASYNC",
                ignore_error=True,
            ),
        )
        cleanup(
            "failpoint",
            lambda: instance.query(
                f"SYSTEM DISABLE FAILPOINT {FAILPOINT}", ignore_error=True
            ),
        )
        if request is not None and not request_consumed:
            cleanup("query request", request.get_answer_and_error)
        cleanup("mock state", lambda: control("reset"))

        if cleanup_errors and not had_test_exception:
            raise cleanup_errors[0]
