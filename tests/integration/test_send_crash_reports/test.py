import os
import time

import helpers.cluster
import helpers.test_tools
import pytest

from . import fake_sentry_server

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def started_node():
    cluster = helpers.cluster.ClickHouseCluster(__file__)
    try:
        node = cluster.add_instance("node", main_configs=[
            os.path.join(SCRIPT_DIR, "configs", "config_send_crash_reports.xml")
        ])
        cluster.start()
        yield node
    finally:
        cluster.shutdown()


def test_send_segfault(started_node, ):
    started_node.copy_file_to_container(os.path.join(SCRIPT_DIR, "fake_sentry_server.py"), "/fake_sentry_server.py")
    started_node.exec_in_container(["bash", "-c", "python3 /fake_sentry_server.py > /fake_sentry_server.log 2>&1"], detach=True, user="root")
    time.sleep(0.5)
    started_node.exec_in_container(["bash", "-c", "pkill -11 clickhouse"], user="root")

    result = None
    for attempt in range(1, 6):
        time.sleep(0.25 * attempt)
        result = started_node.exec_in_container(['cat', fake_sentry_server.RESULT_PATH], user='root')
        if result == 'OK':
            break
        elif result == 'INITIAL_STATE':
            continue
        elif result:
            assert False, 'Unexpected state: ' + result

    assert result == 'OK', 'Crash report not sent'
