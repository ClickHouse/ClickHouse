import os
import pwd
import re
import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)
other_user_id = pwd.getpwnam("nobody").pw_uid
current_user_id = os.getuid()


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        if current_user_id != 0:
            return

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown(ignore_fatal=True)


def test_different_user(started_cluster):
    with pytest.raises(Exception):
        node.stop_clickhouse()
        node.exec_in_container(
            ["bash", "-c", f"chown {other_user_id} /var/lib/clickhouse"],
            privileged=True,
        )
        node.start_clickhouse(start_wait_sec=3)

    log = node.grep_in_log("Effective")
    expected_message = "Effective user of the process \\(.+?\\) does not match the owner of the data \\(.+?\\)\\. Run under 'sudo -u .*'\\."
    if re.search(expected_message, log) is None:
        pytest.fail(
            'Expected the server to fail with a message "{}", but the last message is "{}"'.format(
                expected_message, log
            )
        )
    node.exec_in_container(
        ["bash", "-c", f"chown {current_user_id} /var/lib/clickhouse"], privileged=True
    )
    node.start_clickhouse()
    node.rotate_logs()
