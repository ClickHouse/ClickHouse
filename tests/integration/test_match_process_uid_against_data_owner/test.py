import os
import pwd
import re
import logging
import pytest
from helpers.cluster import ClickHouseCluster, CLICKHOUSE_START_COMMAND

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node')
other_user_id = pwd.getpwnam('nobody').pw_uid

@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        current_user_id = os.getuid()
        if current_user_id != 0:
            return

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown(ignore_fatal=True)


def test_different_user():

    container = node.get_docker_handle()
    container.stop()
    container.start()
    container.exec_run('chown {} /var/lib/clickhouse'.format(other_user_id), privileged=True)
    container.exec_run(CLICKHOUSE_START_COMMAND)
    container.exec_run('sleep 1') # Without sleep logs can not be flushed before check
    with open(os.path.join(node.path, 'logs/clickhouse-server.err.log')) as log:
        expected_message = "Effective user of the process \(.*\) does not match the owner of the data \(.*\)\. Run under 'sudo -u .*'\."
        last_message = ""
        for line in log:
            if "Effective" in line:
                last_message = line

        if re.search(expected_message, last_message) is None:
            pytest.fail(
                'Expected the server to fail with a message "{}", but the last message is "{}"'.format(expected_message,
                                                                                                       last_message))
    container.exec_run('chown clickhouse /var/lib/clickhouse', privileged=True)
    container.exec_run(CLICKHOUSE_START_COMMAND)
    node.rotate_logs()
