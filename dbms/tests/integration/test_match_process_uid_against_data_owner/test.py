import docker
import os
import pwd
import pytest
import re

from helpers.cluster import ClickHouseCluster, CLICKHOUSE_START_COMMAND


def test_different_user():
    current_user_id = os.getuid()

    if current_user_id != 0:
        return

    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance('node')

    with pytest.raises(Exception):
        cluster.start()

    other_user_id = pwd.getpwnam('nobody').pw_uid

    docker_api = docker.from_env().api
    container = node.get_docker_handle()
    container.start()
    container.exec_run('chown {} /var/lib/clickhouse'.format(other_user_id), privileged=True)
    container.exec_run(CLICKHOUSE_START_COMMAND)

    cluster.shutdown() # cleanup

    with open(os.path.join(node.path, 'logs/clickhouse-server.err.log')) as log:
        last_message = log.readlines()[-1].strip()
        expected_message = 'Effective user of the process \(.*\) does not match the owner of the data \(.*\)'

        if re.search(expected_message, last_message) is None:
            pytest.fail('Expected the server to fail with a message "{}", but the last message is "{}"'.format(expected_message, last_message))
