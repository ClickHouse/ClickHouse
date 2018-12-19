import docker
import os
import pwd
import pytest
import re

from helpers.cluster import ClickHouseCluster


def expect_failure_with_message(expected_message):
    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance('node', with_zookeeper=False)

    with pytest.raises(Exception):
        cluster.start()

    current_user_id = os.getuid()
    other_user_id = pwd.getpwnam('nobody').pw_uid

    docker_api = docker.from_env().api
    container = node.get_docker_handle()
    container.start()
    container.exec_run('chown {0} /var/lib/clickhouse'.format(other_user_id), privileged=True)
    container.exec_run('clickhouse server --config-file=/etc/clickhouse-server/config.xml --log-file=/var/log/clickhouse-server/clickhouse-server.log --errorlog-file=/var/log/clickhouse-server/clickhouse-server.err.log', privileged=True)

    cluster.shutdown() # cleanup

    with open(os.path.join(node.path, 'logs/clickhouse-server.err.log')) as log:
        last_message = log.readlines()[-1].strip()
        if re.search(expected_message, last_message) is None:
            pytest.fail('Expected the server to fail with a message "{}", but the last message is "{}"'.format(expected_message, last_message))


def test_different_user():
    current_user_id = os.getuid()

    if current_user_id == 0:
        current_user_name = pwd.getpwuid(current_user_id).pw_name

        expect_failure_with_message(
            'Effective user of the process \(.*\) does not match the owner of the data \(.*\)',
        )
