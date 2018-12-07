import os
import pwd
import pytest
import re

from helpers.cluster import ClickHouseCluster


def expect_failure_with_message(config, expected_message):
    cluster = ClickHouseCluster(__file__)
    node = cluster.add_instance('node', main_configs=[config], with_zookeeper=False)

    with pytest.raises(Exception):
        cluster.start()

    cluster.shutdown() # cleanup

    with open(os.path.join(node.path, 'logs/stderr.log')) as log:
        last_message = log.readlines()[-1].strip()
        if re.search(expected_message, last_message) is None:
            pytest.fail('Expected the server to fail with a message "{}", but the last message is "{}"'.format(expected_message, last_message))


def test_no_such_directory():
    expect_failure_with_message('configs/no_such_directory.xml', 'Failed to stat.*no_such_directory')


def test_different_user():
    current_user_id = os.getuid()

    if current_user_id != 0:
        current_user_name = pwd.getpwuid(current_user_id).pw_name

        expect_failure_with_message(
            'configs/owner_mismatch.xml',
            'Effective user of the process \(({}|{})\) does not match the owner of the data \((0|root)\)'.format(current_user_id, current_user_name),
        )
