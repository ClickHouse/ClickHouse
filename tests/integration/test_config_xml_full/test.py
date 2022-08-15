import time
import threading
from os import path as p, unlink
from tempfile import NamedTemporaryFile

import helpers
import pytest
from helpers.cluster import ClickHouseCluster


def test_xml_full_conf():
    # all configs are in XML
    cluster = ClickHouseCluster(__file__, zookeeper_config_path='configs/config.d/zookeeper.xml')

    all_confd = ['configs/config.d/access_control.xml',
                'configs/config.d/keeper_port.xml',
                'configs/config.d/logging_no_rotate.xml',
                'configs/config.d/log_to_console.xml',
                'configs/config.d/macros.xml',
                'configs/config.d/metric_log.xml',
                'configs/config.d/more_clusters.xml',
                'configs/config.d/part_log.xml',
                'configs/config.d/path.xml',
                'configs/config.d/query_masking_rules.xml',
                'configs/config.d/tcp_with_proxy.xml',
                'configs/config.d/text_log.xml',
                'configs/config.d/zookeeper.xml']

    all_userd = ['configs/users.d/allow_introspection_functions.xml',
                'configs/users.d/log_queries.xml']

    node = cluster.add_instance('node', base_config_dir='configs', main_configs=all_confd, user_configs=all_userd, with_zookeeper=False)

    try:
        cluster.start()
        assert(node.query("select value from system.settings where name = 'max_memory_usage'") == "10000000000\n")
        assert(node.query("select value from system.settings where name = 'max_block_size'") == "64999\n")

    finally:
        cluster.shutdown()
