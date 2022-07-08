import time
import threading
from os import path as p, unlink
from tempfile import NamedTemporaryFile

import helpers
import pytest
from helpers.cluster import ClickHouseCluster


def test_xml_main_conf():
    # main configs are in XML; config.d and users.d are in YAML
    cluster = ClickHouseCluster(
        __file__, zookeeper_config_path="configs/config.d/zookeeper.yaml"
    )

    all_confd = [
        "configs/config.d/access_control.yaml",
        "configs/config.d/keeper_port.yaml",
        "configs/config.d/logging_no_rotate.yaml",
        "configs/config.d/log_to_console.yaml",
        "configs/config.d/macros.yaml",
        "configs/config.d/metric_log.yaml",
        "configs/config.d/more_clusters.yaml",
        "configs/config.d/part_log.yaml",
        "configs/config.d/path.yaml",
        "configs/config.d/query_masking_rules.yaml",
        "configs/config.d/tcp_with_proxy.yaml",
        "configs/config.d/test_cluster_with_incorrect_pw.yaml",
        "configs/config.d/text_log.yaml",
        "configs/config.d/zookeeper.yaml",
    ]

    all_userd = [
        "configs/users.d/allow_introspection_functions.yaml",
        "configs/users.d/log_queries.yaml",
    ]

    node = cluster.add_instance(
        "node",
        base_config_dir="configs",
        main_configs=all_confd,
        user_configs=all_userd,
        with_zookeeper=False,
        config_root_name="clickhouse",
    )

    try:
        cluster.start()
        assert (
            node.query(
                "select value from system.settings where name = 'max_memory_usage'"
            )
            == "10000000000\n"
        )
        assert (
            node.query(
                "select value from system.settings where name = 'max_block_size'"
            )
            == "64999\n"
        )

    finally:
        cluster.shutdown()
