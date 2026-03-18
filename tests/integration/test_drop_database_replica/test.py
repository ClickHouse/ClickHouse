import logging
import os
import re
import shutil
import threading
import time

import pytest

from helpers.cluster import ClickHouseCluster

test_recover_staled_replica_run = 1

cluster = ClickHouseCluster(__file__)

node1 = cluster.add_instance(
    "node1",
    main_configs=[
        "configs/config.xml",
    ],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r1"},
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

node2 = cluster.add_instance(
    "node2",
    main_configs=[
        "configs/config.xml",
    ],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s1", "replica": "r2"},
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)

node3 = cluster.add_instance(
    "node3",
    main_configs=[
        "configs/config.xml",
    ],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s2", "replica": "r1"},
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)
node4 = cluster.add_instance(
    "node4",
    main_configs=[
        "configs/config.xml",
    ],
    user_configs=["configs/settings.xml"],
    with_zookeeper=True,
    stay_alive=True,
    macros={"shard": "s2", "replica": "r2"},
    with_minio=True,
    keeper_required_feature_flags=["multi_read", "create_if_not_exists"],
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        yield cluster

    finally:
        cluster.shutdown()


@pytest.mark.parametrize("with_tables", [False, True])
def test_drop_database_replica(started_cluster, with_tables: bool):
    node1.query("DROP DATABASE IF EXISTS db")
    node2.query("DROP DATABASE IF EXISTS db")
    node3.query("DROP DATABASE IF EXISTS db")
    node4.query("DROP DATABASE IF EXISTS db")

    zk_path = "/test/db"
    node1.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )

    node1.query(f"CREATE TABLE db.t (x INT) ENGINE=MergeTree ORDER BY x")
    node1.query(
        f"CREATE TABLE db.mv_target (x INT) ENGINE=ReplicatedMergeTree ORDER BY x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv1 REFRESH EVERY 1 SECOND APPEND (x INT) ENGINE=MergeTree ORDER BY x AS SELECT 1 AS x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv2 REFRESH EVERY 1 SECOND TO db.mv_target AS SELECT 1 AS x"
    )
    with_tables_clause = " WITH TABLES" if with_tables else ""
    assert "SYNTAX_ERROR" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM TABLE t"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM DATABASE db"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM DATABASE db"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM ZKPATH '{zk_path}' {with_tables_clause}"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '{zk_path}' {with_tables_clause}"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM ZKPATH '{zk_path}/'  {with_tables_clause}"
    )
    assert "There is a local database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '{zk_path}/' {with_tables_clause}"
    )

    node2.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )
    node3.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )

    node2.query(f"SYSTEM SYNC DATABASE REPLICA db")
    node3.query(f"SYSTEM SYNC DATABASE REPLICA db")

    assert "is active, cannot drop it" in node2.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM DATABASE db"
    )

    node1.query(f"CREATE TABLE db.t2 (x INT) ENGINE=Log")
    node1.query(f"CREATE TABLE db.t3 (x INT) ENGINE=Log")
    node1.query(f"CREATE TABLE db.t4 (x INT) ENGINE=Log")

    node4_uuid = node4.query("SELECT serverUUID()").strip()
    node4.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )
    node4.query("DETACH DATABASE db")
    node4.query(
        f"INSERT INTO system.zookeeper(name, path, value) VALUES ('active', '{zk_path}/replicas/s2|r2', '{node4_uuid}')",
    )

    assert "TIMEOUT_EXCEEDED" in node1.query_and_get_error(
        "CREATE TABLE db.t22 (n int) ENGINE=Log",
        settings={
            "distributed_ddl_task_timeout": 5,
            "distributed_ddl_output_mode": "none_only_active",
        },
    )
    assert "TIMEOUT_EXCEEDED" in node1.query_and_get_error(
        "CREATE TABLE db.t33 (n int) ENGINE=Log",
        settings={
            "distributed_ddl_task_timeout": 5,
            "distributed_ddl_output_mode": "throw_only_active",
        },
    )
    node1.query(
        "CREATE TABLE db.t44 (n int) ENGINE=Log",
        settings={
            "distributed_ddl_task_timeout": 5,
            "distributed_ddl_output_mode": "null_status_on_timeout_only_active",
        },
    )
    node4.query("ATTACH DATABASE db")

    node3.query("DETACH DATABASE db")
    node4.query("SYSTEM DROP DATABASE replica 'r1' FROM SHARD 's2' FROM DATABASE db")
    node3.query("ATTACH DATABASE db")
    assert "Database is in readonly mode" in node3.query_and_get_error(
        "CREATE TABLE db.t55 (n int) ENGINE=MergeTree",
        settings={
            "distributed_ddl_output_mode": "none",
        },
    )
    node4.query("DROP DATABASE db SYNC")

    node2.query("DETACH DATABASE db")
    node1.query("SYSTEM DROP DATABASE REPLICA 's1|r2' FROM DATABASE db")
    node2.query("ATTACH DATABASE db")
    assert "Database is in readonly mode" in node2.query_and_get_error(
        "CREATE TABLE db.t55 (n int) ENGINE=MergeTree",
        settings={
            "distributed_ddl_output_mode": "none",
        },
    )

    node1.query("DETACH DATABASE db")
    node4.query(
        f"SYSTEM DROP DATABASE REPLICA 's1|r1' FROM ZKPATH '{zk_path}'  {with_tables_clause}"
    )
    node1.query("ATTACH DATABASE db")
    assert "Database is in readonly mode" in node1.query_and_get_error(
        "CREATE TABLE db.t55 (n int) ENGINE=MergeTree",
        settings={
            "distributed_ddl_output_mode": "none",
        },
    )

    node1.query(f"SYSTEM DROP DATABASE REPLICA 'dummy' FROM SHARD 'dummy'")

    node1.query("DROP DATABASE db SYNC")
    node2.query("DROP DATABASE db SYNC")
    node3.query("DROP DATABASE db SYNC")

    node4.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )

    node4.query(
        "CREATE TABLE db.rmt (x INT) ENGINE=ReplicatedMergeTree ORDER BY x",
        settings={
            "distributed_ddl_output_mode": "none",
        },
    )
    node4.query(f"SYSTEM DROP REPLICA 'dummy' FROM DATABASE db")
    node4.query(f"SYSTEM DROP REPLICA 'dummy'")

    node4.query("DROP DATABASE db SYNC")


def test_drop_database_replica_with_tables_for_non_existing_db(
    started_cluster,
):
    zk_path = "/test/db_dummy"
    assert "Database metadata keeper path does not exists" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '{zk_path}/' WITH TABLES"
    )


def test_drop_database_replica_with_tables_for_dropped_db(
    started_cluster,
):
    node1.query("DROP DATABASE IF EXISTS db SYNC")

    zk_path = "/test/db"
    node1.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )

    node1.query(f"CREATE TABLE db.t (x INT) ENGINE=MergeTree ORDER BY x")
    node1.query(
        f"CREATE TABLE db.mv_target (x INT) ENGINE=ReplicatedMergeTree ORDER BY x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv1 REFRESH EVERY 1 SECOND APPEND (x INT) ENGINE=MergeTree ORDER BY x AS SELECT 1 AS x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv2 REFRESH EVERY 1 SECOND TO db.mv_target AS SELECT 1 AS x"
    )

    node1.query("DROP DATABASE `db` SYNC")
    assert "Database metadata keeper path does not exists" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '{zk_path}/' WITH TABLES"
    )


def test_drop_database_replica_with_tables_for_detached_db(
    started_cluster,
):
    node1.query("DROP DATABASE IF EXISTS db SYNC")

    zk_path = "/test/db"
    node1.query(
        f"CREATE DATABASE db ENGINE = Replicated('{zk_path}'"
        + r", '{shard}', '{replica}')"
    )

    node1.query(f"CREATE TABLE db.t (x INT) ENGINE=MergeTree ORDER BY x")
    node1.query(
        f"CREATE TABLE db.mv_target (x INT) ENGINE=ReplicatedMergeTree ORDER BY x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv1 REFRESH EVERY 1 SECOND APPEND (x INT) ENGINE=MergeTree ORDER BY x AS SELECT 1 AS x"
    )
    node1.query(
        "CREATE MATERIALIZED VIEW db.rmv2 REFRESH EVERY 1 SECOND TO db.mv_target AS SELECT 1 AS x"
    )

    node1.query("DETACH DATABASE `db`")
    assert "There is a detached database" in node1.query_and_get_error(
        f"SYSTEM DROP DATABASE REPLICA 'r1' FROM SHARD 's1' FROM ZKPATH '{zk_path}/' WITH TABLES"
    )

    node1.query("ATTACH DATABASE `db`")

    node1.query("DROP DATABASE IF EXISTS db SYNC")
