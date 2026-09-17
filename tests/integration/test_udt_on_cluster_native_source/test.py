"""Distributed native schema-copy boundaries for mapped Atomic tables."""

import uuid

import pytest

from helpers.cluster import ClickHouseCluster


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/udt.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/remote_servers.xml"],
    user_configs=["configs/udt.xml"],
    with_zookeeper=True,
    with_remote_database_disk=False,
)

ENABLED = {"allow_experimental_user_defined_types": 1}
DISTRIBUTED_V2 = {
    **ENABLED,
    "distributed_ddl_entry_format_version": 2,
    "distributed_ddl_output_mode": "throw",
    "distributed_ddl_task_timeout": 60,
    "use_legacy_to_time": 0,
}


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown(ignore_fatal=True)


def query(instance, sql, *, settings=ENABLED):
    return instance.query(sql, settings=settings, timeout=120)


def assert_worker_rejects(database, destination, statement):
    error = node1.query_and_get_error(
        statement,
        settings=DISTRIBUTED_V2,
        timeout=120,
    )
    assert error, statement
    normalized_error = error.lower()
    assert "logical native table source-sidecar" in normalized_error, error
    assert "ddl" in normalized_error, error
    assert query(node2, f"EXISTS TABLE {database}.{destination}").strip() == "0"


def test_worker_rejects_mapped_native_source_and_keeps_physical_copying(started_cluster):
    database = f"udt_on_cluster_source_{uuid.uuid4().hex[:8]}"

    try:
        for instance in (node1, node2):
            query(instance, f"CREATE DATABASE {database} ENGINE = Atomic")
            query(
                instance,
                f"CREATE TABLE {database}.physical_src (id UInt64) "
                "ENGINE = MergeTree ORDER BY id",
            )

        query(
            node1,
            f"CREATE TABLE {database}.src (id UInt64) "
            "ENGINE = MergeTree ORDER BY id",
        )
        query(node2, f"CREATE TYPE {database}.UserId AS UInt64")
        query(
            node2,
            f"CREATE TABLE {database}.src (id {database}.UserId) "
            "ENGINE = MergeTree ORDER BY id",
        )
        query(node2, f"INSERT INTO {database}.physical_src VALUES (42)")

        assert_worker_rejects(
            database,
            "mapped_as_copy",
            f"CREATE TABLE {database}.mapped_as_copy "
            "ON CLUSTER udt_worker_only "
            f"AS {database}.src ENGINE = Memory",
        )
        assert_worker_rejects(
            database,
            "mapped_clone",
            f"CREATE TABLE {database}.mapped_clone "
            "ON CLUSTER udt_worker_only "
            f"CLONE AS {database}.src",
        )

        query(
            node1,
            f"CREATE TABLE {database}.physical_as_copy "
            "ON CLUSTER udt_worker_only "
            f"AS {database}.physical_src ENGINE = Memory",
            settings=DISTRIBUTED_V2,
        )
        physical_as_create = query(
            node2, f"SHOW CREATE TABLE {database}.physical_as_copy"
        )
        assert f"{database}.UserId" not in physical_as_create

        query(
            node1,
            f"CREATE TABLE {database}.physical_clone "
            "ON CLUSTER udt_worker_only "
            f"CLONE AS {database}.physical_src",
            settings=DISTRIBUTED_V2,
        )
        assert query(
            node2, f"SELECT sum(id) FROM {database}.physical_clone"
        ).strip() == "42"
        physical_clone_create = query(
            node2, f"SHOW CREATE TABLE {database}.physical_clone"
        )
        assert f"{database}.UserId" not in physical_clone_create
    finally:
        try:
            query(node2, f"DROP DATABASE IF EXISTS {database} SYNC")
        finally:
            query(node1, f"DROP DATABASE IF EXISTS {database} SYNC")
