import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.export_partition_helpers import (
    make_mt,
    unique_suffix,
    wait_for_export_status,
    wait_for_export_to_start,
)
from helpers.network import PartitionManager

# What is left here is the plain-`MergeTree` behavior that the unified suites cannot express:
#
#  * this cluster has no ZooKeeper at all, which is what proves a plain `MergeTree` export needs no
#    Keeper ensemble - `test_export_partition_to_object_storage` runs inside a Keeper-backed cluster,
#  * the task descriptor lives on the table's disk instead of in Keeper, so it must survive a hard
#    restart and resume on its own.
#
# Everything else that used to live here now runs as the `mt` parameter of
# `test_export_partition_to_object_storage`.


def skip_if_remote_database_disk_enabled(cluster):
    for instance in cluster.instances.values():
        if instance.with_remote_database_disk:
            pytest.skip(
                "Test cannot run with remote database disk enabled, as it blocks MinIO which stores database metadata"
            )


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node",
            main_configs=[
                "configs/named_collections.xml",
                "configs/allow_experimental_export_partition.xml",
            ],
            user_configs=["configs/users.d/profile.xml"],
            with_minio=True,
            stay_alive=True,
        )
        logging.info("Starting cluster...")
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def drop_tables_after_test(cluster):
    yield
    for instance_name, instance in cluster.instances.items():
        try:
            tables_str = instance.query(
                "SELECT name FROM system.tables WHERE database = 'default' FORMAT TabSeparated"
            ).strip()
            if not tables_str:
                continue
            # One client invocation for the whole batch. Every query spawns a fresh client
            # process, which costs seconds on a sanitizer build, so dropping tables one at a
            # time made teardown a large share of this suite's runtime.
            tables = [table.strip() for table in tables_str.split("\n") if table.strip()]
            if tables:
                instance.query(
                    "".join(f"DROP TABLE IF EXISTS default.`{table}` SYNC;" for table in tables)
                )
        except Exception as e:
            logging.warning(f"drop_tables_after_test: cleanup failed on {instance_name}: {e}")


def create_tables_and_insert_data(node, mt_table, s3_table):
    node.query(f"DROP TABLE IF EXISTS {mt_table} SYNC")
    make_mt(node, mt_table, "id UInt64, year UInt16", "year")
    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020), (3, 2020), (4, 2021)")
    node.query(
        f"CREATE TABLE {s3_table} (id UInt64, year UInt16) "
        f"ENGINE = S3(s3_conn, filename='{s3_table}', format=Parquet, partition_strategy='hive') "
        f"PARTITION BY year"
    )


def test_export_partition_without_keeper(cluster):
    """A plain MergeTree export is driven entirely by the local scheduler, so it must work on a node
    that has no ZooKeeper configured at all."""
    node = cluster.instances["node"]

    # Asserted against the cluster definition rather than a system table: this is a property of
    # how the instance was built, and it holds on every server version.
    assert not node.with_zookeeper, (
        "This suite must run without ZooKeeper, otherwise it proves nothing about a Keeper-less "
        "export"
    )

    postfix = unique_suffix()
    mt_table = f"basic_mt_{postfix}"
    s3_table = f"basic_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED")

    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n"
    assert (
        node.query(
            f"SELECT count() FROM s3(s3_conn, filename='{s3_table}/commit_2020_*', format=LineAsString)"
        )
        != "0\n"
    ), "Commit file missing for partition 2020"


def test_export_partition_resumes_after_restart(cluster):
    """The distinguishing feature of the plain MergeTree implementation: the on-disk task
    descriptor must let an in-flight export resume after a hard restart."""
    skip_if_remote_database_disk_enabled(cluster)
    node = cluster.instances["node"]

    postfix = unique_suffix()
    mt_table = f"restart_mt_{postfix}"
    s3_table = f"restart_s3_{postfix}"

    create_tables_and_insert_data(node, mt_table, s3_table)

    minio_ip = cluster.minio_ip
    minio_port = cluster.minio_port

    with PartitionManager() as pm:
        pm.add_rule({
            "instance": node,
            "destination": node.ip_address,
            "protocol": "tcp",
            "source_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })
        pm.add_rule({
            "instance": node,
            "destination": minio_ip,
            "protocol": "tcp",
            "destination_port": minio_port,
            "action": "REJECT --reject-with tcp-reset",
        })

        node.query(f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {s3_table}")
        wait_for_export_to_start(node, mt_table, s3_table, "2020")

        # Kill the server while the export is still in flight (S3 blocked, nothing committed yet).
        node.stop_clickhouse(kill=True)

    # We cannot observe the "nothing committed before the crash" invariant on a single node: while
    # the only node is down there is nothing to query S3 with, and once it restarts (S3 now
    # reachable) the persisted PENDING task resumes immediately. So we only assert the actual
    # restart-resume behavior: the task must resume from its on-disk descriptor and complete.
    node.start_clickhouse()

    wait_for_export_status(node, mt_table, s3_table, "2020", "COMPLETED", timeout=90)
    assert node.query(f"SELECT count() FROM {s3_table} WHERE year = 2020") == "3\n"
