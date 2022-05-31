import os
import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster, get_instances_dir


COMMON_CONFIGS = ["configs/config.d/clusters.xml"]


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node1z",
            main_configs=COMMON_CONFIGS + ["configs/config.d/storage_conf.xml"],
            macros={"cluster": "node_zero_copy", "replica": "0"},
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2z",
            main_configs=COMMON_CONFIGS + ["configs/config.d/storage_conf.xml"],
            macros={"cluster": "node_zero_copy", "replica": "1"},
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node1n",
            main_configs=COMMON_CONFIGS
            + ["configs/config.d/storage_conf_without_zero_copy.xml"],
            macros={"cluster": "node_no_zero_copy", "replica": "2"},
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node2n",
            main_configs=COMMON_CONFIGS
            + ["configs/config.d/storage_conf_without_zero_copy.xml"],
            macros={"cluster": "node_no_zero_copy", "replica": "3"},
            with_zookeeper=True,
            stay_alive=True,
        )
        cluster.add_instance(
            "node_another_bucket",
            main_configs=COMMON_CONFIGS
            + ["configs/config.d/storage_conf_another_bucket.xml"],
            macros={"cluster": "node_another_bucket", "replica": "0"},
            with_zookeeper=True,
            stay_alive=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def create_table(node, table_name, schema, attach=False, db_atomic=False, uuid=""):
    node.query(
        "CREATE DATABASE IF NOT EXISTS s3 {on_cluster} ENGINE = {engine}".format(
            engine="Atomic" if db_atomic else "Ordinary",
            on_cluster="ON CLUSTER '{cluster}'",
        )
    )

    create_table_statement = """
        {create} TABLE s3.{table_name} {uuid} {on_cluster} (
            key UInt32,
            {schema}
        ) ENGINE={engine}
        PARTITION BY key
        ORDER BY key
        SETTINGS
            storage_policy='s3',
            old_parts_lifetime=600,
            index_granularity=512
        """.format(
        create="ATTACH" if attach else "CREATE",
        table_name=table_name,
        uuid="UUID '{uuid}'".format(uuid=uuid) if db_atomic and uuid else "",
        on_cluster="ON CLUSTER '{cluster}'",
        schema=schema,
        engine="ReplicatedMergeTree('/clickhouse/tables/{cluster}/test', '{replica}')",
    )

    node.query(create_table_statement)


def purge_s3(cluster, bucket):
    minio = cluster.minio_client
    for obj in list(minio.list_objects(bucket, recursive=True)):
        if str(obj.object_name).find(".SCHEMA_VERSION") != -1:
            continue
        minio.remove_object(bucket, obj.object_name)


def drop_s3_metadata(node):
    node.exec_in_container(
        ["bash", "-c", "rm -rf /var/lib/clickhouse/disks/s3/*"], user="root"
    )


def drop_shadow_information(node):
    node.exec_in_container(
        ["bash", "-c", "rm -rf /var/lib/clickhouse/shadow/*"], user="root"
    )


def create_restore_file(node, revision=None, bucket=None, path=None, detached=None):
    node.exec_in_container(
        ["bash", "-c", "mkdir -p /var/lib/clickhouse/disks/s3/"], user="root"
    )
    node.exec_in_container(
        ["bash", "-c", "touch /var/lib/clickhouse/disks/s3/restore"], user="root"
    )

    add_restore_option = 'echo -en "{}={}\n" >> /var/lib/clickhouse/disks/s3/restore'
    if revision:
        node.exec_in_container(
            ["bash", "-c", add_restore_option.format("revision", revision)], user="root"
        )
    if bucket:
        node.exec_in_container(
            ["bash", "-c", add_restore_option.format("source_bucket", bucket)],
            user="root",
        )
    if path:
        node.exec_in_container(
            ["bash", "-c", add_restore_option.format("source_path", path)], user="root"
        )
    if detached:
        node.exec_in_container(
            ["bash", "-c", add_restore_option.format("detached", "true")], user="root"
        )


def get_revision_counter(node, backup_number):
    return int(
        node.exec_in_container(
            [
                "bash",
                "-c",
                "cat /var/lib/clickhouse/disks/s3/shadow/{}/revision.txt".format(
                    backup_number
                ),
            ],
            user="root",
        )
    )


def get_table_uuid(node, db_atomic, table):
    uuid = ""
    if db_atomic:
        uuid = node.query(
            "SELECT uuid FROM system.tables WHERE database='s3' AND table='{}' FORMAT TabSeparated".format(
                table
            )
        ).strip()
    return uuid


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield

    node_names = ["node1z", "node2z", "node1n", "node2n", "node_another_bucket"]

    for node_name in node_names:
        node = cluster.instances[node_name]
        node.query("DROP TABLE IF EXISTS s3.test SYNC")
        node.query("DROP DATABASE IF EXISTS s3 SYNC")

        drop_s3_metadata(node)
        drop_shadow_information(node)

    buckets = [cluster.minio_bucket, cluster.minio_bucket_2]
    for bucket in buckets:
        purge_s3(cluster, bucket)


@pytest.mark.parametrize("db_atomic", [False, True])
@pytest.mark.parametrize("zero_copy", [False, True])
def test_restore_another_bucket_path(cluster, db_atomic, zero_copy):
    suffix = "z" if zero_copy else "n"
    nodes = [cluster.instances[f"node1{suffix}"], cluster.instances[f"node2{suffix}"]]

    keys = 100
    data_columns = 10
    size = 1

    columns = []
    for c in range(0, data_columns):
        columns.append("data{c} String".format(c=c))
    schema = ", ".join(columns)

    create_table(nodes[0], "test", schema, db_atomic=db_atomic)
    uuid = get_table_uuid(nodes[0], db_atomic, "test")

    dropped_keys = 0

    for key in range(0, keys):
        node = nodes[key % 2]
        node.query(
            "INSERT INTO s3.test SELECT {key}, * FROM generateRandom('{schema}') LIMIT {size}".format(
                key=key, schema=schema, size=size
            )
        )
        if not (key % 3):
            dropped_keys += 1
            node.query("ALTER TABLE s3.test DROP PARTITION '{key}'".format(key=key))

    for key in range(0, keys):
        if not ((key + 1) % 3):
            dropped_keys += 1
            node.query("ALTER TABLE s3.test DROP PARTITION '{key}'".format(key=key))

    nodes[0].query("SYSTEM SYNC REPLICA s3.test")
    nodes[1].query("SYSTEM SYNC REPLICA s3.test")

    # To ensure parts have merged
    nodes[0].query("OPTIMIZE TABLE s3.test")
    nodes[1].query("OPTIMIZE TABLE s3.test")

    assert nodes[0].query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(size * (keys - dropped_keys))
    assert nodes[1].query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(size * (keys - dropped_keys))

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_restore_file(node_another_bucket, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket, "test", schema, attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(size * (keys - dropped_keys))
