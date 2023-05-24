import os
import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
COMMON_CONFIGS = [
    "configs/config.d/bg_processing_pool_conf.xml",
    "configs/config.d/clusters.xml",
]


def replace_config(path, old, new):
    config = open(path, "r")
    config_lines = config.readlines()
    config.close()
    config_lines = [line.replace(old, new) for line in config_lines]
    config = open(path, "w")
    config.writelines(config_lines)
    config.close()


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        cluster.add_instance(
            "node",
            main_configs=COMMON_CONFIGS + ["configs/config.d/storage_conf.xml"],
            macros={"cluster": "node", "replica": "0"},
            with_minio=True,
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
        cluster.add_instance(
            "node_another_bucket_path",
            main_configs=COMMON_CONFIGS
            + ["configs/config.d/storage_conf_another_bucket_path.xml"],
            stay_alive=True,
        )
        cluster.add_instance(
            "node_not_restorable",
            main_configs=COMMON_CONFIGS
            + ["configs/config.d/storage_conf_not_restorable.xml"],
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


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}',{})".format(x, y, z, 0) for x, y, z in data])


def create_table(
    node, table_name, attach=False, replicated=False, db_atomic=False, uuid=""
):
    node.query("DROP DATABASE IF EXISTS s3")

    node.query(
        "CREATE DATABASE IF NOT EXISTS s3 ENGINE = {engine}".format(
            engine="Atomic" if db_atomic else "Ordinary"
        ),
        settings={"allow_deprecated_database_ordinary": 1},
    )

    create_table_statement = """
        {create} TABLE s3.{table_name} {uuid} {on_cluster} (
            dt Date,
            id Int64,
            data String,
            counter Int64,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE={engine}
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS
            storage_policy='s3',
            old_parts_lifetime=600,
            index_granularity=512
        """.format(
        create="ATTACH" if attach else "CREATE",
        table_name=table_name,
        uuid="UUID '{uuid}'".format(uuid=uuid) if db_atomic and uuid else "",
        on_cluster="ON CLUSTER '{}'".format(node.name) if replicated else "",
        engine="ReplicatedMergeTree('/clickhouse/tables/{cluster}/test', '{replica}')"
        if replicated
        else "MergeTree()",
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

    node_names = [
        "node",
        "node_another_bucket",
        "node_another_bucket_path",
        "node_not_restorable",
    ]

    for node_name in node_names:
        node = cluster.instances[node_name]
        node.query("DROP TABLE IF EXISTS s3.test SYNC")
        node.query("DROP DATABASE IF EXISTS s3 SYNC")

        drop_s3_metadata(node)
        drop_shadow_information(node)

    buckets = [cluster.minio_bucket, cluster.minio_bucket_2]
    for bucket in buckets:
        purge_s3(cluster, bucket)


@pytest.mark.parametrize("replicated", [False, True])
@pytest.mark.parametrize("db_atomic", [False, True])
def test_full_restore(cluster, replicated, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", attach=False, replicated=replicated, db_atomic=db_atomic)

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096, -1))
    )

    node.query("DETACH TABLE s3.test")
    drop_s3_metadata(node)
    create_restore_file(node)
    node.query("SYSTEM RESTART DISK s3")
    node.query("ATTACH TABLE s3.test")

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(
        4096 * 4
    )
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)


@pytest.mark.parametrize("db_atomic", [False, True])
def test_restore_another_bucket_path(cluster, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096, -1))
    )

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3.test")

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(
        4096 * 4
    )
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_restore_file(node_another_bucket, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket, "test", attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)

    node_another_bucket_path = cluster.instances["node_another_bucket_path"]

    create_restore_file(node_another_bucket_path, bucket="root2", path="data")
    node_another_bucket_path.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket_path, "test", attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket_path.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)
    assert node_another_bucket_path.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)


@pytest.mark.parametrize("db_atomic", [False, True])
def test_restore_different_revisions(cluster, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )

    node.query("ALTER TABLE s3.test FREEZE")
    revision1 = get_revision_counter(node, 1)

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096, -1))
    )

    node.query("ALTER TABLE s3.test FREEZE")
    revision2 = get_revision_counter(node, 2)

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3.test")

    node.query("ALTER TABLE s3.test FREEZE")
    revision3 = get_revision_counter(node, 3)

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(
        4096 * 4
    )
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node.query("SELECT count(*) from system.parts where table = 'test'") == "5\n"

    node_another_bucket = cluster.instances["node_another_bucket"]

    # Restore to revision 1 (2 parts).
    create_restore_file(node_another_bucket, revision=revision1, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket, "test", attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert (
        node_another_bucket.query(
            "SELECT count(*) from system.parts where table = 'test'"
        )
        == "2\n"
    )

    # Restore to revision 2 (4 parts).
    node_another_bucket.query("DETACH TABLE s3.test")
    create_restore_file(node_another_bucket, revision=revision2, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    node_another_bucket.query("ATTACH TABLE s3.test")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert (
        node_another_bucket.query(
            "SELECT count(*) from system.parts where table = 'test'"
        )
        == "4\n"
    )

    # Restore to revision 3 (4 parts + 1 merged).
    node_another_bucket.query("DETACH TABLE s3.test")
    create_restore_file(node_another_bucket, revision=revision3, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    node_another_bucket.query("ATTACH TABLE s3.test")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert (
        node_another_bucket.query(
            "SELECT count(*) from system.parts where table = 'test'"
        )
        == "5\n"
    )


@pytest.mark.parametrize("db_atomic", [False, True])
def test_restore_mutations(cluster, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096, -1))
    )

    node.query("ALTER TABLE s3.test FREEZE")
    revision_before_mutation = get_revision_counter(node, 1)

    node.query(
        "ALTER TABLE s3.test UPDATE counter = 1 WHERE 1", settings={"mutations_sync": 2}
    )

    node.query("ALTER TABLE s3.test FREEZE")
    revision_after_mutation = get_revision_counter(node, 2)

    node_another_bucket = cluster.instances["node_another_bucket"]

    # Restore to revision before mutation.
    create_restore_file(
        node_another_bucket, revision=revision_before_mutation, bucket="root"
    )
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket, "test", attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test FORMAT Values"
    ) == "({})".format(0)

    # Restore to revision after mutation.
    node_another_bucket.query("DETACH TABLE s3.test")
    create_restore_file(
        node_another_bucket, revision=revision_after_mutation, bucket="root"
    )
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    node_another_bucket.query("ATTACH TABLE s3.test")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test WHERE id > 0 FORMAT Values"
    ) == "({})".format(4096)

    # Restore to revision in the middle of mutation.
    # Unfinished mutation should be completed after table startup.
    node_another_bucket.query("DETACH TABLE s3.test")
    revision = (revision_before_mutation + revision_after_mutation) // 2
    create_restore_file(node_another_bucket, revision=revision, bucket="root")
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    node_another_bucket.query("ATTACH TABLE s3.test")

    # Wait for unfinished mutation completion.
    time.sleep(3)

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 2)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test WHERE id > 0 FORMAT Values"
    ) == "({})".format(4096)


def test_migrate_to_restorable_schema(cluster):
    db_atomic = True
    node = cluster.instances["node_not_restorable"]
    config_path = os.path.join(
        SCRIPT_DIR,
        "./{}/node_not_restorable/configs/config.d/storage_conf_not_restorable.xml".format(
            cluster.instances_dir_name
        ),
    )

    create_table(node, "test", db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096, -1))
    )

    replace_config(
        config_path,
        "<send_metadata>false</send_metadata>",
        "<send_metadata>true</send_metadata>",
    )
    node.restart_clickhouse()

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-06", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-06", 4096, -1))
    )

    node.query("ALTER TABLE s3.test FREEZE")
    revision = get_revision_counter(node, 1)

    assert revision != 0

    node_another_bucket = cluster.instances["node_another_bucket"]

    # Restore to revision before mutation.
    create_restore_file(
        node_another_bucket, revision=revision, bucket="root", path="another_data"
    )
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket, "test", attach=True, db_atomic=db_atomic, uuid=uuid
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 6)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)


@pytest.mark.parametrize("replicated", [False, True])
@pytest.mark.parametrize("db_atomic", [False, True])
def test_restore_to_detached(cluster, replicated, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", attach=False, replicated=replicated, db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-04", 4096, -1))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-05", 4096))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-06", 4096, -1))
    )
    node.query(
        "INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-07", 4096, 0))
    )

    # Add some mutation.
    node.query(
        "ALTER TABLE s3.test UPDATE counter = 1 WHERE 1", settings={"mutations_sync": 2}
    )

    # Detach some partition.
    node.query("ALTER TABLE s3.test DETACH PARTITION '2020-01-07'")

    node.query("ALTER TABLE s3.test FREEZE")
    revision = get_revision_counter(node, 1)

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_restore_file(
        node_another_bucket,
        revision=revision,
        bucket="root",
        path="data",
        detached=True,
    )
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket,
        "test",
        replicated=replicated,
        db_atomic=db_atomic,
        uuid=uuid,
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(0)

    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-03'")
    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-04'")
    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-05'")
    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-06'")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 4)

    # Attach partition that was already detached before backup-restore.
    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-07'")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 5)
    assert node_another_bucket.query(
        "SELECT sum(id) FROM s3.test FORMAT Values"
    ) == "({})".format(0)
    assert node_another_bucket.query(
        "SELECT sum(counter) FROM s3.test FORMAT Values"
    ) == "({})".format(4096 * 5)


@pytest.mark.parametrize("replicated", [False, True])
@pytest.mark.parametrize("db_atomic", [False, True])
def test_restore_without_detached(cluster, replicated, db_atomic):
    node = cluster.instances["node"]

    create_table(node, "test", attach=False, replicated=replicated, db_atomic=db_atomic)
    uuid = get_table_uuid(node, db_atomic, "test")

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values("2020-01-03", 1)))

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(1)

    node.query("ALTER TABLE s3.test FREEZE")
    revision = get_revision_counter(node, 1)

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_restore_file(
        node_another_bucket,
        revision=revision,
        bucket="root",
        path="data",
        detached=True,
    )
    node_another_bucket.query("SYSTEM RESTART DISK s3")
    create_table(
        node_another_bucket,
        "test",
        replicated=replicated,
        db_atomic=db_atomic,
        uuid=uuid,
    )

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(0)

    node_another_bucket.query("ALTER TABLE s3.test ATTACH PARTITION '2020-01-03'")

    assert node_another_bucket.query(
        "SELECT count(*) FROM s3.test FORMAT Values"
    ) == "({})".format(1)
