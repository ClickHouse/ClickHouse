import logging
import random
import string
import time

import pytest
from helpers.cluster import ClickHouseCluster

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance("node", main_configs=[
            "configs/config.d/storage_conf.xml",
            "configs/config.d/bg_processing_pool_conf.xml",
            "configs/config.d/log_conf.xml"], user_configs=[], with_minio=True, stay_alive=True)
        cluster.add_instance("node_another_bucket", main_configs=[
            "configs/config.d/storage_conf_another_bucket.xml",
            "configs/config.d/bg_processing_pool_conf.xml",
            "configs/config.d/log_conf.xml"], user_configs=[], stay_alive=True)
        cluster.add_instance("node_another_bucket_path", main_configs=[
            "configs/config.d/storage_conf_another_bucket_path.xml",
            "configs/config.d/bg_processing_pool_conf.xml",
            "configs/config.d/log_conf.xml"], user_configs=[], stay_alive=True)
        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        yield cluster
    finally:
        cluster.shutdown()


def random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}',{})".format(x, y, z, 0) for x, y, z in data])


def create_table(node, table_name, additional_settings=None):
    node.query("CREATE DATABASE IF NOT EXISTS s3 ENGINE = Ordinary")

    create_table_statement = """
        CREATE TABLE s3.{} (
            dt Date,
            id Int64,
            data String,
            counter Int64,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=MergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS
            storage_policy='s3',
            old_parts_lifetime=600,
            index_granularity=512
        """.format(table_name)

    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings

    node.query(create_table_statement)


def purge_s3(cluster, bucket):
    minio = cluster.minio_client
    for obj in list(minio.list_objects(bucket, recursive=True)):
        minio.remove_object(bucket, obj.object_name)


def drop_s3_metadata(node):
    node.exec_in_container(['bash', '-c', 'rm -rf /var/lib/clickhouse/disks/s3/*'], user='root')


def drop_shadow_information(node):
    node.exec_in_container(['bash', '-c', 'rm -rf /var/lib/clickhouse/shadow/*'], user='root')


def create_restore_file(node, revision=0, bucket=None, path=None):
    add_restore_option = 'echo -en "{}\n" >> /var/lib/clickhouse/disks/s3/restore'
    node.exec_in_container(['bash', '-c', add_restore_option.format(revision)], user='root')
    if bucket:
        node.exec_in_container(['bash', '-c', add_restore_option.format(bucket)], user='root')
    if path:
        node.exec_in_container(['bash', '-c', add_restore_option.format(path)], user='root')


def get_revision_counter(node, backup_number):
    return int(node.exec_in_container(['bash', '-c', 'cat /var/lib/clickhouse/disks/s3/shadow/{}/revision.txt'.format(backup_number)], user='root'))


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield

    node_names = ["node", "node_another_bucket", "node_another_bucket_path"]

    for node_name in node_names:
        node = cluster.instances[node_name]
        node.query("DROP TABLE IF EXISTS s3.test NO DELAY")

        drop_s3_metadata(node)
        drop_shadow_information(node)

    buckets = [cluster.minio_bucket, cluster.minio_bucket_2]
    for bucket in buckets:
        purge_s3(cluster, bucket)


def test_full_restore(cluster):
    node = cluster.instances["node"]

    create_table(node, "test")

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-04', 4096, -1)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096, -1)))

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3.test")

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)

    node.stop_clickhouse()
    drop_s3_metadata(node)
    node.start_clickhouse()

    # All data is removed.
    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(0)

    node.stop_clickhouse()
    create_restore_file(node)
    node.start_clickhouse(10)

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)


def test_restore_another_bucket_path(cluster):
    node = cluster.instances["node"]

    create_table(node, "test")

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-04', 4096, -1)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096, -1)))

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3.test")

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_table(node_another_bucket, "test")

    node_another_bucket.stop_clickhouse()
    create_restore_file(node_another_bucket, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)

    node_another_bucket_path = cluster.instances["node_another_bucket_path"]

    create_table(node_another_bucket_path, "test")

    node_another_bucket_path.stop_clickhouse()
    create_restore_file(node_another_bucket_path, bucket="root2", path="data")
    node_another_bucket_path.start_clickhouse(10)

    assert node_another_bucket_path.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node_another_bucket_path.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)


def test_restore_different_revisions(cluster):
    node = cluster.instances["node"]

    create_table(node, "test")

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-04', 4096, -1)))

    node.query("ALTER TABLE s3.test FREEZE")
    revision1 = get_revision_counter(node, 1)

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-05', 4096, -1)))

    node.query("ALTER TABLE s3.test FREEZE")
    revision2 = get_revision_counter(node, 2)

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3.test")

    node.query("ALTER TABLE s3.test FREEZE")
    revision3 = get_revision_counter(node, 3)

    assert node.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node.query("SELECT count(*) from system.parts where table = 'test'") == '5\n'

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_table(node_another_bucket, "test")

    # Restore to revision 1 (2 parts).
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    create_restore_file(node_another_bucket, revision=revision1, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT count(*) from system.parts where table = 'test'") == '2\n'

    # Restore to revision 2 (4 parts).
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    create_restore_file(node_another_bucket, revision=revision2, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT count(*) from system.parts where table = 'test'") == '4\n'

    # Restore to revision 3 (4 parts + 1 merged).
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    create_restore_file(node_another_bucket, revision=revision3, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 4)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT count(*) from system.parts where table = 'test'") == '5\n'


def test_restore_mutations(cluster):
    node = cluster.instances["node"]

    create_table(node, "test")

    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3.test VALUES {}".format(generate_values('2020-01-03', 4096, -1)))

    node.query("ALTER TABLE s3.test FREEZE")
    revision_before_mutation = get_revision_counter(node, 1)

    node.query("ALTER TABLE s3.test UPDATE counter = 1 WHERE 1", settings={"mutations_sync": 2})

    node.query("ALTER TABLE s3.test FREEZE")
    revision_after_mutation = get_revision_counter(node, 2)

    node_another_bucket = cluster.instances["node_another_bucket"]

    create_table(node_another_bucket, "test")

    # Restore to revision before mutation.
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    create_restore_file(node_another_bucket, revision=revision_before_mutation, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT sum(counter) FROM s3.test FORMAT Values") == "({})".format(0)

    # Restore to revision after mutation.
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    create_restore_file(node_another_bucket, revision=revision_after_mutation, bucket="root")
    node_another_bucket.start_clickhouse(10)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT sum(counter) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(counter) FROM s3.test WHERE id > 0 FORMAT Values") == "({})".format(4096)

    # Restore to revision in the middle of mutation.
    # Unfinished mutation should be completed after table startup.
    node_another_bucket.stop_clickhouse()
    drop_s3_metadata(node_another_bucket)
    purge_s3(cluster, cluster.minio_bucket_2)
    revision = (revision_before_mutation + revision_after_mutation) // 2
    create_restore_file(node_another_bucket, revision=revision, bucket="root")
    node_another_bucket.start_clickhouse(10)

    # Wait for unfinished mutation completion.
    time.sleep(3)

    assert node_another_bucket.query("SELECT count(*) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(id) FROM s3.test FORMAT Values") == "({})".format(0)
    assert node_another_bucket.query("SELECT sum(counter) FROM s3.test FORMAT Values") == "({})".format(4096 * 2)
    assert node_another_bucket.query("SELECT sum(counter) FROM s3.test WHERE id > 0 FORMAT Values") == "({})".format(4096)
