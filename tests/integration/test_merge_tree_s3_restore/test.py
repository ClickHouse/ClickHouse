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
        cluster.add_instance("node", main_configs=["configs/config.d/storage_conf.xml",
                                                   "configs/config.d/bg_processing_pool_conf.xml",
                                                   "configs/config.d/log_conf.xml"], user_configs=[], with_minio=True, stay_alive=True)
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
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, table_name, additional_settings=None):
    node = cluster.instances["node"]

    create_table_statement = """
        CREATE TABLE {} (
            dt Date,
            id Int64,
            data String,
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


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    node = cluster.instances["node"]
    minio = cluster.minio_client

    node.query("DROP TABLE IF EXISTS s3_test NO DELAY")

    for obj in list(minio.list_objects(cluster.minio_bucket, 'data/')):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


# Restore to the same bucket and path with latest revision.
def test_simple_full_restore(cluster):
    create_table(cluster, "s3_test")

    node = cluster.instances["node"]

    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-03', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-04', 4096, -1)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-05', 4096)))
    node.query("INSERT INTO s3_test VALUES {}".format(generate_values('2020-01-05', 4096, -1)))

    # To ensure parts have merged
    node.query("OPTIMIZE TABLE s3_test")

    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "({})".format(0)

    node.stop_clickhouse()
    node.exec_in_container(['bash', '-c', 'rm -r /var/lib/clickhouse/disks/s3/*'], user='root')
    node.start_clickhouse()

    # All data is removed.
    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "({})".format(0)

    node.stop_clickhouse()
    node.exec_in_container(['bash', '-c', 'touch /var/lib/clickhouse/disks/s3/restore'], user='root')
    node.start_clickhouse()

    assert node.query("SELECT count(*) FROM s3_test FORMAT Values") == "({})".format(4096 * 4)
    assert node.query("SELECT sum(id) FROM s3_test FORMAT Values") == "({})".format(0)
