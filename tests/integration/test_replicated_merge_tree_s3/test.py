import logging
import os
import random
import string
import tempfile

import minio
import pytest

import helpers.cluster
import helpers.utility


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))


@pytest.fixture(scope="module")
def cluster():
    try:
        with tempfile.TemporaryDirectory() as d:
            cluster = helpers.cluster.ClickHouseCluster(__file__)
            main_configs = ["configs/config.d/storage_conf.xml"]

            if os.environ.get("CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE"):
                assert os.environ["CLICKHOUSE_AWS_HOST_NAME"] + "/" + os.environ["CLICKHOUSE_AWS_BUCKET"] in os.environ["CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE"]
                new_config_name = os.path.join(d, "storage_conf.xml")
                helpers.utility.replace_xml_by_xpath(
                    os.path.join(SCRIPT_DIR, main_configs[0]),
                    new_config_name,
                    replace_text={
                        "//storage_configuration/disks/s3/endpoint": os.environ["CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE"],
                        "//storage_configuration/disks/s3/access_key_id": os.environ["CLICKHOUSE_AWS_ACCESS_KEY_ID"],
                        "//storage_configuration/disks/s3/secret_access_key": os.environ["CLICKHOUSE_AWS_SECRET_ACCESS_KEY"],
                        "//storage_configuration/disks/s3/region": os.environ["CLICKHOUSE_AWS_REGION"],
                    }
                )
                main_configs[0] = new_config_name

            cluster.add_instance(
                "node1",
                main_configs=main_configs,
                macros={"replica": "1"},
                with_minio=True,
                with_zookeeper=True,
            )
            cluster.add_instance(
                "node2",
                main_configs=main_configs,
                macros={"replica": "2"},
                with_zookeeper=True,
            )
            cluster.add_instance(
                "node3",
                main_configs=main_configs,
                macros={"replica": "3"},
                with_zookeeper=True,
            )

            logging.info("Starting cluster...")
            cluster.start()
            logging.info("Cluster started")

            if os.environ.get("CLICKHOUSE_AWS_ENDPOINT_URL_OVERRIDE"):
                cluster.minio_client = minio.Minio(
                    os.environ["CLICKHOUSE_AWS_HOST_NAME"],
                    access_key=os.environ["CLICKHOUSE_AWS_ACCESS_KEY_ID"],
                    secret_key=os.environ["CLICKHOUSE_AWS_SECRET_ACCESS_KEY"],
                    region=os.environ["CLICKHOUSE_AWS_REGION"],
                    secure=False
                )
                cluster.minio_bucket = os.environ["CLICKHOUSE_AWS_BUCKET"]

            yield cluster
    finally:
        cluster.shutdown()


FILES_OVERHEAD = 1
FILES_OVERHEAD_PER_COLUMN = 2  # Data and mark files
FILES_OVERHEAD_PER_PART_WIDE = FILES_OVERHEAD_PER_COLUMN * 3 + 2 + 6 + 1
FILES_OVERHEAD_PER_PART_COMPACT = 10 + 1


def random_string(length):
    letters = string.ascii_letters
    return "".join(random.choice(letters) for i in range(length))


def generate_values(date_str, count, sign=1):
    data = [[date_str, sign * (i + 1), random_string(10)] for i in range(count)]
    data.sort(key=lambda tup: tup[1])
    return ",".join(["('{}',{},'{}')".format(x, y, z) for x, y, z in data])


def create_table(cluster, additional_settings=None):
    create_table_statement = """
        CREATE TABLE s3_test ON CLUSTER cluster(
            dt Date,
            id Int64,
            data String,
            INDEX min_max (id) TYPE minmax GRANULARITY 3
        ) ENGINE=ReplicatedMergeTree()
        PARTITION BY dt
        ORDER BY (dt, id)
        SETTINGS storage_policy='s3'
        """
    if additional_settings:
        create_table_statement += ","
        create_table_statement += additional_settings

    list(cluster.instances.values())[0].query(create_table_statement)


@pytest.fixture(autouse=True)
def drop_table(cluster):
    yield
    for node in list(cluster.instances.values()):
        node.query("DROP TABLE IF EXISTS s3_test")

    minio = cluster.minio_client
    # Remove extra objects to prevent tests cascade failing
    for obj in list(minio.list_objects(cluster.minio_bucket, "data/")):
        minio.remove_object(cluster.minio_bucket, obj.object_name)


@pytest.mark.parametrize(
    "min_rows_for_wide_part,files_per_part",
    [(0, FILES_OVERHEAD_PER_PART_WIDE), (8192, FILES_OVERHEAD_PER_PART_COMPACT)],
)
def test_insert_select_replicated(cluster, min_rows_for_wide_part, files_per_part):
    create_table(
        cluster,
        additional_settings="min_rows_for_wide_part={}".format(min_rows_for_wide_part),
    )

    all_values = ""
    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        values = generate_values("2020-01-0" + str(node_idx), 4096)
        node.query(
            "INSERT INTO s3_test VALUES {}".format(values),
            settings={"insert_quorum": 3},
        )
        if node_idx != 1:
            all_values += ","
        all_values += values

    for node_idx in range(1, 4):
        node = cluster.instances["node" + str(node_idx)]
        assert (
            node.query(
                "SELECT * FROM s3_test order by dt, id FORMAT Values",
                settings={"select_sequential_consistency": 1},
            )
            == all_values
        )

    minio = cluster.minio_client
    assert len(list(minio.list_objects(cluster.minio_bucket, "data/"))) == 3 * (
        FILES_OVERHEAD + files_per_part * 3
    )
