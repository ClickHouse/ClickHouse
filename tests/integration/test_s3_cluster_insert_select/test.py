import csv
import io
import logging
import os
import shutil

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
S3_DATA = []

generated_rows = 0

def create_buckets_s3(cluster):
    global generated_rows

    minio = cluster.minio_client

    for file_number in range(100):
        file_name = f"data/generated/file_{file_number}.csv"
        os.makedirs(os.path.join(SCRIPT_DIR, "data/generated/"), exist_ok=True)
        S3_DATA.append(file_name)
        with open(os.path.join(SCRIPT_DIR, file_name), "w+", encoding="utf-8") as f:
            # a String, b UInt64
            data = []

            # Make all files a bit different
            for number in range(100 + file_number):
                data.append(
                    ["str_" + str(number + file_number) * 10, number + file_number]
                )
                generated_rows += 1

            writer = csv.writer(f)
            writer.writerows(data)

    for file in S3_DATA:
        minio.fput_object(
            bucket_name=cluster.minio_bucket,
            object_name=file,
            file_path=os.path.join(SCRIPT_DIR, file),
        )
    for obj in minio.list_objects(cluster.minio_bucket, recursive=True):
        print(obj.object_name)


def run_s3_mocks(started_cluster):
    script_dir = os.path.join(os.path.dirname(__file__), "s3_mocks")
    start_mock_servers(
        started_cluster,
        script_dir,
        [
            ("s3_mock.py", "resolver", "8080"),
        ],
    )


cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    with_minio=True,
    with_zookeeper=True,
    macros={"replica": "node1"},
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    macros={"replica": "node2"},
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/cluster.xml"],
    user_configs=["configs/users.xml"],
    stay_alive=True,
    with_zookeeper=True,
    macros={"replica": "node3"},
)

@pytest.fixture(scope="module")
def started_cluster():
    try:

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        run_s3_mocks(cluster)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated/"))
        cluster.shutdown()


def test_distributed_insert_select_to_rmt(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"
    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/b0612a53-5f58-4df0-a28f-a154e0cdb797/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} SELECT * FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == generated_rows
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


def test_distributed_insert_select_to_rmt_limit(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"
    limit = generated_rows - 1000

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/32c614a9-13af-43c5-848c-a3f62a78e390/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} SELECT * FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) LIMIT {limit} SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == limit
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


def test_distributed_insert_select_to_rmt_where(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/32c614a9-13af-43c5-848c-a3f62a78e390/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} SELECT * FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) WHERE b = 100
    SETTINGS
        parallel_distributed_insert_select=2,
        -- disable deduplication since all rows identical, and if the batch size will be the same size on different nodes it will be deduplicated
        insert_deduplicate=0;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == 99
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


def test_distributed_insert_select_to_rmt_cte_const(started_cluster):
    table = "t_rmt_target"
    cluster_name = "cluster_1_shard_3_replicas"

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )

    node1.query(
        f"""
    CREATE TABLE {table} ON CLUSTER {cluster_name} (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/178ed413-b055-46eb-9657-442811aef552/{table}', '{{replica}}')
    ORDER BY (a, b);
        """
    )

    node1.query(
        f"""
    INSERT INTO {table} WITH 1 + 1 as two SELECT a, b + (select sum(number) from numbers(10) where number < 2) FROM s3Cluster(
        '{cluster_name}',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=2;
        """
    )

    node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

    assert (
        int(
            node1.query(
                f"SELECT count(*) FROM {table};"
            ).strip()
        ) == generated_rows
    )

    node1.query(
        f"""DROP TABLE IF EXISTS {table} ON CLUSTER '{cluster_name}' SYNC;"""
    )


@pytest.mark.parametrize(
    "parallel_mode,distributed_destination",
    [(0, False), (2, False), (2, True)],
)
@pytest.mark.parametrize("defaults_for_omitted_fields", [0, 1])
def test_auto_structure_now64_default(
    started_cluster, parallel_mode, distributed_destination, defaults_for_omitted_fields
):
    cluster_name = "cluster_1_shard_3_replicas"
    table = f"t_s3_cluster_now64_{parallel_mode}_{int(distributed_destination)}_{defaults_for_omitted_fields}"
    distributed_table = f"{table}_distributed"
    object_name = f"data/defaults/{table}.jsonl"
    data = (
        b'{"id":1,"b":99}\n'
        b'{"id":2,"synced_at":"2024-01-02 03:04:05.123","b":99}\n'
    )
    started_cluster.minio_client.put_object(
        started_cluster.minio_bucket, object_name, io.BytesIO(data), len(data)
    )

    node1.query(f"DROP TABLE IF EXISTS {distributed_table} ON CLUSTER {cluster_name} SYNC")
    node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER {cluster_name} SYNC")
    try:
        node1.query(
            f"""
            CREATE TABLE {table} ON CLUSTER {cluster_name}
            (
                id UInt32,
                synced_at DateTime64(3, 'UTC') DEFAULT now64(3),
                a UInt32,
                b UInt32 DEFAULT a + 7
            )
            ENGINE = ReplicatedMergeTree(
                '/clickhouse/tables/issue_8987/{table}', '{{replica}}'
            ) ORDER BY id
            """
        )
        destination = table
        if distributed_destination:
            node1.query(
                f"""
                CREATE TABLE {distributed_table} ON CLUSTER {cluster_name}
                AS {table}
                ENGINE = Distributed('{cluster_name}', 'default', '{table}')
                """
            )
            destination = distributed_table

        before_ms = int(node1.query("SELECT toUnixTimestamp64Milli(now64(3))"))
        node1.query(
            f"""
            INSERT INTO {destination} (id, synced_at, b)
            SELECT `id` AS `id`, `synced_at` AS `synced_at`, `b` AS `b` FROM s3Cluster(
                '{cluster_name}',
                'http://minio1:9001/root/{object_name}',
                'minio', '{minio_secret_key}', 'JSONEachRow', 'auto'
            )
            SETTINGS
                input_format_defaults_for_omitted_fields = {defaults_for_omitted_fields},
                use_structure_from_insertion_table_in_table_functions = 2,
                parallel_distributed_insert_select = {parallel_mode},
                insert_distributed_sync = 1
            """
        )
        after_ms = int(node1.query("SELECT toUnixTimestamp64Milli(now64(3))"))
        node1.query(f"SYSTEM SYNC REPLICA ON CLUSTER {cluster_name} {table}")

        for node in (node1, node2, node3):
            rows = [
                tuple(map(int, row.split("\t")))
                for row in node.query(
                    f"SELECT id, toUnixTimestamp64Milli(synced_at), a, b FROM {table} ORDER BY id"
                ).splitlines()
            ]
            assert len(rows) == 2
            assert rows[0][0] == 1
            if defaults_for_omitted_fields:
                assert before_ms <= rows[0][1] <= after_ms
            else:
                assert rows[0][1] == 0
            assert rows[0][2:] == (0, 99)
            assert rows[1] == (2, 1704164645123, 0, 99)
    finally:
        node1.query(f"DROP TABLE IF EXISTS {distributed_table} ON CLUSTER {cluster_name} SYNC")
        node1.query(f"DROP TABLE IF EXISTS {table} ON CLUSTER {cluster_name} SYNC")
