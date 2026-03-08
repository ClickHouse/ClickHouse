import csv
import logging
import os
import shutil
import uuid
from email.errors import HeaderParseError

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key
from helpers.mock_servers import start_mock_servers
from helpers.test_tools import TSV
from helpers.utility import random_string

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
S3_DATA = [
    "data/clickhouse/part1.csv",
    "data/clickhouse/part123.csv",
    "data/database/part2.csv",
    "data/database/partition675.csv",
]


def create_buckets_s3(cluster):
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


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s0_0_0",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "node1", "shard": "shard1"},
            with_minio=True,
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_0_1",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica2", "shard": "shard1"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_1_0",
            main_configs=["configs/cluster.xml", "configs/named_collections.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica1", "shard": "shard2"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        create_buckets_s3(cluster)

        run_s3_mocks(cluster)

        yield cluster
    finally:
        shutil.rmtree(os.path.join(SCRIPT_DIR, "data/generated/"))
        cluster.shutdown()


def test_select_all(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        f"""
    SELECT * from s3(
        'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    ORDER BY (name, value, polygon)"""
    )
    # print(pure_s3)
    s3_distributed = node.query(
        f"""
    SELECT * from s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{{clickhouse,database}}/*', 'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') ORDER BY (name, value, polygon)"""
    )
    # print(s3_distributed)

    assert TSV(pure_s3) == TSV(s3_distributed)


def test_count(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        f"""
    SELECT count(*) from s3(
        'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(pure_s3)
    s3_distributed = node.query(
        f"""
    SELECT count(*) from s3Cluster(
        'cluster_simple', 'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distributed)

    assert TSV(pure_s3) == TSV(s3_distributed)


def test_count_macro(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    s3_macro = node.query(
        f"""
    SELECT count(*) from s3Cluster(
        '{{default_cluster_macro}}', 'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distributed)
    s3_distributed = node.query(
        f"""
    SELECT count(*) from s3Cluster(
        'cluster_simple', 'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')"""
    )
    # print(s3_distributed)

    assert TSV(s3_macro) == TSV(s3_distributed)


def test_union_all(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    pure_s3 = node.query(
        f"""
    SELECT * FROM
    (
        SELECT * from s3(
            'http://minio1:9001/root/data/{{clickhouse,database}}/*',
            'minio', '{minio_secret_key}', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
        UNION ALL
        SELECT * from s3(
            'http://minio1:9001/root/data/{{clickhouse,database}}/*',
            'minio', '{minio_secret_key}', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    )
    ORDER BY (name, value, polygon)
    """
    )
    # print(pure_s3)
    s3_distributed = node.query(
        f"""
    SELECT * FROM
    (
        SELECT * from s3Cluster(
            'cluster_simple',
            'http://minio1:9001/root/data/{{clickhouse,database}}/*', 'minio', '{minio_secret_key}', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
        UNION ALL
        SELECT * from s3Cluster(
            'cluster_simple',
            'http://minio1:9001/root/data/{{clickhouse,database}}/*', 'minio', '{minio_secret_key}', 'CSV',
            'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    )
    ORDER BY (name, value, polygon)
    """
    )
    # print(s3_distributed)

    assert TSV(pure_s3) == TSV(s3_distributed)


def test_wrong_cluster(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    error = node.query_and_get_error(
        f"""
    SELECT count(*) from s3Cluster(
        'non_existent_cluster',
        'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    UNION ALL
    SELECT count(*) from s3Cluster(
        'non_existent_cluster',
        'http://minio1:9001/root/data/{{clickhouse,database}}/*',
        'minio', '{minio_secret_key}', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    """
    )

    assert "not found" in error


def test_ambiguous_join(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    result = node.query(
        f"""
    SELECT l.name, r.value from s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{{clickhouse,database}}/*', 'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') as l
    JOIN s3Cluster(
        'cluster_simple',
        'http://minio1:9001/root/data/{{clickhouse,database}}/*', 'minio', '{minio_secret_key}', 'CSV',
        'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))') as r
    ON l.name = r.name
    """
    )
    assert "AMBIGUOUS_COLUMN_NAME" not in result


def test_skip_unavailable_shards(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    result = node.query(
        f"""
    SELECT count(*) from s3Cluster(
        'cluster_non_existent_port',
        'http://minio1:9001/root/data/clickhouse/part1.csv',
        'minio', '{minio_secret_key}', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    SETTINGS skip_unavailable_shards = 1
    """
    )

    assert result == "10\n"


def test_unset_skip_unavailable_shards(started_cluster):
    # Although skip_unavailable_shards is not set, cluster table functions should always skip unavailable shards.
    node = started_cluster.instances["s0_0_0"]
    result = node.query(
        f"""
    SELECT count(*) from s3Cluster(
        'cluster_non_existent_port',
        'http://minio1:9001/root/data/clickhouse/part1.csv',
        'minio', '{minio_secret_key}', 'CSV', 'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))')
    """
    )

    assert result == "10\n"


def test_distributed_insert_select_with_replicated(started_cluster):
    first_replica_first_shard = started_cluster.instances["s0_0_0"]
    second_replica_first_shard = started_cluster.instances["s0_0_1"]

    first_replica_first_shard.query(
        """DROP TABLE IF EXISTS insert_select_replicated_local ON CLUSTER 'first_shard' SYNC;"""
    )

    first_replica_first_shard.query(
        """
    CREATE TABLE insert_select_replicated_local ON CLUSTER 'first_shard' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/insert_select_with_replicated', '{replica}')
    ORDER BY (a, b);
        """
    )

    for replica in [first_replica_first_shard, second_replica_first_shard]:
        replica.query(
            """
            SYSTEM STOP FETCHES;
            """
        )
        replica.query(
            """
            SYSTEM STOP MERGES;
            """
        )

    first_replica_first_shard.query(
        f"""
    INSERT INTO insert_select_replicated_local SELECT * FROM s3Cluster(
        'first_shard',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64'
    ) SETTINGS parallel_distributed_insert_select=1;
        """
    )

    for replica in [first_replica_first_shard, second_replica_first_shard]:
        replica.query(
            """
            SYSTEM FLUSH LOGS;
            """
        )

    assert (
        int(
            second_replica_first_shard.query(
                """SELECT count(*) FROM system.query_log WHERE not is_initial_query and query ilike '%s3Cluster%';"""
            ).strip()
        )
        != 0
    )

    # Check whether we inserted at least something
    assert (
        int(
            second_replica_first_shard.query(
                """SELECT count(*) FROM insert_select_replicated_local;"""
            ).strip()
        )
        != 0
    )

    first_replica_first_shard.query(
        """DROP TABLE IF EXISTS insert_select_replicated_local ON CLUSTER 'first_shard' SYNC;"""
    )


def test_parallel_distributed_insert_select_with_schema_inference(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    node.query(
        """DROP TABLE IF EXISTS parallel_insert_select ON CLUSTER 'first_shard' SYNC;"""
    )

    node.query(
        """
    CREATE TABLE parallel_insert_select ON CLUSTER 'first_shard' (a String, b UInt64)
    ENGINE=ReplicatedMergeTree('/clickhouse/tables/{shard}/test_parallel_distributed_insert_select_with_schema_inference', '{replica}')
    ORDER BY (a, b);
        """
    )

    node.query(
        f"""
    INSERT INTO parallel_insert_select SELECT * FROM s3Cluster(
        'first_shard',
        'http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV'
    ) SETTINGS parallel_distributed_insert_select=1, use_structure_from_insertion_table_in_table_functions=0;
        """
    )

    node.query("SYSTEM SYNC REPLICA parallel_insert_select")

    actual_count = int(
        node.query(
            f"SELECT count() FROM s3('http://minio1:9001/root/data/generated/*.csv', 'minio', '{minio_secret_key}', 'CSV','a String, b UInt64')"
        )
    )

    count = int(node.query("SELECT count() FROM parallel_insert_select"))
    assert count == actual_count


def test_cluster_with_header(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    assert (
        node.query(
            "SELECT * from s3('http://resolver:8080/bucket/key.csv', headers(MyCustomHeader = 'SomeValue'))"
        )
        == "SomeValue\n"
    )
    assert (
        node.query(
            "SELECT * from s3('http://resolver:8080/bucket/key.csv', headers(MyCustomHeader = 'SomeValue'), 'CSV')"
        )
        == "SomeValue\n"
    )
    assert (
        node.query(
            "SELECT * from s3Cluster('cluster_simple', 'http://resolver:8080/bucket/key.csv', headers(MyCustomHeader = 'SomeValue'))"
        )
        == "SomeValue\n"
    )
    assert (
        node.query(
            "SELECT * from s3Cluster('cluster_simple', 'http://resolver:8080/bucket/key.csv', headers(MyCustomHeader = 'SomeValue'), 'CSV')"
        )
        == "SomeValue\n"
    )


def test_cluster_with_named_collection(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    pure_s3 = node.query("""SELECT * from s3(test_s3) ORDER BY (c1, c2, c3)""")

    s3_cluster = node.query(
        """SELECT * from s3Cluster(cluster_simple, test_s3) ORDER BY (c1, c2, c3)"""
    )

    assert TSV(pure_s3) == TSV(s3_cluster)

    s3_cluster = node.query(
        """SELECT * from s3Cluster(cluster_simple, test_s3, structure='auto') ORDER BY (c1, c2, c3)"""
    )

    assert TSV(pure_s3) == TSV(s3_cluster)


def test_cluster_format_detection(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    expected_desc_result = node.query(
        f"desc s3('http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', 'CSV')"
    )

    desc_result = node.query(
        f"desc s3('http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}')"
    )

    assert expected_desc_result == desc_result

    expected_result = node.query(
        f"SELECT * FROM s3('http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', 'CSV', 'a String, b UInt64') order by a, b"
    )

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}') order by c1, c2"
    )

    assert result == expected_result

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/generated/*', 'minio', '{minio_secret_key}', auto, 'a String, b UInt64') order by a, b"
    )

    assert result == expected_result


def test_cluster_default_expression(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    node.query(
        f"insert into function s3('http://minio1:9001/root/data/data1', 'minio', '{minio_secret_key}', JSONEachRow) select 1 as id settings s3_truncate_on_insert=1"
    )
    node.query(
        f"insert into function s3('http://minio1:9001/root/data/data2', 'minio', '{minio_secret_key}', JSONEachRow) select * from numbers(0) settings s3_truncate_on_insert=1"
    )
    node.query(
        f"insert into function s3('http://minio1:9001/root/data/data3', 'minio', '{minio_secret_key}', JSONEachRow) select 2 as id settings s3_truncate_on_insert=1"
    )

    expected_result = node.query(
        f"SELECT * FROM s3('http://minio1:9001/root/data/data{{1,2,3}}', 'minio', '{minio_secret_key}', 'JSONEachRow', 'id UInt32, date Date DEFAULT 18262') order by id"
    )

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/data{{1,2,3}}', 'minio', '{minio_secret_key}', 'JSONEachRow', 'id UInt32, date Date DEFAULT 18262') order by id"
    )

    assert result == expected_result

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/data{{1,2,3}}', 'minio', '{minio_secret_key}', 'auto', 'id UInt32, date Date DEFAULT 18262') order by id"
    )

    assert result == expected_result

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/data{{1,2,3}}', 'minio', '{minio_secret_key}', 'JSONEachRow', 'id UInt32, date Date DEFAULT 18262', 'auto') order by id"
    )

    assert result == expected_result

    result = node.query(
        f"SELECT * FROM s3Cluster(cluster_simple, 'http://minio1:9001/root/data/data{{1,2,3}}', 'minio', '{minio_secret_key}', 'auto', 'id UInt32, date Date DEFAULT 18262', 'auto') order by id"
    )

    assert result == expected_result

    result = node.query(
        "SELECT * FROM s3Cluster(cluster_simple, test_s3_with_default) order by id"
    )

    assert result == expected_result


@pytest.mark.parametrize("allow_experimental_analyzer", [0, 1])
@pytest.mark.parametrize("use_partition_strategy", [False, True])
def test_hive_partitioning(started_cluster, allow_experimental_analyzer, use_partition_strategy):
    node = started_cluster.instances["s0_0_0"]

    data_path = f"root/data/hive_{allow_experimental_analyzer}/{random_string(6)}"

    if use_partition_strategy:
        node.query(
            f"""
            INSERT
                INTO FUNCTION s3(
                        'http://minio1:9001/{data_path}',
                        'minio',
                        '{minio_secret_key}',
                        format='Parquet',
                        partition_strategy='hive',
                        structure='key Int32, value Int32')
                    PARTITION BY key
                SELECT number+1 as key, number+1 as value
                FROM system.numbers
                LIMIT 4
            """
        )
    else:
        for i in range(1, 5):
            node.query(
                f"""
                INSERT
                    INTO FUNCTION s3('http://minio1:9001/{data_path}/key={i}/data.parquet', 'minio', '{minio_secret_key}', 'Parquet', 'key Int32, value Int32')
                    SELECT {i}, {i}
                """
            )

    # With use_hive_partitioning=1 and use_hive_partitioning=0 we have no 'key' column
    if not use_partition_strategy:
        query_id_full = str(uuid.uuid4())
        result = node.query(
            f"""
            SELECT count()
                FROM s3('http://minio1:9001/{data_path}/key=**.parquet', 'minio', '{minio_secret_key}', 'Parquet', 'key Int32, value Int32')
                WHERE key <= 2
                FORMAT TSV
                SETTINGS enable_filesystem_cache = 0, use_query_cache = 0, use_cache_for_count_from_files = 0, use_hive_partitioning = 0, allow_experimental_analyzer={allow_experimental_analyzer}
            """,
            query_id=query_id_full,
        )
        result = int(result)
        assert result == 2

    query_id_optimized = str(uuid.uuid4())
    result = node.query(
        f"""
        SELECT count()
            FROM s3('http://minio1:9001/{data_path}/key=**.parquet', 'minio', '{minio_secret_key}', 'Parquet', 'key Int32, value Int32')
            WHERE key <= 2
            FORMAT TSV
            SETTINGS enable_filesystem_cache = 0, use_query_cache = 0, use_cache_for_count_from_files = 0, use_hive_partitioning = 1, allow_experimental_analyzer={allow_experimental_analyzer}
        """,
        query_id=query_id_optimized,
    )
    result = int(result)
    assert result == 2

    if not use_partition_strategy:
        query_id_cluster_full = str(uuid.uuid4())
        result = node.query(
            f"""
            SELECT count()
                FROM s3Cluster(cluster_simple, 'http://minio1:9001/{data_path}/key=**.parquet', 'minio', '{minio_secret_key}', 'Parquet', 'key Int32, value Int32')
                WHERE key <= 2
                FORMAT TSV
                SETTINGS enable_filesystem_cache = 0, use_query_cache = 0, use_cache_for_count_from_files = 0, use_hive_partitioning = 0, allow_experimental_analyzer={allow_experimental_analyzer}
            """,
            query_id=query_id_cluster_full,
        )
        result = int(result)
        assert result == 2

    query_id_cluster_optimized = str(uuid.uuid4())
    result = node.query(
        f"""
        SELECT count()
            FROM s3Cluster(cluster_simple, 'http://minio1:9001/{data_path}/key=**.parquet', 'minio', '{minio_secret_key}', 'Parquet', 'key Int32, value Int32')
            WHERE key <= 2
            FORMAT TSV
            SETTINGS enable_filesystem_cache = 0, use_query_cache = 0, use_cache_for_count_from_files = 0, use_hive_partitioning = 1, allow_experimental_analyzer={allow_experimental_analyzer}
        """,
        query_id=query_id_cluster_optimized,
    )
    result = int(result)
    assert result == 2

    node.query("SYSTEM FLUSH LOGS ON CLUSTER 'cluster_simple'")

    event = "EngineFileLikeReadFiles"

    if not use_partition_strategy:
        full_traffic = node.query(
            f"""
            SELECT sum(ProfileEvents['{event}'])
                FROM clusterAllReplicas(cluster_simple, system.query_log)
                WHERE type='QueryFinish' AND initial_query_id='{query_id_full}'
                FORMAT TSV
            """
        )
        full_traffic = int(full_traffic)
        assert full_traffic == 4

    optimized_traffic = node.query(
        f"""
        SELECT sum(ProfileEvents['{event}'])
            FROM clusterAllReplicas(cluster_simple, system.query_log)
            WHERE type='QueryFinish' AND initial_query_id='{query_id_optimized}'
            FORMAT TSV
        """
    )
    optimized_traffic = int(optimized_traffic)
    assert optimized_traffic == 2

    if not use_partition_strategy:
        cluster_full_traffic = node.query(
            f"""
            SELECT sum(ProfileEvents['{event}'])
                FROM clusterAllReplicas(cluster_simple, system.query_log)
                WHERE type='QueryFinish' AND initial_query_id='{query_id_cluster_full}'
                FORMAT TSV
            """
        )
        cluster_full_traffic = int(cluster_full_traffic)
        assert cluster_full_traffic == full_traffic

    cluster_optimized_traffic = node.query(
        f"""
        SELECT sum(ProfileEvents['{event}'])
            FROM clusterAllReplicas(cluster_simple, system.query_log)
            WHERE type='QueryFinish' AND initial_query_id='{query_id_cluster_optimized}'
            FORMAT TSV
        """
    )
    cluster_optimized_traffic = int(cluster_optimized_traffic)
    assert cluster_optimized_traffic == optimized_traffic
