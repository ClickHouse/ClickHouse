import logging

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV

logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler())


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "s0_0_0",
            main_configs=["configs/cluster.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "node1", "shard": "shard1"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_0_1",
            main_configs=["configs/cluster.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica2", "shard": "shard1"},
            with_zookeeper=True,
        )
        cluster.add_instance(
            "s0_1_0",
            main_configs=["configs/cluster.xml"],
            user_configs=["configs/users.xml"],
            macros={"replica": "replica1", "shard": "shard2"},
            with_zookeeper=True,
        )

        logging.info("Starting cluster...")
        cluster.start()
        logging.info("Cluster started")

        for node_name in ("s0_0_0", "s0_0_1", "s0_1_0"):
            for i in range(1, 3):
                cluster.instances[node_name].query(
                    f"""
                INSERT INTO TABLE FUNCTION file(
                    'file{i}.csv', 'CSV', 's String, i UInt32') VALUES ('file{i}',{i})
                    """
                )

        yield cluster
    finally:
        cluster.shutdown()


def get_query(select: str, cluster: bool, files_nums: str, order_by="ORDER BY (i, s)"):
    if cluster:
        return f"SELECT {select} from fileCluster('my_cluster', 'file{{{files_nums}}}.csv', 'CSV', 's String, i UInt32') {order_by}"
    else:
        return f"SELECT {select} from file('file{{{files_nums}}}.csv', 'CSV', 's String, i UInt32') {order_by}"


def test_select_all(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    local = node.query(get_query("*", False, "1,2"))
    distributed = node.query(get_query("*", True, "1,2"))

    assert TSV(local) == TSV(distributed)


def test_count(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    local = node.query(get_query("count(*)", False, "1,2", ""))
    distributed = node.query(get_query("count(*)", True, "1,2", ""))

    assert TSV(local) == TSV(distributed)


def test_non_existent_cluster(started_cluster):
    node = started_cluster.instances["s0_0_0"]
    error = node.query_and_get_error(
        """
    SELECT count(*) from fileCluster(
        'non_existent_cluster', 'file{1,2}.csv', 'CSV', 's String, i UInt32')
    UNION ALL
    SELECT count(*) from fileCluster(
        'non_existent_cluster', 'file{1,2}.csv', 'CSV', 's String, i UInt32')
    """
    )

    assert "not found" in error


def test_missing_file(started_cluster):
    """
    Select from a list of files, _some_ of them don't exist
    """
    node = started_cluster.instances["s0_0_0"]

    local_with_missing_file = node.query(get_query("*", False, "1,2,3"))
    local_wo_missing_file = node.query(get_query("*", False, "1,2"))

    distributed_with_missing_file = node.query(get_query("*", True, "1,2,3"))
    distributed_wo_missing_file = node.query(get_query("*", True, "1,2"))

    assert TSV(local_with_missing_file) == TSV(distributed_with_missing_file)
    assert TSV(local_wo_missing_file) == TSV(distributed_wo_missing_file)
    assert TSV(local_with_missing_file) == TSV(distributed_wo_missing_file)
    assert TSV(local_wo_missing_file) == TSV(distributed_with_missing_file)


def test_no_such_files(started_cluster):
    """
    Select from a list of files, _none_ of them don't exist
    """
    node = started_cluster.instances["s0_0_0"]

    local = node.query(get_query("*", False, "3,4"))
    distributed = node.query(get_query("*", True, "3,4"))

    assert TSV(local) == TSV(distributed)


def test_schema_inference(started_cluster):
    node = started_cluster.instances["s0_0_0"]

    expected_result = node.query(
        "select * from file('file*.csv', 'CSV', 's String, i UInt32') ORDER BY (i, s)"
    )
    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv') ORDER BY (c1, c2)"
    )
    assert result == expected_result


    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', CSV) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', auto, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', CSV, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', auto, auto, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file*.csv', CSV, auto, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result


def test_parquet_field_ids_are_validated_on_cluster_after_schema_inference(started_cluster):
    node_names = ("s0_0_0", "s0_0_1", "s0_1_0")
    file_name = "parquet_field_ids_on_cluster.parquet"

    started_cluster.instances[node_names[0]].query(
        f"INSERT INTO FUNCTION file('{file_name}', Parquet) SELECT 1::Int64 AS x"
    )
    for node_name in node_names[1:]:
        started_cluster.instances[node_name].query(
            f"INSERT INTO FUNCTION file('{file_name}', Parquet) SELECT 1::Int64 AS y"
        )

    error = started_cluster.instances[node_names[0]].query_and_get_error(
        f"""
        CREATE TABLE parquet_field_ids_on_cluster ON CLUSTER my_cluster
        ENGINE = File(Parquet, '{file_name}')
        SETTINGS output_format_parquet_column_field_ids = {{'x': '1'}}
        """
    )
    assert "BAD_ARGUMENTS" in error


def test_parquet_field_ids_are_validated_on_cluster_for_wildcard_url(started_cluster):
    """
    The wildcard `URL` engine resolves a node-local URL against each DDL worker's own
    endpoint, so a definition-supplied `field_id` map that is valid for the initiator's
    inferred schema may be invalid for a worker's. Every worker must validate its own
    resolved header (`validate_secondary_create`), not skip under `SECONDARY_CREATE`.
    """
    node_names = ("s0_0_0", "s0_0_1", "s0_1_0")
    port = 8880

    for index, node_name in enumerate(node_names):
        node = started_cluster.instances[node_name]
        column = "x" if index == 0 else "y"
        node.query(
            f"INSERT INTO FUNCTION file('url_field_ids/part1.parquet', Parquet) SELECT 1::Int64 AS {column}"
        )
        # `http.server` generates directory index pages, which is what the wildcard
        # expansion lists; every node serves its own `user_files` directory, so the
        # node-local URL below resolves to a different schema on the first node.
        node.exec_in_container(
            [
                "python3",
                "-m",
                "http.server",
                str(port),
                "--directory",
                "/var/lib/clickhouse/user_files",
            ],
            detach=True,
        )
    for node_name in node_names:
        started_cluster.instances[node_name].exec_in_container(
            [
                "bash",
                "-c",
                f"for _ in {{1..100}}; do curl -sf http://127.0.0.1:{port}/url_field_ids/ > /dev/null && exit 0; sleep 0.2; done; exit 1",
            ]
        )

    error = started_cluster.instances[node_names[0]].query_and_get_error(
        f"""
        CREATE TABLE parquet_field_ids_on_cluster_url ON CLUSTER my_cluster
        ENGINE = URL('http://127.0.0.1:{port}/url_field_ids/part*.parquet', Parquet)
        SETTINGS output_format_parquet_column_field_ids = {{'x': '1'}}
        """,
        settings={"allow_experimental_url_wildcard_from_index_pages": 1},
    )
    assert "BAD_ARGUMENTS" in error


def test_format_detection(started_cluster):
    for node_name in ("s0_0_0", "s0_0_1", "s0_1_0"):
        for i in range(1, 3):
            started_cluster.instances[node_name].query(
                f"""
                INSERT INTO TABLE FUNCTION file(
                    'file_for_format_detection_{i}', 'CSV', 's String, i UInt32') VALUES ('file{i}',{i})
                    """
            )

    node = started_cluster.instances["s0_0_0"]
    expected_result = node.query(
        "select * from file('file_for_format_detection*', 'CSV', 's String, i UInt32') ORDER BY (i, s)"
    )

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*') ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*', auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*', auto, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*', auto, 's String, i UInt32') ORDER BY (i, s)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*', auto, auto, auto) ORDER BY (c1, c2)"
    )
    assert result == expected_result

    result = node.query(
        "select * from fileCluster('my_cluster', 'file_for_format_detection*', auto, 's String, i UInt32', auto) ORDER BY (i, s)"
    )
    assert result == expected_result
