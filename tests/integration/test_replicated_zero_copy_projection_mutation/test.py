import logging
import pathlib
import time
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.mock_servers import start_s3_mock
from helpers.test_tools import assert_eq_with_retry


def args_to_dict(**kwargs):
    return kwargs


@pytest.fixture(scope="module")
def cluster():
    try:
        cluster = ClickHouseCluster(__file__)

        kwargs = args_to_dict(
            main_configs=[
                "configs/config.d/storage_conf.xml",
            ],
            user_configs=[
                "configs/config.d/users.xml",
            ],
            with_minio=True,
            with_zookeeper=True,
            stay_alive=True,
        )

        cluster.add_instance("node1", **kwargs)
        cluster.add_instance("node2", **kwargs)

        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def all_cluster_nodes(cluster):
    yield cluster.instances.values()


@pytest.fixture(scope="module")
def first_cluster_node(cluster):
    yield cluster.instances["node1"]


@pytest.fixture(scope="module")
def second_cluster_node(cluster):
    yield cluster.instances["node2"]


@pytest.fixture(scope="module")
def init_broken_s3(cluster):
    yield start_s3_mock(cluster, "broken_s3", "8081")


@pytest.fixture(scope="function")
def broken_s3(init_broken_s3):
    init_broken_s3.reset()
    yield init_broken_s3


def list_objects(cluster, path="data/", hint="list_objects"):
    minio = cluster.minio_client
    objects = list(minio.list_objects(cluster.minio_bucket, path, recursive=True))
    names = [x.object_name for x in objects]
    names.sort()
    logging.info(f"{hint} ({len(objects)}): {names}")
    return names


def wait_for_delete_s3_objects(cluster, expected, timeout=30):
    while timeout > 0:
        if len(list_objects(cluster, "data/")) == expected:
            return
        timeout -= 1
        time.sleep(1)
    final_listing = list_objects(cluster, "data/")
    assert len(final_listing) == expected, ",".join(final_listing)


def remove_all_s3_objects(cluster):
    minio = cluster.minio_client
    for obj in list_objects(cluster, "data/"):
        minio.remove_object(cluster.minio_bucket, obj)


@pytest.fixture(autouse=True, scope="function")
def clear_minio(cluster):
    try:
        # CH do some writes to the S3 at start. For example, file data/clickhouse_access_check_{server_uuid}.
        # Set the timeout there as 10 sec in order to resolve the race with that file exists.
        wait_for_delete_s3_objects(cluster, 0, timeout=10)
    except:
        # Remove extra objects to prevent tests cascade failing
        remove_all_s3_objects(cluster)

    yield


@contextmanager
def drop_table_guard(nodes, table):
    for node in nodes:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")
    try:
        yield
    finally:
        for node in nodes:
            node.query(f"DROP TABLE IF EXISTS {table} SYNC")


def test_all_projection_files_are_dropped_when_part_is_dropped(
    cluster, first_cluster_node
):
    node = first_cluster_node

    with drop_table_guard([node], "test_all_projection_files_are_dropped"):
        node.query(
            """
            CREATE TABLE test_all_projection_files_are_dropped(a UInt32, b UInt32)
            ENGINE MergeTree()
            ORDER BY a
            SETTINGS storage_policy='s3', old_parts_lifetime=0
            """
        )

        node.query(
            "ALTER TABLE test_all_projection_files_are_dropped ADD projection b_order (SELECT a, b ORDER BY b)"
        )
        node.query(
            "ALTER TABLE test_all_projection_files_are_dropped MATERIALIZE projection b_order"
        )
        objects_empty_table = list_objects(cluster)

        node.query(
            """
            INSERT INTO test_all_projection_files_are_dropped
            VALUES (1, 105), (5, 101), (3, 103), (4, 102), (2, 104)
            """
        )

        node.query(
            "ALTER TABLE test_all_projection_files_are_dropped DROP PARTITION ID 'all'"
        )

        objects_at_the_end = list_objects(cluster)
        assert objects_at_the_end == objects_empty_table


def test_hardlinks_preserved_when_projection_dropped(
    cluster, all_cluster_nodes, first_cluster_node, second_cluster_node
):
    with drop_table_guard(
        all_cluster_nodes, "test_hardlinks_preserved_when_projection_dropped"
    ):
        create_query = """
            CREATE TABLE test_hardlinks_preserved_when_projection_dropped
            (
                a UInt32,
                b UInt32,
                c UInt32,
                PROJECTION projection_order_by_b
               (
                   SELECT a, b ORDER BY b
               )
            )
            ENGINE ReplicatedMergeTree('/clickhouse/tables/test_projection', '{instance}')
            ORDER BY a
            SETTINGS cleanup_delay_period=1, max_cleanup_delay_period=3
        """

        first_node_settings = ", storage_policy='s3', old_parts_lifetime=0"

        # big old_parts_lifetime value makes second node to hold outdated part for us, we make it as broken_on_start
        second_node_settings = ", storage_policy='s3', old_parts_lifetime=10000"

        first_cluster_node.query(create_query + first_node_settings)
        second_cluster_node.query(create_query + second_node_settings)

        objects_empty_table = list_objects(cluster)

        first_cluster_node.query("SYSTEM FLUSH LOGS")
        table_uuid = first_cluster_node.query(
            """
            SELECT uuid FROM system.tables
            WHERE name = 'test_hardlinks_preserved_when_projection_dropped'
            """
        ).strip()

        first_cluster_node.query(
            """
            INSERT INTO test_hardlinks_preserved_when_projection_dropped
            VALUES (1, 105, 1), (5, 101, 1), (3, 103, 1), (4, 102, 1), (2, 104, 1)
            """
        )

        # second_cluster_node will fetch the mutated part when it is ready on first_cluster_node
        second_cluster_node.query("SYSTEM STOP MERGES")

        first_cluster_node.query(
            """
            ALTER TABLE test_hardlinks_preserved_when_projection_dropped
            UPDATE c = 2 where c = 1
            """,
            settings={"mutations_sync": "1"},
        )

        assert_eq_with_retry(
            first_cluster_node, "SELECT COUNT() FROM system.replication_queue", "0"
        )

        # the mutated part is ready on first_cluster_node, second replica just fetches it
        second_cluster_node.query("SYSTEM START MERGES")

        # fist node removed outdated part
        assert_eq_with_retry(
            first_cluster_node,
            """
            SELECT removal_state FROM system.parts
            WHERE name = 'all_0_0_0'
                AND table = 'test_hardlinks_preserved_when_projection_dropped'
                AND not active
            """,
            "",
            retry_count=300,
            sleep_time=1,
        )

        # make sure that alter update made hardlinks inside projection
        hardlinks = (
            first_cluster_node.query(
                f"""
                SELECT value
                FROM system.zookeeper
                WHERE
                path like '/clickhouse/zero_copy/zero_copy_s3/{table_uuid}' AND name = 'all_0_0_0'
                """,
                settings={"allow_unrestricted_reads_from_keeper": "1"},
            )
            .strip()
            .split()
        )
        assert len(hardlinks) > 0, ",".join(hardlinks)
        assert any(["proj/" in x for x in hardlinks]), ",".join(hardlinks)

        part_path_on_second_node = second_cluster_node.query(
            """
            SELECT path FROM system.parts
            WHERE
                name = 'all_0_0_0' AND table = 'test_hardlinks_preserved_when_projection_dropped'
            """
        ).strip()

        # that corrupts outdatated part all_0_0_0
        script = (
            f"INDEX_FILE={part_path_on_second_node}/primary.cidx"
            """
            cp $INDEX_FILE $INDEX_FILE.backup
            echo "unexpected data in metadata file" | cat > $INDEX_FILE
            """
        )
        second_cluster_node.exec_in_container(["bash", "-c", script])

        # corrupted outdatated part all_0_0_0 is detached as broken_on_start
        second_cluster_node.restart_clickhouse()

        second_cluster_node.query(
            "SYSTEM WAIT LOADING PARTS test_hardlinks_preserved_when_projection_dropped"
        )

        second_cluster_node.query("SYSTEM FLUSH LOGS")

        # make sure there is outdated broken-on-start part
        broken_parts = (
            second_cluster_node.query(
                """
                SELECT name, reason, path FROM system.detached_parts
                WHERE
                    table = 'test_hardlinks_preserved_when_projection_dropped'
                """
            )
            .strip()
            .split("\n")
        )
        assert len(broken_parts) == 1, broken_parts
        # style checker black asked to do this. It is crazy
        broken_part_name, reason, broken_part_path_on_second_node = broken_parts[
            0
        ].split("\t")
        assert "broken-on-start" == reason

        script = (
            f"INDEX_FILE={broken_part_path_on_second_node}/primary.cidx"
            """
            mv $INDEX_FILE.backup $INDEX_FILE
            """
        )
        second_cluster_node.exec_in_container(["bash", "-c", script])

        # when detached part is removed, removeSharedRecursive is called
        second_cluster_node.query(
            f"""
            ALTER TABLE test_hardlinks_preserved_when_projection_dropped
            DROP DETACHED PART '{broken_part_name}'
            """,
            settings={"allow_drop_detached": "1"},
        )

        # it is an easy way to read all data in part
        # "0" means corrupted, https://clickhouse.com/docs/en/sql-reference/statements/check-table
        assert (
            "1"
            == first_cluster_node.query(
                """
                CHECK TABLE test_hardlinks_preserved_when_projection_dropped
                """
            ).strip()
        )

        assert (
            "1"
            == second_cluster_node.query(
                """
                CHECK TABLE test_hardlinks_preserved_when_projection_dropped
                """
            ).strip()
        )

        second_cluster_node.query(
            f"""
            ALTER TABLE test_hardlinks_preserved_when_projection_dropped
            DROP PART 'all_0_0_0_1'
            """,
            settings={"alter_sync": 2},
        )

        wait_for_delete_s3_objects(cluster, len(objects_empty_table))

        objects_at_the_end = list_objects(cluster)
        assert objects_at_the_end == objects_empty_table
