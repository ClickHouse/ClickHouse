import pytest
import uuid
import time

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node = cluster.add_instance("node", main_configs=["configs/with_delay_config.xml"])


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_undrop_drop_and_undrop_loop(started_cluster):
    # create, drop, undrop, drop, undrop table 5 times
    for _ in range(5):
        table_uuid = str(uuid.uuid1())
        table = f"test_undrop_loop"
        node.query(
            f"CREATE TABLE {table} "
            f"UUID '{table_uuid}' (id Int32) "
            f"Engine=MergeTree() ORDER BY id"
        )

        node.query(f"DROP TABLE {table}")
        node.query(f"UNDROP TABLE {table} UUID '{table_uuid}'")

        node.query(f"DROP TABLE {table}")
        # database_atomic_delay_before_drop_table_sec=3
        time.sleep(6)

        """
        Expect two things:
        1. Table is dropped - UNKNOWN_TABLE in error
        2. Table in process of dropping - Return code: 60.
            The drop task of table ... (uuid) is in progress,
            has been dropped or the database engine doesn't support it
        """
        error = node.query_and_get_error(f"UNDROP TABLE {table} UUID '{table_uuid}'")
        assert "UNKNOWN_TABLE" in error or "The drop task of table" in error
