import logging
import time
from contextlib import contextmanager

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    stay_alive=True,
    with_zookeeper=False,
    main_configs=[
        "configs/keeper.xml",
        "configs/transactions.xml",
    ],
)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="function")
def test_name(request):
    return request.node.name


@pytest.fixture(scope="function")
def exclusive_table(test_name):
    normalized = (
        test_name.replace("[", "_")
        .replace("]", "_")
        .replace(" ", "_")
        .replace("-", "_")
    )
    return "table_" + normalized


def get_event_select_count():
    return int(
        node.query(
            """
                SELECT value FROM system.events WHERE event = 'SelectQuery';
            """
        )
    )


def get_query_processes_count(query_id):
    q = f"""
            SELECT count() FROM system.processes WHERE query_id = '{query_id}';
        """
    return q


def is_query_running(query_id):
    return 1 == int(node.query(get_query_processes_count(query_id)))


def wait_select_start(query_id):
    assert_eq_with_retry(
        node,
        get_query_processes_count(query_id),
        "1\n",
    )


LOCK_FREE_QUERIES = {
    "detach table": "DETACH TABLE {table};",
    "drop part": "ALTER TABLE {table} DROP PART 'all_1_1_0';",
    "detach part": "ALTER TABLE {table} DETACH PART 'all_1_1_0';",
    "truncate": "TRUNCATE TABLE {table};",
}


@pytest.mark.parametrize(
    "lock_free_query", LOCK_FREE_QUERIES.values(), ids=LOCK_FREE_QUERIES.keys()
)
def test_query_is_lock_free(lock_free_query, exclusive_table):
    node.query(
        f"""
            CREATE TABLE {exclusive_table}
            (a Int64)
            Engine=MergeTree ORDER BY a;
        """
    )
    node.query(
        f"""
            INSERT INTO {exclusive_table} SELECT number FROM numbers(50);
        """
    )

    query_id = "select-" + exclusive_table

    select_handler = node.get_query_request(
        f"""
            SELECT sleepEachRow(3) FROM {exclusive_table} SETTINGS function_sleep_max_microseconds_per_block = 0;
        """,
        query_id=query_id,
    )
    wait_select_start(query_id)

    for _ in [1, 2, 3, 4, 5]:
        assert is_query_running(query_id)
        assert select_handler.process.poll() is None
        time.sleep(1)

    node.query(lock_free_query.format(table=exclusive_table))

    assert is_query_running(query_id)

    if "DETACH TABLE" in lock_free_query:
        result = node.query_and_get_error(
            f"""
                SELECT count() FROM {exclusive_table};
            """
        )
        assert (
            f"Table default.{exclusive_table} does not exist" in result
            or f"Unknown table expression identifier '{exclusive_table}'" in result
        )
    else:
        assert 0 == int(
            node.query(
                f"""
                        SELECT count() FROM {exclusive_table};
                    """
            )
        )


PERMANENT_QUERIES = {
    "truncate": ("TRUNCATE TABLE {table};", 0),
    "detach-partition-all": ("ALTER TABLE {table} DETACH PARTITION ALL;", 0),
    "detach-part": ("ALTER TABLE {table} DETACH PARTITION '20221001';", 49),
    "drop-part": ("ALTER TABLE {table} DROP PART '20220901_1_1_0';", 49),
}


@pytest.mark.parametrize(
    "transaction", ["NoTx", "TxCommit", "TxRollback", "TxNotFinished"]
)
@pytest.mark.parametrize(
    "permanent", PERMANENT_QUERIES.values(), ids=PERMANENT_QUERIES.keys()
)
def test_query_is_permanent(transaction, permanent, exclusive_table):
    node.query(
        f"""
            CREATE TABLE {exclusive_table}
            (
                a Int64,
                date Date
            )
            Engine=MergeTree
            PARTITION BY date
            ORDER BY a;
        """
    )
    node.query(
        f"""
            INSERT INTO {exclusive_table} SELECT number, toDate('2022-09-01') + INTERVAL number DAY FROM numbers(50);
        """
    )

    query_id = "select-" + exclusive_table

    select_handler = node.get_query_request(
        f"""
            SELECT sleepEachRow(3) FROM {exclusive_table} SETTINGS function_sleep_max_microseconds_per_block = 0, max_threads=1;
        """,
        query_id=query_id,
    )
    wait_select_start(query_id)

    for _ in [1, 2, 3, 4, 5]:
        assert is_query_running(query_id)
        assert select_handler.process.poll() is None
        time.sleep(1)

    permanent_query = permanent[0]
    result = permanent[1]
    statement = permanent_query.format(table=exclusive_table)
    if transaction == "TxCommit":
        query = f"""
            BEGIN TRANSACTION;
            {statement}
            COMMIT;
            """
    elif transaction == "TxRollback":
        query = f"""
            BEGIN TRANSACTION;
            {statement}
            ROLLBACK;
            """
        result = 50
    elif transaction == "TxNotFinished":
        query = f"""
            BEGIN TRANSACTION;
            {statement}
            """
        result = 50
    else:
        query = statement

    node.query(query)

    node.restart_clickhouse(kill=True)

    assert result == int(
        node.query(
            f"""
                SELECT count() FROM {exclusive_table};
            """
        )
    )
