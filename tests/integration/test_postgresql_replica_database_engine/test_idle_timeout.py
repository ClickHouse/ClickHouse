import time
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.postgres_utility import (
    PostgresManager,
    check_tables_are_synchronized,
)

cluster = ClickHouseCluster(__file__)
instance = cluster.add_instance(
    "instance",
    main_configs=["configs/log_conf.xml"],
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)

pg_manager = PostgresManager()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        pg_manager.init(
            instance,
            cluster.postgres_ip,
            cluster.postgres_port,
            default_database="postgres_database",
        )
        yield cluster

    finally:
        cluster.shutdown()


@pytest.fixture(autouse=True)
def setup_teardown():
    print("PostgreSQL is available - running test")
    yield  # run test
    pg_manager.restart()


def test_idle_in_transaction_session_timeout(started_cluster):
    """
    Regression test for #80724: idle_in_transaction_session_timeout
    must not kill the snapshot-holder connection during initial sync.
    """

    # 1. Set aggressive timeout on PostgreSQL
    pg_manager.execute(
        "ALTER SYSTEM SET idle_in_transaction_session_timeout = '2s';"
    )
    pg_manager.execute("SELECT pg_reload_conf();")

    # 2. Create a table with enough data to make sync take >2 seconds
    pg_manager.create_postgres_table("test_timeout_table")
    instance.query(
        "INSERT INTO postgres_database.test_timeout_table "
        "SELECT number, number FROM numbers(10000000)"
    )

    # 3. Create the materialized database. 
    # Without the fix, this would fail with "invalid snapshot identifier"
    # because PostgreSQL would kill the idle exported-snapshot session before
    # the 1M rows finish copying.
    pg_manager.create_materialized_db(
        ip=started_cluster.postgres_ip,
        port=started_cluster.postgres_port,
        settings=[
            "materialized_postgresql_tables_list = 'test_timeout_table'"
        ],
    )

    # 4. If we reach here, the snapshot survived the timeout
    check_tables_are_synchronized(instance, "test_timeout_table")

    # Cleanup: restore default timeout
    pg_manager.execute(
        "ALTER SYSTEM RESET idle_in_transaction_session_timeout;"
    )
    pg_manager.execute("SELECT pg_reload_conf();")
