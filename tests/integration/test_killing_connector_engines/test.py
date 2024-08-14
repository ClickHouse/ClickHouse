import logging
import time

import pytest
import psycopg2

from helpers.cluster import ClickHouseCluster
from helpers.test_tools import assert_eq_with_retry
from helpers.postgres_utility import get_postgres_conn
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


def get_postgres_cursor(started_cluster, database=None):
    # connect to database as well
    conn = get_postgres_conn(
        started_cluster.postgres_ip, started_cluster.postgres_port, database=database
    )
    return conn.cursor()


def test_postgres_database_engine_restart():
    cluster = ClickHouseCluster(__file__)
    try:
        node1 = cluster.add_instance(
            "node1",
            with_postgres_ssl=True,
        )
        cluster.start()

        cursor = get_postgres_cursor(cluster)
        cursor.execute("CREATE DATABASE postgres_database")
        cursor = get_postgres_cursor(cluster, database=True)

        cursor.execute("DROP TABLE IF EXISTS test_table")
        cursor.execute("CREATE TABLE test_table (id Integer NOT NULL, value Integer, PRIMARY KEY (id))")
        node1.query(
            "CREATE DATABASE postgres_database ENGINE = PostgreSQL('postgres1:5432', 'postgres_database', 'postgres', 'mysecretpassword')"
        )
        assert "postgres_database" in node1.query("SHOW DATABASES")
        assert "test_table" in node1.query("SHOW TABLES FROM postgres_database")
        assert node1.query("INSERT INTO  postgres_database.test_table VALUES (1, 1)") == ""
        assert node1.query("SELECT id FROM postgres_database.test_table") == '1\n'

        # kill postgres
        cluster.restart_postgress()

        timeout = 300
        timeout_time = time.time() + timeout
        connection_restored = False
        while time.time() < timeout_time and not connection_restored:
            time.sleep(5)
            try:
                connection_restored = node1.query("SELECT id FROM postgres_database.test_table") == '1\n'
            except Exception as e:
                logging.debug(f"connection failed: {e}")

        assert node1.query("SELECT id FROM postgres_database.test_table") == '1\n'

        cursor = get_postgres_cursor(cluster, database=True)
        cursor.execute("DROP TABLE test_table")
        # assert False
    finally:
        cluster.shutdown()
