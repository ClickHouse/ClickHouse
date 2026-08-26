import time

import psycopg2
import pymysql.connections
import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# The authentication method's `VALID UNTIL` must be enforced per query, not only at login.
# Stateful protocols (MySQL, PostgreSQL) authenticate once at connection startup and then run
# every later command through `Session::makeQueryContext`, so without a per-query re-check a
# credential that expires after login would keep working for the lifetime of the connection.
# The check lives in `Session::makeQueryContextImpl`, so every protocol shares it.
node = cluster.add_instance("node", main_configs=["configs/protocols.xml"])

MYSQL_PORT = 9001
POSTGRES_PORT = 5433

# Lifetime of the expiring credential, measured from user creation. The first query on each
# connection runs within a fraction of a second of creation (well inside the lifetime); the second
# query runs after sleeping past the expiry.
EXPIRING_LIFETIME_S = 6


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def create_users():
    node.query("DROP USER IF EXISTS u_expiring, u_lasting")
    expiry = node.query(f"SELECT toString(now() + INTERVAL {EXPIRING_LIFETIME_S} SECOND)").strip()
    # Plaintext-stored passwords so both the MySQL and PostgreSQL frontends can verify them.
    node.query(f"CREATE USER u_expiring IDENTIFIED WITH plaintext_password BY 'pw' VALID UNTIL '{expiry}'")
    node.query("CREATE USER u_lasting IDENTIFIED WITH plaintext_password BY 'pw' VALID UNTIL '2999-01-01 00:00:00'")
    node.query("GRANT SELECT ON system.one TO u_expiring, u_lasting")
    return expiry


def sleep_past(expiry):
    while int(node.query(f"SELECT now() > toDateTime('{expiry}')").strip()) != 1:
        time.sleep(0.5)
    # The check is `now > valid_until` with second precision, so cross the boundary decisively.
    time.sleep(1.5)


def test_mysql_connection_stops_working_after_expiry(started_cluster):
    expiry = create_users()
    host = started_cluster.get_instance_ip("node")

    def connect(user):
        return pymysql.connections.Connection(host=host, user=user, password="pw", database="default", port=MYSQL_PORT)

    expiring = connect("u_expiring")
    lasting = connect("u_lasting")

    # Both credentials are valid at connection time and for the first query.
    for client in (expiring, lasting):
        cursor = client.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == ((1,),)

    sleep_past(expiry)

    # The same, still-open connection must stop working once the method has expired ...
    with pytest.raises(pymysql.err.MySQLError, match="expired"):
        expiring.cursor().execute("SELECT 1")

    # ... while a connection under a non-expired method keeps working.
    cursor = lasting.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchall() == ((1,),)

    lasting.close()


def test_postgresql_connection_stops_working_after_expiry(started_cluster):
    expiry = create_users()
    host = started_cluster.get_instance_ip("node")

    def connect(user):
        conn = psycopg2.connect(host=host, user=user, password="pw", dbname="default", port=POSTGRES_PORT)
        conn.autocommit = True
        return conn

    expiring = connect("u_expiring")
    lasting = connect("u_lasting")

    for client in (expiring, lasting):
        cursor = client.cursor()
        cursor.execute("SELECT 1")
        assert cursor.fetchall() == [(1,)]

    sleep_past(expiry)

    # The PostgreSQL handler sends an ErrorResponse and then terminates the connection on an expired
    # credential, and psycopg2 may surface either the message or the connection loss - both are
    # fail-close; the point is that the query must not succeed.
    with pytest.raises(psycopg2.Error, match="expired|closed the connection"):
        expiring.cursor().execute("SELECT 1")

    cursor = lasting.cursor()
    cursor.execute("SELECT 1")
    assert cursor.fetchall() == [(1,)]

    lasting.close()
