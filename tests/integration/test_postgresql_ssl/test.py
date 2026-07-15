"""End-to-end tests for TLS/SSL connections to PostgreSQL.

The PostgreSQL container from the shared compose file starts without TLS, so the
fixture enables SSL at runtime (self-signed certificate) and then tightens
`pg_hba.conf` to require SSL for every TCP connection. That way the
`sslmode=disable` negative test provably exercises the SSL negotiation path:
without SSL support the connection could not be established at all.
"""

import base64
import time

import psycopg2
import pytest

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import pg_pass

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/named_collections.xml"],
    with_postgres=True,
    stay_alive=True,
)

# The docker service/alias name the ClickHouse node uses to reach PostgreSQL.
# It must match the certificate CN/SAN for `sslmode=verify-full` to succeed.
PG_HOST = "postgres1"
PG_DATA_DIR = "/postgres/data"
# Paths on the ClickHouse node where the fixture drops the certificates.
CA_CERT_PATH = "/etc/clickhouse-server/postgresql-ca.crt"
WRONG_CA_CERT_PATH = "/etc/clickhouse-server/postgresql-wrong-ca.crt"


def pg_connect(sslmode="prefer", timeout=2):
    conn_string = (
        f"host={cluster.postgres_ip} port={cluster.postgres_port} "
        f"user='postgres' password='{pg_pass}' dbname='postgres' "
        f"sslmode={sslmode} connect_timeout={timeout}"
    )
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    return conn


def pg_exec(cmd):
    return cluster.exec_in_container(cluster.postgres_id, ["bash", "-c", cmd])


def node_write_file(path, content):
    # base64 keeps the PEM intact across the shell without newline/quoting issues.
    encoded = base64.b64encode(content.encode()).decode()
    node.exec_in_container(["bash", "-c", f"echo '{encoded}' | base64 -d > {path}"])


def wait_for(predicate, timeout, description):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            if predicate():
                return
            last_error = None
        except Exception as e:  # noqa: BLE001
            last_error = e
        time.sleep(0.5)
    raise Exception(f"Timed out waiting for {description}: {last_error}")


def enable_postgres_ssl():
    # Generate a self-signed server certificate. It is used both as the server
    # certificate and as the CA for verify-ca/verify-full. CN and SAN match the
    # host name the ClickHouse node connects to.
    pg_exec(
        f"openssl req -new -x509 -days 3650 -nodes "
        f"-out {PG_DATA_DIR}/server.crt -keyout {PG_DATA_DIR}/server.key "
        f"-subj '/CN={PG_HOST}' -addext 'subjectAltName=DNS:{PG_HOST}'"
    )
    pg_exec(f"chmod 600 {PG_DATA_DIR}/server.key")
    pg_exec(f"chown postgres:postgres {PG_DATA_DIR}/server.key {PG_DATA_DIR}/server.crt")

    # Turn SSL on and wait until an encrypted connection is accepted.
    conn = pg_connect()
    conn.cursor().execute("ALTER SYSTEM SET ssl = 'on'")
    conn.cursor().execute("SELECT pg_reload_conf()")
    conn.close()

    def ssl_is_up():
        c = pg_connect(sslmode="require")
        c.cursor().execute("SELECT 1")
        c.close()
        return True

    wait_for(ssl_is_up, 30, "PostgreSQL to accept SSL connections")

    # Now require SSL for every TCP connection so a plaintext attempt is refused.
    # `hostssl all all all trust` matches both ordinary and logical-replication
    # connections (the latter are matched by database name / `all`).
    pg_exec(
        "cat > %s/pg_hba.conf <<'EOF'\n"
        "local all all trust\n"
        "hostssl all all all trust\n"
        "EOF" % PG_DATA_DIR
    )
    pg_connect(sslmode="require").cursor().execute("SELECT pg_reload_conf()")

    def plaintext_is_refused():
        try:
            pg_connect(sslmode="disable")
            return False
        except psycopg2.OperationalError:
            return True

    wait_for(plaintext_is_refused, 30, "PostgreSQL to reject plaintext connections")

    # Export the server certificate to the ClickHouse node so it can be used as
    # sslrootcert, plus an unrelated CA to check that verification actually fails.
    ca_pem = pg_exec(f"cat {PG_DATA_DIR}/server.crt")
    node_write_file(CA_CERT_PATH, ca_pem)
    pg_exec(
        "openssl req -new -x509 -days 3650 -nodes "
        "-out /tmp/wrong-ca.crt -keyout /tmp/wrong-ca.key -subj '/CN=wrong'"
    )
    wrong_ca_pem = pg_exec("cat /tmp/wrong-ca.crt")
    node_write_file(WRONG_CA_CERT_PATH, wrong_ca_pem)


def seed_table(name, count):
    conn = pg_connect(sslmode="require")
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {name}")
    cursor.execute(f"CREATE TABLE {name} (key integer PRIMARY KEY, value integer)")
    cursor.execute(f"INSERT INTO {name} SELECT i, i * 10 FROM generate_series(0, {count - 1}) AS i")
    conn.close()


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        enable_postgres_ssl()
        seed_table("test_table", 10)
        yield cluster
    finally:
        cluster.shutdown()


def test_sslmode_require(started_cluster):
    assert (
        node.query("SELECT count() FROM postgresql(pg_ssl, sslmode='require')").strip()
        == "10"
    )
    assert (
        node.query("SELECT sum(value) FROM postgresql(pg_ssl, sslmode='require')").strip()
        == str(sum(i * 10 for i in range(10)))
    )


def test_sslmode_disable_is_rejected(started_cluster):
    # The server requires SSL, so a plaintext connection must be refused rather
    # than silently downgraded. This proves ClickHouse honors sslmode=require above.
    error = node.query_and_get_error(
        "SELECT count() FROM postgresql(pg_ssl, sslmode='disable')"
    )
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_sslmode_verify_ca(started_cluster):
    assert (
        node.query(
            f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-ca', sslrootcert='{CA_CERT_PATH}')"
        ).strip()
        == "10"
    )


def test_sslmode_verify_full(started_cluster):
    assert (
        node.query(
            f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert='{CA_CERT_PATH}')"
        ).strip()
        == "10"
    )


def test_verify_full_with_wrong_ca_is_rejected(started_cluster):
    # A CA that did not sign the server certificate must fail verification.
    error = node.query_and_get_error(
        f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert='{WRONG_CA_CERT_PATH}')"
    )
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_table_engine_over_ssl(started_cluster):
    node.query("DROP TABLE IF EXISTS ch_pg_ssl")
    node.query(
        "CREATE TABLE ch_pg_ssl (key UInt32, value UInt32) "
        "ENGINE = PostgreSQL(pg_ssl, sslmode='verify-full', sslrootcert='%s')" % CA_CERT_PATH
    )
    assert node.query("SELECT count() FROM ch_pg_ssl").strip() == "10"
    node.query("DROP TABLE ch_pg_ssl")


def test_materialized_postgresql_database_ssl(started_cluster):
    seed_table("mat_table", 50)
    node.query("DROP DATABASE IF EXISTS mpg_ssl")
    node.query(
        f"""
        CREATE DATABASE mpg_ssl
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')
        SETTINGS
            materialized_postgresql_ssl_mode = 'verify-full',
            materialized_postgresql_ssl_root_cert = '{CA_CERT_PATH}',
            materialized_postgresql_tables_list = 'mat_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )

    wait_for(
        lambda: node.query("SELECT count() FROM mpg_ssl.mat_table").strip() == "50",
        120,
        "MaterializedPostgreSQL initial sync over SSL",
    )
    assert node.query("SELECT sum(value) FROM mpg_ssl.mat_table").strip() == str(
        sum(i * 10 for i in range(50))
    )
    node.query("DROP DATABASE mpg_ssl")
