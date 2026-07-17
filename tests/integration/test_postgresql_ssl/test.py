"""End-to-end tests for TLS/SSL connections to PostgreSQL.

The PostgreSQL container from the shared compose file starts without TLS, so the
fixture enables SSL at runtime (self-signed certificate) and then tightens
`pg_hba.conf` to require SSL for every TCP connection. That way the
`sslmode=disable` negative test provably exercises the SSL negotiation path:
without SSL support the connection could not be established at all.
"""

import base64
import os
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
# Paths on the ClickHouse node where the fixture drops the certificates. They must
# be inside the `user_files` directory: certificate/key paths provided through SQL
# are restricted to it (see `StoragePostgreSQL::validateSSLCertificatePaths`).
USER_FILES_DIR = "/var/lib/clickhouse/user_files"
CA_CERT_NAME = "postgresql-ca.crt"
CA_CERT_PATH = f"{USER_FILES_DIR}/{CA_CERT_NAME}"
WRONG_CA_CERT_PATH = f"{USER_FILES_DIR}/postgresql-wrong-ca.crt"
# Client certificate/key used to exercise the sslcert/sslkey (client-auth) path.
CLIENT_CERT_PATH = f"{USER_FILES_DIR}/postgresql-client.crt"
CLIENT_KEY_PATH = f"{USER_FILES_DIR}/postgresql-client.key"
# A database that only accepts connections presenting a verified client
# certificate (see the `pg_hba.conf` written by `enable_postgres_ssl`).
CERT_DB = "certdb"
CERT_TABLE = "cert_table"


def pg_connect(sslmode="prefer", timeout=2):
    conn_string = f"host={cluster.postgres_ip} port={cluster.postgres_port} user='postgres' password='{pg_pass}' dbname='postgres' sslmode={sslmode} connect_timeout={timeout}"
    conn = psycopg2.connect(conn_string)
    conn.autocommit = True
    return conn


def pg_exec(cmd):
    return cluster.exec_in_container(cluster.postgres_id, ["bash", "-c", cmd])


def node_write_file(path, content):
    # base64 keeps the PEM intact across the shell without newline/quoting issues.
    encoded = base64.b64encode(content.encode()).decode()
    node.exec_in_container(["bash", "-c", f"mkdir -p $(dirname {path}) && echo '{encoded}' | base64 -d > {path}"])


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
    # Build a tiny PKI inside the PostgreSQL container: one CA that signs both the
    # server certificate (so verify-ca/verify-full can validate the server) and a
    # client certificate (so the sslcert/sslkey client-authentication path can be
    # exercised). The server certificate CN/SAN match the host name the ClickHouse
    # node connects to; the client certificate CN is the PostgreSQL user name,
    # which the `cert` authentication method maps to that user.
    pg_exec(
        "set -e\n"
        f"cd {PG_DATA_DIR}\n"
        "openssl req -new -x509 -days 3650 -nodes -out ca.crt -keyout ca.key "
        "-subj '/CN=Test PostgreSQL CA' -addext 'basicConstraints=critical,CA:TRUE'\n"
        f"openssl req -new -nodes -out server.csr -keyout server.key -subj '/CN={PG_HOST}'\n"
        f"printf 'subjectAltName=DNS:{PG_HOST}\\n' > server_ext.cnf\n"
        "openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial "
        "-days 3650 -out server.crt -extfile server_ext.cnf\n"
        "openssl req -new -nodes -out client.csr -keyout client.key -subj '/CN=postgres'\n"
        "openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial "
        "-days 3650 -out client.crt\n"
        # PostgreSQL refuses a group/world-readable key and must own its files.
        "chmod 600 server.key ca.key\n"
        "chown postgres:postgres ca.crt server.crt server.key\n"
    )

    # Turn SSL on (the server cert/key default to server.crt/server.key in PGDATA)
    # and trust our CA for client certificates, then wait for encryption to be up.
    conn = pg_connect()
    cursor = conn.cursor()
    cursor.execute("ALTER SYSTEM SET ssl = 'on'")
    cursor.execute(f"ALTER SYSTEM SET ssl_ca_file = '{PG_DATA_DIR}/ca.crt'")
    cursor.execute("SELECT pg_reload_conf()")
    conn.close()

    def ssl_is_up():
        c = pg_connect(sslmode="require")
        c.cursor().execute("SELECT 1")
        c.close()
        return True

    wait_for(ssl_is_up, 30, "PostgreSQL to accept SSL connections")

    # Require SSL for every TCP connection so a plaintext attempt is refused, and
    # require a verified client certificate for connections to CERT_DB (the `cert`
    # method maps the certificate CN to the database user). `hostssl all all all
    # trust` matches both ordinary and logical-replication connections (the latter
    # are matched by database name / `all`).
    pg_exec("cat > %s/pg_hba.conf <<'EOF'\nlocal all all trust\nhostssl %s all all cert\nhostssl all all all trust\nEOF" % (PG_DATA_DIR, CERT_DB))
    pg_connect(sslmode="require").cursor().execute("SELECT pg_reload_conf()")

    def plaintext_is_refused():
        try:
            pg_connect(sslmode="disable")
            return False
        except psycopg2.OperationalError:
            return True

    wait_for(plaintext_is_refused, 30, "PostgreSQL to reject plaintext connections")

    # A database reachable only with a valid client certificate, seeded over the
    # local socket (which uses `trust`, so it needs no certificate).
    pg_exec(f"psql -v ON_ERROR_STOP=1 -U postgres -c 'DROP DATABASE IF EXISTS {CERT_DB}'")
    pg_exec(f"psql -v ON_ERROR_STOP=1 -U postgres -c 'CREATE DATABASE {CERT_DB}'")
    pg_exec(
        f"psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "
        f'"CREATE TABLE {CERT_TABLE} (key integer PRIMARY KEY, value integer); '
        f'INSERT INTO {CERT_TABLE} SELECT i, i * 10 FROM generate_series(0, 9) AS i"'
    )

    # Export the CA to the ClickHouse node for use as sslrootcert, an unrelated CA
    # to check that verification actually fails, and the client certificate/key.
    node_write_file(CA_CERT_PATH, pg_exec(f"cat {PG_DATA_DIR}/ca.crt"))
    node_write_file(CLIENT_CERT_PATH, pg_exec(f"cat {PG_DATA_DIR}/client.crt"))
    node_write_file(CLIENT_KEY_PATH, pg_exec(f"cat {PG_DATA_DIR}/client.key"))
    # libpq refuses a client key that is group/world-readable and requires it to be
    # owned by the effective user of the connecting process (the server, which the
    # cluster starts under the current uid).
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"chown {os.getuid()}:{os.getgid()} {CLIENT_KEY_PATH} && chmod 600 {CLIENT_KEY_PATH}",
        ]
    )
    pg_exec("openssl req -new -x509 -days 3650 -nodes -out /tmp/wrong-ca.crt -keyout /tmp/wrong-ca.key -subj '/CN=wrong'")
    node_write_file(WRONG_CA_CERT_PATH, pg_exec("cat /tmp/wrong-ca.crt"))


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
    assert node.query("SELECT count() FROM postgresql(pg_ssl, sslmode='require')").strip() == "10"
    assert node.query("SELECT sum(value) FROM postgresql(pg_ssl, sslmode='require')").strip() == str(sum(i * 10 for i in range(10)))


def test_sslmode_disable_is_rejected(started_cluster):
    # The server requires SSL, so a plaintext connection must be refused rather
    # than silently downgraded. This proves ClickHouse honors sslmode=require above.
    error = node.query_and_get_error("SELECT count() FROM postgresql(pg_ssl, sslmode='disable')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_sslmode_verify_ca(started_cluster):
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-ca', sslrootcert='{CA_CERT_PATH}')").strip() == "10"


def test_sslmode_verify_full(started_cluster):
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert='{CA_CERT_PATH}')").strip() == "10"


def test_verify_full_with_wrong_ca_is_rejected(started_cluster):
    # A CA that did not sign the server certificate must fail verification.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert='{WRONG_CA_CERT_PATH}')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_table_engine_over_ssl(started_cluster):
    node.query("DROP TABLE IF EXISTS ch_pg_ssl")
    node.query("CREATE TABLE ch_pg_ssl (key UInt32, value UInt32) ENGINE = PostgreSQL(pg_ssl, sslmode='verify-full', sslrootcert='%s')" % CA_CERT_PATH)
    assert node.query("SELECT count() FROM ch_pg_ssl").strip() == "10"
    node.query("DROP TABLE ch_pg_ssl")


def test_postgresql_database_engine_over_ssl(started_cluster):
    # The `PostgreSQL` database engine has its own registration and
    # named-collection parsing path in `DatabasePostgreSQL.cpp`, separate from the
    # table engine and table function that go through `StoragePostgreSQL`. Prove the
    # TLS parameters are honored there too by creating the database over a
    # verify-full connection and reading through one of its tables.
    #
    # The collection here (`pg_ssl_db`) carries no `table` key: the database engine
    # wraps a whole database and rejects a `table` key, so the single-table `pg_ssl`
    # collection would fail validation before the SSL path is ever reached.
    node.query("DROP DATABASE IF EXISTS pg_db_ssl")
    node.query(f"CREATE DATABASE pg_db_ssl ENGINE = PostgreSQL(pg_ssl_db, sslmode='verify-full', sslrootcert='{CA_CERT_PATH}')")
    assert node.query("SELECT count() FROM pg_db_ssl.test_table").strip() == "10"
    assert node.query("SELECT sum(value) FROM pg_db_ssl.test_table").strip() == str(sum(i * 10 for i in range(10)))
    node.query("DROP DATABASE pg_db_ssl")


def test_postgresql_dictionary_over_ssl(started_cluster):
    # The dictionary source used to accept `sslmode` and then silently ignore it;
    # this checks the whole chain (sslmode + sslrootcert) is now honored.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_ssl")
    node.query(
        f"""
        CREATE DICTIONARY dict_pg_ssl (key UInt32, value UInt32)
        PRIMARY KEY key
        SOURCE(POSTGRESQL(
            host '{PG_HOST}' port 5432
            user 'postgres' password '{pg_pass}'
            db 'postgres' table 'test_table'
            sslmode 'verify-full' sslrootcert '{CA_CERT_PATH}'))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    assert node.query("SELECT count() FROM dict_pg_ssl").strip() == "10"
    assert node.query("SELECT dictGetUInt32(dict_pg_ssl, 'value', toUInt64(5))").strip() == "50"
    node.query("DROP DICTIONARY dict_pg_ssl")


def test_client_certificate_authentication(started_cluster):
    # CERT_DB requires a verified client certificate, so a successful read proves
    # the sslcert/sslkey parameters are forwarded to libpq and accepted.
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl_cert, sslmode='verify-full', sslrootcert='{CA_CERT_PATH}', sslcert='{CLIENT_CERT_PATH}', sslkey='{CLIENT_KEY_PATH}')").strip() == "10"


def test_client_certificate_is_required(started_cluster):
    # The same connection without a client certificate must be rejected, so the
    # positive test above really did authenticate with the certificate.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_cert, sslmode='verify-full', sslrootcert='{CA_CERT_PATH}')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


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
    assert node.query("SELECT sum(value) FROM mpg_ssl.mat_table").strip() == str(sum(i * 10 for i in range(50)))

    # Insert a row after the database exists so the WAL consumer (not just the
    # initial snapshot) has to replicate it over the verify-full connection.
    conn = pg_connect(sslmode="require")
    conn.cursor().execute("INSERT INTO mat_table VALUES (50, 500)")
    conn.close()

    wait_for(
        lambda: node.query("SELECT value FROM mpg_ssl.mat_table WHERE key = 50").strip() == "500",
        120,
        "MaterializedPostgreSQL to replicate a post-create insert over SSL",
    )
    assert node.query("SELECT count() FROM mpg_ssl.mat_table").strip() == "51"
    node.query("DROP DATABASE mpg_ssl")


def test_materialized_postgresql_table_engine_ssl(started_cluster):
    # The standalone MaterializedPostgreSQL table engine goes through its own
    # registration and replication-handler startup path (separate from the database
    # engine), so prove it independently: initial snapshot plus a post-create insert
    # both replicated over a verify-full connection.
    seed_table("tbl_engine_table", 25)
    node.query("DROP TABLE IF EXISTS mpg_tbl_ssl SYNC")
    node.query(
        f"""
        CREATE TABLE mpg_tbl_ssl (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'tbl_engine_table', 'postgres', '{pg_pass}')
        ORDER BY key
        SETTINGS
            materialized_postgresql_ssl_mode = 'verify-full',
            materialized_postgresql_ssl_root_cert = '{CA_CERT_PATH}'
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )

    wait_for(
        lambda: node.query("SELECT count() FROM mpg_tbl_ssl").strip() == "25",
        120,
        "MaterializedPostgreSQL table engine initial sync over SSL",
    )
    assert node.query("SELECT sum(value) FROM mpg_tbl_ssl").strip() == str(sum(i * 10 for i in range(25)))

    conn = pg_connect(sslmode="require")
    conn.cursor().execute("INSERT INTO tbl_engine_table VALUES (25, 250)")
    conn.close()

    wait_for(
        lambda: node.query("SELECT value FROM mpg_tbl_ssl WHERE key = 25").strip() == "250",
        120,
        "MaterializedPostgreSQL table engine to replicate a post-create insert over SSL",
    )
    node.query("DROP TABLE mpg_tbl_ssl SYNC")


def test_relative_certificate_path_is_resolved_against_user_files(started_cluster):
    # A relative sslrootcert is resolved against the `user_files` directory (libpq
    # itself would resolve it against the server's working directory).
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert='{CA_CERT_NAME}')").strip() == "10"


def test_certificate_path_outside_user_files_is_rejected(started_cluster):
    # Certificate/key paths provided through SQL must stay inside `user_files`,
    # otherwise any user able to define a PostgreSQL source could make the server
    # open arbitrary local files with its own privileges.
    outside_path = "/etc/clickhouse-server/config.xml"

    for option in ("sslrootcert", "sslcert", "sslkey"):
        error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', {option}='{outside_path}')")
        assert "PATH_ACCESS_DENIED" in error

    # The same restriction covers the MaterializedPostgreSQL settings surface...
    node.query("DROP DATABASE IF EXISTS mpg_bad_path")
    error = node.query_and_get_error(
        f"""
        CREATE DATABASE mpg_bad_path
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}')
        SETTINGS
            materialized_postgresql_ssl_mode = 'verify-full',
            materialized_postgresql_ssl_root_cert = '{outside_path}',
            materialized_postgresql_tables_list = 'mat_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    assert "PATH_ACCESS_DENIED" in error

    # ... and dictionaries created through DDL. Depending on `dictionaries_lazy_load`
    # the source is instantiated either at CREATE or on first use, so accept the
    # error from either step.
    node.query("DROP DICTIONARY IF EXISTS dict_bad_path")
    try:
        node.query(
            f"""
            CREATE DICTIONARY dict_bad_path (key UInt32, value UInt32)
            PRIMARY KEY key
            SOURCE(POSTGRESQL(
                host '{PG_HOST}' port 5432
                user 'postgres' password '{pg_pass}'
                db 'postgres' table 'test_table'
                sslmode 'verify-full' sslrootcert '{outside_path}'))
            LAYOUT(HASHED())
            LIFETIME(MIN 0 MAX 0)
            """
        )
    except Exception as e:
        error = str(e)
    else:
        error = node.query_and_get_error("SELECT dictGetUInt32(dict_bad_path, 'value', toUInt64(1))")
        node.query("DROP DICTIONARY dict_bad_path")
    assert "PATH_ACCESS_DENIED" in error
