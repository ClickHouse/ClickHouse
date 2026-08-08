"""End-to-end tests for TLS/SSL connections to PostgreSQL.

The PostgreSQL container from the shared compose file starts without TLS, so the
fixture enables SSL at runtime (self-signed certificate) and then tightens
`pg_hba.conf` to require SSL for every TCP connection. That way the
`sslmode=disable` negative test provably exercises the SSL negotiation path:
without SSL support the connection could not be established at all.

Certificates and keys are passed to ClickHouse as literal PEM contents
(`sslrootcert_pem` / `sslcert_pem` / `sslkey_pem`), which is the only form
accepted from SQL; paths (`sslrootcert` / `sslcert` / `sslkey`) are only
accepted from a named collection defined in the server configuration file
(`configs/named_collections.xml` here, with the files written by the fixture).
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
    user_configs=["configs/users.xml"],
    with_postgres=True,
    stay_alive=True,
)

# The docker service/alias name the ClickHouse node uses to reach PostgreSQL.
# It must match the certificate CN/SAN for `sslmode=verify-full` to succeed.
PG_HOST = "postgres1"
PG_DATA_DIR = "/postgres/data"
# Paths on the ClickHouse node where the fixture drops the certificate files that
# the configuration-defined named collections (`pg_ssl_paths` etc.) point to.
NODE_CERT_DIR = "/var/lib/clickhouse/pg_certs"
CA_CERT_PATH = f"{NODE_CERT_DIR}/postgresql-ca.crt"
CLIENT_CERT_PATH = f"{NODE_CERT_DIR}/postgresql-client.crt"
CLIENT_KEY_PATH = f"{NODE_CERT_DIR}/postgresql-client.key"
# A database that only accepts connections presenting a verified client
# certificate (see the `pg_hba.conf` written by `enable_postgres_ssl`).
CERT_DB = "certdb"
CERT_TABLE = "cert_table"

# PEM contents exported from the PostgreSQL container by the fixture.
ca_pem = None
wrong_ca_pem = None
client_cert_pem = None
client_key_pem = None


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


def quote_pem(pem):
    """Escape PEM contents for use inside a single-quoted SQL string literal."""
    return pem.replace("\\", "\\\\").replace("'", "\\'").replace("\n", "\\n")


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
    global ca_pem, wrong_ca_pem, client_cert_pem, client_key_pem

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

    # Export the credentials: as PEM contents for the `*_pem` arguments, and as
    # files on the ClickHouse node for the configuration-defined named collections
    # that carry paths (`pg_ssl_paths`, `pg_ssl_cert_paths`, `pg_ssl_locked_paths`).
    # An unrelated CA is generated to check that verification actually fails.
    ca_pem = pg_exec(f"cat {PG_DATA_DIR}/ca.crt")
    client_cert_pem = pg_exec(f"cat {PG_DATA_DIR}/client.crt")
    client_key_pem = pg_exec(f"cat {PG_DATA_DIR}/client.key")
    pg_exec("openssl req -new -x509 -days 3650 -nodes -out /tmp/wrong-ca.crt -keyout /tmp/wrong-ca.key -subj '/CN=wrong'")
    wrong_ca_pem = pg_exec("cat /tmp/wrong-ca.crt")

    node_write_file(CA_CERT_PATH, ca_pem)
    node_write_file(CLIENT_CERT_PATH, client_cert_pem)
    node_write_file(CLIENT_KEY_PATH, client_key_pem)
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
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-ca', sslrootcert_pem='{quote_pem(ca_pem)}')").strip() == "10"


def test_sslmode_verify_full(started_cluster):
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')").strip() == "10"


def test_verify_full_with_wrong_ca_is_rejected(started_cluster):
    # A CA that did not sign the server certificate must fail verification.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl, sslmode='verify-full', sslrootcert_pem='{quote_pem(wrong_ca_pem)}')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_positional_arguments_with_tls_key_value_arguments(started_cluster):
    # Without a named collection the TLS parameters follow the positional arguments
    # as `key = value` pairs, which take a separate parsing path
    # (`StoragePostgreSQL::extractSSLParamsFromArguments`).
    assert (
        node.query(
            f"""SELECT count() FROM postgresql('{PG_HOST}:5432', 'postgres', 'test_table', 'postgres', '{pg_pass}',
                                           sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')"""
        ).strip()
        == "10"
    )

    # The same with a CA that did not sign the server certificate must fail, so the
    # positive case above cannot pass with the arguments silently dropped (libpq
    # would fall back to `sslmode=prefer` and still connect).
    error = node.query_and_get_error(
        f"""SELECT count() FROM postgresql('{PG_HOST}:5432', 'postgres', 'test_table', 'postgres', '{pg_pass}',
                                           sslmode='verify-full', sslrootcert_pem='{quote_pem(wrong_ca_pem)}')"""
    )
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_table_engine_over_ssl(started_cluster):
    node.query("DROP TABLE IF EXISTS ch_pg_ssl")
    node.query(f"CREATE TABLE ch_pg_ssl (key UInt32, value UInt32) ENGINE = PostgreSQL(pg_ssl, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')")
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
    node.query(f"CREATE DATABASE pg_db_ssl ENGINE = PostgreSQL(pg_ssl_db, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')")
    assert node.query("SELECT count() FROM pg_db_ssl.test_table").strip() == "10"
    assert node.query("SELECT sum(value) FROM pg_db_ssl.test_table").strip() == str(sum(i * 10 for i in range(10)))
    node.query("DROP DATABASE pg_db_ssl")


def test_postgresql_dictionary_over_ssl(started_cluster):
    # The dictionary source used to accept `sslmode` and then silently ignore it;
    # this checks the whole chain (sslmode + sslrootcert_pem) is honored.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_ssl")
    node.query(
        f"""
        CREATE DICTIONARY dict_pg_ssl (key UInt32, value UInt32)
        PRIMARY KEY key
        SOURCE(POSTGRESQL(
            host '{PG_HOST}' port 5432
            user 'postgres' password '{pg_pass}'
            db 'postgres' table 'test_table'
            sslmode 'verify-full' sslrootcert_pem '{quote_pem(ca_pem)}'))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    assert node.query("SELECT count() FROM dict_pg_ssl").strip() == "10"
    assert node.query("SELECT dictGetUInt32(dict_pg_ssl, 'value', toUInt64(5))").strip() == "50"
    node.query("DROP DICTIONARY dict_pg_ssl")


def test_postgresql_dictionary_client_certificate(started_cluster):
    # `PostgreSQLDictionarySource` builds its configuration on its own path instead
    # of reusing `StoragePostgreSQL::getConfiguration`, so the table-function
    # coverage does not prove the client-certificate keys on the dictionary surface.
    # Source a dictionary from `certdb`, which only accepts a connection presenting
    # a verified client certificate: dropped `sslcert_pem`/`sslkey_pem` would make
    # the connection fail before any row is read, so a successful load proves the
    # keys reach `libpq`. A dedicated table keeps the test independent of the other
    # `certdb` cases.
    pg_exec(
        f"psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "
        f'"CREATE TABLE IF NOT EXISTS dict_cert_table (key integer PRIMARY KEY, value integer); '
        f"TRUNCATE dict_cert_table; "
        f'INSERT INTO dict_cert_table SELECT i, i * 10 FROM generate_series(0, 9) AS i"'
    )
    node.query("DROP DICTIONARY IF EXISTS dict_pg_cert")
    node.query(
        f"""
        CREATE DICTIONARY dict_pg_cert (key UInt32, value UInt32)
        PRIMARY KEY key
        SOURCE(POSTGRESQL(
            host '{PG_HOST}' port 5432
            user 'postgres' password '{pg_pass}'
            db '{CERT_DB}' table 'dict_cert_table'
            sslmode 'verify-full' sslrootcert_pem '{quote_pem(ca_pem)}'
            sslcert_pem '{quote_pem(client_cert_pem)}' sslkey_pem '{quote_pem(client_key_pem)}'))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    assert node.query("SELECT count() FROM dict_pg_cert").strip() == "10"
    assert node.query("SELECT dictGetUInt32(dict_pg_cert, 'value', toUInt64(5))").strip() == "50"
    node.query("DROP DICTIONARY dict_pg_cert")


def test_postgresql_dictionary_client_certificate_is_required(started_cluster):
    # The same dictionary without a client certificate must fail, so the positive
    # test above really did authenticate with the certificate rather than connecting
    # anonymously. Depending on `dictionaries_lazy_load` the source is instantiated
    # either at CREATE or on first use, so accept the error from either step.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_cert_missing")
    try:
        node.query(
            f"""
            CREATE DICTIONARY dict_pg_cert_missing (key UInt32, value UInt32)
            PRIMARY KEY key
            SOURCE(POSTGRESQL(
                host '{PG_HOST}' port 5432
                user 'postgres' password '{pg_pass}'
                db '{CERT_DB}' table 'dict_cert_table'
                sslmode 'verify-full' sslrootcert_pem '{quote_pem(ca_pem)}'))
            LAYOUT(HASHED())
            LIFETIME(MIN 0 MAX 0)
            """
        )
    except Exception as e:
        error = str(e)
    else:
        error = node.query_and_get_error("SELECT dictGetUInt32(dict_pg_cert_missing, 'value', toUInt64(1))")
        node.query("DROP DICTIONARY dict_pg_cert_missing")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_dictionary_wrong_ca_is_rejected(started_cluster):
    # The positive dictionary cases connect to a server that accepts any SSL session
    # (`hostssl ... trust`), so they would still pass if `PostgreSQLDictionarySource`
    # dropped `sslmode`/`sslrootcert_pem` and libpq fell back to its default
    # `sslmode=prefer`. Pointing the same source at a CA that did not sign the server
    # certificate must fail: certificate verification is only performed when both
    # settings reach libpq, so dropped parameters would connect happily and this test
    # would not see an error. Depending on `dictionaries_lazy_load` the source is
    # instantiated either at CREATE or on first use, so accept the error from either
    # step.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_wrong_ca")
    try:
        node.query(
            f"""
            CREATE DICTIONARY dict_pg_wrong_ca (key UInt32, value UInt32)
            PRIMARY KEY key
            SOURCE(POSTGRESQL(
                host '{PG_HOST}' port 5432
                user 'postgres' password '{pg_pass}'
                db 'postgres' table 'test_table'
                sslmode 'verify-full' sslrootcert_pem '{quote_pem(wrong_ca_pem)}'))
            LAYOUT(HASHED())
            LIFETIME(MIN 0 MAX 0)
            """
        )
    except Exception as e:
        error = str(e)
    else:
        error = node.query_and_get_error("SELECT dictGetUInt32(dict_pg_wrong_ca, 'value', toUInt64(1))")
        node.query("DROP DICTIONARY dict_pg_wrong_ca")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_dictionary_named_collection_over_ssl(started_cluster):
    # `SOURCE(POSTGRESQL(NAME ...))` takes a different branch in
    # `PostgreSQLDictionarySource`: it reads the TLS parameters out of the named
    # collection (with the query keys applied as overrides) instead of the DDL keys,
    # so the inline cases above cannot falsify a regression there.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_nc_ssl")
    node.query(
        f"""
        CREATE DICTIONARY dict_pg_nc_ssl (key UInt32, value UInt32)
        PRIMARY KEY key
        SOURCE(POSTGRESQL(NAME pg_ssl sslmode 'verify-full' sslrootcert_pem '{quote_pem(ca_pem)}'))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    assert node.query("SELECT count() FROM dict_pg_nc_ssl").strip() == "10"
    assert node.query("SELECT dictGetUInt32(dict_pg_nc_ssl, 'value', toUInt64(5))").strip() == "50"
    node.query("DROP DICTIONARY dict_pg_nc_ssl")


def test_postgresql_dictionary_named_collection_wrong_ca_is_rejected(started_cluster):
    # The named-collection positive case connects to a server that accepts any SSL
    # session, so it would still pass if the named-collection branch dropped
    # `sslmode`/`sslrootcert_pem` and libpq fell back to `sslmode=prefer`. A CA that
    # did not sign the server certificate must fail. Depending on
    # `dictionaries_lazy_load` the source is instantiated either at CREATE or on
    # first use, so accept either.
    node.query("DROP DICTIONARY IF EXISTS dict_pg_nc_wrong_ca")
    try:
        node.query(
            f"""
            CREATE DICTIONARY dict_pg_nc_wrong_ca (key UInt32, value UInt32)
            PRIMARY KEY key
            SOURCE(POSTGRESQL(NAME pg_ssl sslmode 'verify-full' sslrootcert_pem '{quote_pem(wrong_ca_pem)}'))
            LAYOUT(HASHED())
            LIFETIME(MIN 0 MAX 0)
            """
        )
    except Exception as e:
        error = str(e)
    else:
        error = node.query_and_get_error("SELECT dictGetUInt32(dict_pg_nc_wrong_ca, 'value', toUInt64(1))")
        node.query("DROP DICTIONARY dict_pg_nc_wrong_ca")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_postgresql_dictionary_named_collection_client_certificate(started_cluster):
    # The client-certificate half of the same branch: `certdb` only accepts a connection
    # presenting a verified client certificate, so dropped `sslcert_pem`/`sslkey_pem`
    # would fail before any row is read. The `table` key of the collection is overridden
    # to the dedicated table used by the inline dictionary case.
    pg_exec(
        f"psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "
        f'"CREATE TABLE IF NOT EXISTS dict_cert_table (key integer PRIMARY KEY, value integer); '
        f"TRUNCATE dict_cert_table; "
        f'INSERT INTO dict_cert_table SELECT i, i * 10 FROM generate_series(0, 9) AS i"'
    )
    node.query("DROP DICTIONARY IF EXISTS dict_pg_nc_cert")
    node.query(
        f"""
        CREATE DICTIONARY dict_pg_nc_cert (key UInt32, value UInt32)
        PRIMARY KEY key
        SOURCE(POSTGRESQL(
            NAME pg_ssl_cert table 'dict_cert_table'
            sslmode 'verify-full' sslrootcert_pem '{quote_pem(ca_pem)}'
            sslcert_pem '{quote_pem(client_cert_pem)}' sslkey_pem '{quote_pem(client_key_pem)}'))
        LAYOUT(HASHED())
        LIFETIME(MIN 0 MAX 0)
        """
    )
    assert node.query("SELECT count() FROM dict_pg_nc_cert").strip() == "10"
    assert node.query("SELECT dictGetUInt32(dict_pg_nc_cert, 'value', toUInt64(5))").strip() == "50"
    node.query("DROP DICTIONARY dict_pg_nc_cert")


def test_postgresql_database_engine_wrong_ca_is_rejected(started_cluster):
    # Same argument for the `PostgreSQL` database engine, which parses the SSL
    # parameters on its own path in `DatabasePostgreSQL.cpp`: a wrong CA must break the
    # connection, which would not happen if the parameters were dropped there. The
    # database itself is created without connecting, so the failure surfaces on the
    # first query that reaches PostgreSQL.
    node.query("DROP DATABASE IF EXISTS pg_db_wrong_ca")
    node.query(f"CREATE DATABASE pg_db_wrong_ca ENGINE = PostgreSQL(pg_ssl_db, sslmode='verify-full', sslrootcert_pem='{quote_pem(wrong_ca_pem)}')")
    error = node.query_and_get_error("SELECT count() FROM pg_db_wrong_ca.test_table")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error or "UNKNOWN_TABLE" in error
    node.query("DROP DATABASE pg_db_wrong_ca")


def test_client_certificate_authentication(started_cluster):
    # CERT_DB requires a verified client certificate, so a successful read proves
    # the sslcert_pem/sslkey_pem parameters are forwarded to libpq and accepted.
    assert (
        node.query(
            f"SELECT count() FROM postgresql(pg_ssl_cert, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}', "
            f"sslcert_pem='{quote_pem(client_cert_pem)}', sslkey_pem='{quote_pem(client_key_pem)}')"
        ).strip()
        == "10"
    )


def test_client_certificate_is_required(started_cluster):
    # The same connection without a client certificate must be rejected, so the
    # positive test above really did authenticate with the certificate.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_cert, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error


def test_materialized_postgresql_database_ssl(started_cluster):
    seed_table("mat_table", 50)
    node.query("DROP DATABASE IF EXISTS mpg_ssl")
    node.query(
        f"""
        CREATE DATABASE mpg_ssl
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}')
        SETTINGS materialized_postgresql_tables_list = 'mat_table'
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

    # The credential contents must not appear in the stored definition as shown.
    assert quote_pem(ca_pem) not in node.query("SHOW CREATE DATABASE mpg_ssl")

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
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'tbl_engine_table', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}')
        ORDER BY key
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


def test_materialized_postgresql_table_engine_client_certificate(started_cluster):
    # The `verify-full` case above would still pass if `sslcert_pem`/`sslkey_pem`
    # were dropped (the server enforces SSL, but not a client certificate).
    # Materialize a `certdb` table, which only accepts a verified client
    # certificate, so both the snapshot and the WAL-consumer connections must
    # authenticate with it: dropped credentials would make the initial sync never
    # complete. A dedicated table keeps the test independent of the other `certdb`
    # cases.
    pg_exec(
        f"psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "
        f'"CREATE TABLE IF NOT EXISTS tbl_cert_table (key integer PRIMARY KEY, value integer); '
        f"TRUNCATE tbl_cert_table; "
        f'INSERT INTO tbl_cert_table SELECT i, i * 10 FROM generate_series(0, 24) AS i"'
    )
    node.query("DROP TABLE IF EXISTS mpg_tbl_cert_ssl SYNC")
    node.query(
        f"""
        CREATE TABLE mpg_tbl_cert_ssl (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', '{CERT_DB}', 'tbl_cert_table', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}',
                                        sslcert_pem = '{quote_pem(client_cert_pem)}', sslkey_pem = '{quote_pem(client_key_pem)}')
        ORDER BY key
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )

    wait_for(
        lambda: node.query("SELECT count() FROM mpg_tbl_cert_ssl").strip() == "25",
        120,
        "MaterializedPostgreSQL table engine initial sync over a client-certificate connection",
    )
    assert node.query("SELECT sum(value) FROM mpg_tbl_cert_ssl").strip() == str(sum(i * 10 for i in range(25)))

    # A post-create insert must replicate too, so the WAL consumer (a separate
    # connection) also authenticates with the client certificate. Insert over the
    # local socket (trust auth): a psycopg2 client here has no certificate to reach
    # `certdb`.
    pg_exec(f'psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "INSERT INTO tbl_cert_table VALUES (25, 250)"')

    wait_for(
        lambda: node.query("SELECT value FROM mpg_tbl_cert_ssl WHERE key = 25").strip() == "250",
        120,
        "MaterializedPostgreSQL table engine to replicate a post-create insert over a client-certificate connection",
    )
    node.query("DROP TABLE mpg_tbl_cert_ssl SYNC")


def test_materialized_postgresql_client_certificate(started_cluster):
    # The MaterializedPostgreSQL database-engine case above replicates over the
    # SSL-forced connection, so it would still pass if the TLS arguments were
    # silently dropped and libpq fell back to its default `sslmode`. To prove the
    # full TLS parameters are actually threaded into the replication connection --
    # including `sslcert_pem`/`sslkey_pem` -- replicate the `certdb` database, which
    # only accepts a connection presenting a verified client certificate. Both the
    # snapshot and the WAL-consumer connections must authenticate with it, so a
    # successful replication proves the credentials reached libpq. Use a dedicated
    # table rather than the shared `cert_table`: the post-create insert below would
    # otherwise change the row count that other tests reading `cert_table` (through
    # the `pg_ssl_cert`/`pg_ssl_cert_paths` named collections) rely on. Seed it over
    # the local socket (trust auth) so the test stays re-runnable.
    pg_exec(
        f"psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "
        f'"CREATE TABLE IF NOT EXISTS mpg_cert_table (key integer PRIMARY KEY, value integer); '
        f"TRUNCATE mpg_cert_table; "
        f'INSERT INTO mpg_cert_table SELECT i, i * 10 FROM generate_series(0, 9) AS i"'
    )
    node.query("DROP DATABASE IF EXISTS mpg_cert_ssl")
    node.query(
        f"""
        CREATE DATABASE mpg_cert_ssl
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', '{CERT_DB}', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}',
                                        sslcert_pem = '{quote_pem(client_cert_pem)}', sslkey_pem = '{quote_pem(client_key_pem)}')
        SETTINGS materialized_postgresql_tables_list = 'mpg_cert_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )

    wait_for(
        lambda: node.query("SELECT count() FROM mpg_cert_ssl.mpg_cert_table").strip() == "10",
        120,
        "MaterializedPostgreSQL initial sync over a client-certificate connection",
    )
    assert node.query("SELECT sum(value) FROM mpg_cert_ssl.mpg_cert_table").strip() == str(sum(i * 10 for i in range(10)))

    # A row inserted after CREATE DATABASE must replicate too, so the WAL consumer
    # (a separate connection) also authenticates with the client certificate. Insert
    # over the local socket (trust auth): a psycopg2 client here has no certificate to
    # reach `certdb`.
    pg_exec(f'psql -v ON_ERROR_STOP=1 -U postgres -d {CERT_DB} -c "INSERT INTO mpg_cert_table VALUES (10, 100)"')

    wait_for(
        lambda: node.query("SELECT value FROM mpg_cert_ssl.mpg_cert_table WHERE key = 10").strip() == "100",
        120,
        "MaterializedPostgreSQL to replicate a post-create insert over a client-certificate connection",
    )
    assert node.query("SELECT count() FROM mpg_cert_ssl.mpg_cert_table").strip() == "11"

    node.query("DROP DATABASE mpg_cert_ssl")


def test_materialized_postgresql_table_engine_wrong_ca_is_rejected(started_cluster):
    # The `MaterializedPostgreSQL` cases above all connect to a server that accepts any
    # SSL session, so none of them can falsify a regression that drops
    # `sslmode`/`sslrootcert_pem` on the table-engine path: libpq would fall back to
    # `sslmode=prefer` and still connect. A CA that did not sign the server certificate
    # must break the connection, which only happens when both parameters reach libpq.
    # On a fresh CREATE (not an attach) the table engine starts replication
    # synchronously, so the error surfaces at CREATE. The replication handler lets the
    # `pqxx` exception through unwrapped, so the error is a generic `STD_EXCEPTION`
    # rather than `POSTGRESQL_CONNECTION_FAILURE`; assert on the verification failure
    # itself, which is what proves the parameters reached libpq.
    seed_table("tbl_wrong_ca_table", 5)
    node.query("DROP TABLE IF EXISTS mpg_tbl_wrong_ca SYNC")
    error = node.query_and_get_error(
        f"""
        CREATE TABLE mpg_tbl_wrong_ca (key Int32, value Int32)
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'tbl_wrong_ca_table', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(wrong_ca_pem)}')
        ORDER BY key
        """,
        settings={"allow_experimental_materialized_postgresql_table": 1},
    )
    assert "certificate verify failed" in error
    node.query("DROP TABLE IF EXISTS mpg_tbl_wrong_ca SYNC")


def test_materialized_postgresql_database_wrong_ca_is_rejected(started_cluster):
    # The same falsification for the database engine, which parses the engine
    # arguments on its own registration path. Unlike the table engine, the database
    # engine starts replication in a background task that retries on failure, so
    # `CREATE DATABASE` itself succeeds and a rejected certificate shows up as
    # replication never creating the replicated table.
    seed_table("db_wrong_ca_table", 20)
    node.query("DROP DATABASE IF EXISTS mpg_wrong_ca")
    node.query(
        f"""
        CREATE DATABASE mpg_wrong_ca
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(wrong_ca_pem)}')
        SETTINGS materialized_postgresql_tables_list = 'db_wrong_ca_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )

    # The startup task retries every 5 seconds, so a connection that libpq accepted
    # would have produced the table well within this window.
    deadline = time.time() + 20
    while time.time() < deadline:
        node.query_and_get_error("SELECT count() FROM mpg_wrong_ca.db_wrong_ca_table")
        time.sleep(1)
    node.query("DROP DATABASE mpg_wrong_ca")

    # Positive control: the very same database with the correct CA does synchronize, so
    # the absence above is caused by the certificate being rejected and not by an
    # unrelated failure of the replication setup.
    node.query("DROP DATABASE IF EXISTS mpg_right_ca")
    node.query(
        f"""
        CREATE DATABASE mpg_right_ca
        ENGINE = MaterializedPostgreSQL('{PG_HOST}:5432', 'postgres', 'postgres', '{pg_pass}',
                                        sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}')
        SETTINGS materialized_postgresql_tables_list = 'db_wrong_ca_table'
        """,
        settings={"allow_experimental_database_materialized_postgresql": 1},
    )
    wait_for(
        lambda: node.query("SELECT count() FROM mpg_right_ca.db_wrong_ca_table").strip() == "20",
        120,
        "MaterializedPostgreSQL initial sync with the correct CA",
    )
    node.query("DROP DATABASE mpg_right_ca")


def test_certificate_paths_from_config_named_collection(started_cluster):
    # Certificate and key paths are accepted from a named collection defined in the
    # server configuration file; `pg_ssl_paths` carries `sslmode=verify-full` and an
    # `sslrootcert` path, so a plain read proves the configured path reaches libpq.
    assert node.query("SELECT count() FROM postgresql(pg_ssl_paths)").strip() == "10"

    # The client-certificate variant: `certdb` only accepts a verified client
    # certificate, so a successful read proves the configured `sslcert`/`sslkey`
    # paths are used.
    assert node.query("SELECT count() FROM postgresql(pg_ssl_cert_paths)").strip() == "10"


def test_contents_override_configured_path(started_cluster):
    # A query can replace a configured path with credential contents -- that is the
    # only SQL-safe way to override the credential. Overriding the correct CA with a
    # wrong one must break verification, which proves the contents actually replace
    # the configured path rather than being ignored.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_paths, sslrootcert_pem='{quote_pem(wrong_ca_pem)}')")
    assert "POSTGRESQL_CONNECTION_FAILURE" in error

    # The same override with the correct CA keeps working.
    assert node.query(f"SELECT count() FROM postgresql(pg_ssl_paths, sslrootcert_pem='{quote_pem(ca_pem)}')").strip() == "10"

    # A credential locked by the operator (`<sslrootcert overridable="false">`)
    # cannot be replaced, not even through the contents form.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_locked_paths, sslrootcert_pem='{quote_pem(ca_pem)}')")
    assert "Override not allowed for 'sslrootcert'" in error


def test_path_overrides_are_rejected(started_cluster):
    # A path cannot be overridden in a query, not even with an empty value: an empty
    # override would silently drop the credential the operator configured (e.g.
    # disable the verification of the server certificate).
    for value in ("/etc/clickhouse-server/config.xml", ""):
        error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_paths, sslrootcert='{value}')")
        assert "cannot be overridden in a query" in error

    # Empty contents would drop the configured credential the same way.
    error = node.query_and_get_error("SELECT count() FROM postgresql(pg_ssl_paths, sslrootcert_pem='')")
    assert "cannot be overridden with an empty" in error


def test_tls_credentials_are_masked(started_cluster):
    # The credential contents are secrets: they must not show up in the stored table
    # definition nor in `system.query_log`. The key is the most sensitive of the
    # three, so use a table that carries all of them.
    node.query("DROP TABLE IF EXISTS ch_pg_masked")
    node.query(
        f"CREATE TABLE ch_pg_masked (key UInt32, value UInt32) ENGINE = PostgreSQL(pg_ssl_cert, "
        f"sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}', "
        f"sslcert_pem='{quote_pem(client_cert_pem)}', sslkey_pem='{quote_pem(client_key_pem)}')"
    )
    assert node.query("SELECT count() FROM ch_pg_masked").strip() == "10"

    show_create = node.query("SHOW CREATE TABLE ch_pg_masked")
    assert "[HIDDEN]" in show_create
    assert "BEGIN CERTIFICATE" not in show_create
    assert "PRIVATE KEY" not in show_create

    # The split literal keeps this query itself from matching the pattern.
    node.query("SYSTEM FLUSH LOGS query_log")
    assert node.query("SELECT count() FROM system.query_log WHERE query LIKE '%BEGIN PRIVATE%' || 'KEY%'").strip() == "0"

    node.query("DROP TABLE ch_pg_masked")


def test_tls_credentials_survive_restart(started_cluster):
    # Credential contents given in a persisted definition are re-materialized into
    # fresh temporary files when the definition is loaded again, so a table and a
    # database created with them must keep working across a server restart.
    node.query("DROP TABLE IF EXISTS pg_restart_tbl SYNC")
    node.query(f"CREATE TABLE pg_restart_tbl (key UInt32, value UInt32) ENGINE = PostgreSQL(pg_ssl, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')")
    assert node.query("SELECT count() FROM pg_restart_tbl").strip() == "10"

    node.query("DROP DATABASE IF EXISTS pg_restart_db")
    node.query(f"CREATE DATABASE pg_restart_db ENGINE = PostgreSQL(pg_ssl_db, sslmode='verify-full', sslrootcert_pem='{quote_pem(ca_pem)}')")
    assert node.query("SELECT count() FROM pg_restart_db.test_table").strip() == "10"

    node.restart_clickhouse()

    assert node.query("SELECT count() FROM pg_restart_tbl").strip() == "10"
    assert node.query("SELECT count() FROM pg_restart_db.test_table").strip() == "10"

    node.query("DROP TABLE pg_restart_tbl SYNC")
    node.query("DROP DATABASE pg_restart_db")


def test_tls_credentials_in_sql_named_collection(started_cluster):
    # Credential contents are accepted in SQL-created named collections too. That is a
    # separate persistence path from direct query arguments: the multiline PEM has to
    # survive the collection's own serialization to disk and the deserialization on
    # use, and still be materialized into the temporary file for libpq. The collection
    # points at `certdb`, which only accepts a verified client certificate, so a
    # successful read proves all three credentials made it through intact.
    node.query("DROP NAMED COLLECTION IF EXISTS pg_ssl_sql")
    node.query(
        f"CREATE NAMED COLLECTION pg_ssl_sql AS "
        f"user = 'postgres', password = '{pg_pass}', host = '{PG_HOST}', port = 5432, "
        f"database = '{CERT_DB}', `table` = '{CERT_TABLE}', "
        f"sslmode = 'verify-full', sslrootcert_pem = '{quote_pem(ca_pem)}', "
        f"sslcert_pem = '{quote_pem(client_cert_pem)}', sslkey_pem = '{quote_pem(client_key_pem)}'"
    )
    assert node.query("SELECT count() FROM postgresql(pg_ssl_sql)").strip() == "10"

    # An empty contents override cannot replace the credential with another one, only
    # silently drop it -- it must be rejected also when the collection stores the
    # credential in the contents form rather than as a path.
    error = node.query_and_get_error("SELECT count() FROM postgresql(pg_ssl_sql, sslrootcert_pem='')")
    assert "cannot be overridden with an empty" in error

    # A path is still not accepted from a SQL-created collection, overridden or not.
    error = node.query_and_get_error(f"SELECT count() FROM postgresql(pg_ssl_sql, sslrootcert='{CA_CERT_PATH}')")
    assert "can only be specified in a named collection defined in the server configuration file" in error

    # After a restart the collection is loaded back from its on-disk metadata, so the
    # PEM contents must survive that round trip as well.
    node.restart_clickhouse()
    assert node.query("SELECT count() FROM postgresql(pg_ssl_sql)").strip() == "10"

    node.query("DROP NAMED COLLECTION pg_ssl_sql")
