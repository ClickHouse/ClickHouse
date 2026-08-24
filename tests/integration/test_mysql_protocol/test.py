# coding: utf-8

import datetime
import fnmatch
import logging
import math
import os
import socket
import struct
import time
from typing import Literal

import docker
import pymysql.connections
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
DOCKER_COMPOSE_PATH = get_docker_compose_path()


cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=[
        "configs/ssl_conf.xml",
        "configs/mysql.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/users.xml"],
    with_mysql_client=True,
)

node_secure = cluster.add_instance(
    "node_secure",
    main_configs=[
        "configs/ssl_conf.xml",
        "configs/mysql_secure.xml",
        "configs/dhparam.pem",
        "configs/server.crt",
        "configs/server.key",
    ],
    user_configs=["configs/users.xml"],
    with_mysql_client=True,
    with_mysql_dotnet_client=True
)

server_port = 9001


@pytest.fixture(scope="module")
def started_cluster():
    cluster.start()
    try:
        yield cluster
    finally:
        cluster.shutdown()


@pytest.fixture(scope="module")
def golang_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_golang_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "-p",
            cluster.project_name,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.get_instance_docker_id("golang1"))


@pytest.fixture(scope="module")
def php_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_php_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "--env-file",
            cluster.instances["node"].env_file,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.get_instance_docker_id("php1"))


@pytest.fixture(scope="module")
def nodejs_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_js_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "--env-file",
            cluster.instances["node"].env_file,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.get_instance_docker_id("mysqljs1"))


@pytest.fixture(scope="module")
def java_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_java_client.yml"
    )
    run_and_check(
        cluster.compose_cmd(
            "--env-file",
            cluster.instances["node"].env_file,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        )
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.get_instance_docker_id("java1"))


def test_mysql_client(started_cluster):
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u user_with_double_sha1 --password=abacaba
        -e "SELECT 1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    logging.debug(f"test_mysql_client code:{code} stdout:{stdout}, stderr:{stderr}")
    assert stdout.decode() == "\n".join(["1", "1", ""])

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "SELECT 1 as a;"
        -e "SELECT 'тест' as b;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert stdout.decode() == "\n".join(["a", "1", "b", "тест", ""])

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=abc -e "select 1 as a;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert (
        "mysql: [Warning] Using a password on the command line interface can be insecure.\n"
        "ERROR 516 (HY000): default: Authentication failed: password is incorrect, or there is no user with such name"
        in stderr.decode()
    )

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "use system;"
        -e "select count(*) from (select name from tables limit 1);"
        -e "use system2;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert stdout.decode() == "count()\n1\n"
    expected_msg = "\n".join(
        [
            "mysql: [Warning] Using a password on the command line interface can be insecure.",
            "ERROR 81 (HY000) at line 1: Code: 81. DB::Exception: Database system2 does not exist",
        ]
    )
    assert stderr[: len(expected_msg)].decode() == expected_msg

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "CREATE DATABASE x;"
        -e "USE x;"
        -e "CREATE TABLE table1 (column UInt32) ENGINE = Memory;"
        -e "INSERT INTO table1 VALUES (0), (1), (5);"
        -e "INSERT INTO table1 VALUES (0), (1), (5);"
        -e "SELECT * FROM table1 ORDER BY column;"
        -e "DROP DATABASE x;"
        -e "USE default;"
        -e "CREATE TEMPORARY TABLE tmp (tmp_column UInt32);"
        -e "INSERT INTO tmp VALUES (0), (1);"
        -e "SELECT * FROM tmp ORDER BY tmp_column;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert stdout.decode() == "\n".join(
        ["column", "0", "0", "1", "1", "5", "5", "tmp_column", "0", "1", ""]
    )

def test_mysql_client_secure(started_cluster):
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123 --ssl-mode=required -e "select 1 as a;"
    """.format(
            host=started_cluster.get_instance_ip("node_secure"), port=server_port
        ),
        demux=True,
    )

    logging.debug(f"test_mysql_client code:{code} stdout:{stdout}, stderr:{stderr}")
    assert stdout.decode() == "\n".join(["a", "1", ""])

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123 --ssl-mode=disabled -e "select 1 as a;"
    """.format(
            host=started_cluster.get_instance_ip("node_secure"), port=server_port
        ),
        demux=True,
    )

    logging.debug(f"test_mysql_client_secure code:{code} stdout:{stdout}, stderr:{stderr}")
    assert (
        "mysql: [Warning] Using a password on the command line interface can be insecure.\n"
        "ERROR 2013 (HY000): Lost connection to MySQL server at 'reading authorization packet', system error: 0"
        in stderr.decode()
    )

    assert node_secure.contains_in_log(
        "<Error> MySQLHandler: DB::Exception: SSL connection required."
    ) 


def test_mysql_client_exception(started_cluster):
    # Poco exception.
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "CREATE TABLE default.t1_remote_mysql AS mysql('127.0.0.1:10086','default','t1_local','default','');"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    expected_msg = "\n".join(
        [
            "mysql: [Warning] Using a password on the command line interface can be insecure.",
            "ERROR 279 (HY000) at line 1: Code: 279. DB::Exception: Connections to mysql failed: default@127.0.0.1:10086 as user default",
        ]
    )
    assert stderr[: len(expected_msg)].decode() == expected_msg


def test_mysql_affected_rows(started_cluster):
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "CREATE TABLE IF NOT EXISTS default.t1 (n UInt64) ENGINE MergeTree() ORDER BY tuple();"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql -vvv --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "INSERT INTO default.t1(n) VALUES(1);"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert code == 0
    assert "1 row affected" in stdout.decode()

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql -vvv --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "INSERT INTO default.t1(n) SELECT * FROM numbers(1000)"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert code == 0
    assert "1000 rows affected" in stdout.decode()

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "DROP TABLE default.t1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0


def test_mysql_replacement_query(started_cluster):
    # SHOW TABLE STATUS LIKE.
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "show table status like 'xx';"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # SHOW VARIABLES.
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "show variables;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # KILL QUERY.
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "kill query 0;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "kill query where query_id='mysql:0';"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # SELECT DATABASE().
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "select database();"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode().lower() in [
        "currentdatabase()\ndefault\n",
        "database()\ndefault\n",
    ]

    # SELECT SCHEMA().
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "select schema();"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode().lower() in [
        "currentdatabase()\ndefault\n",
        "schema()\ndefault\n",
    ]

    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default
        --password=123 -e "select DATABASE();"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode().lower() in [
        "currentdatabase()\ndefault\n",
        "database()\ndefault\n",
    ]


def test_mysql_replacement_query_injection(started_cluster):
    # The MySQL handler rewrites a few client commands (SHOW TABLE STATUS LIKE, SET <mysql_setting>,
    # KILL QUERY <id>) into ClickHouse SQL. The client-supplied argument must be parsed and re-quoted,
    # never concatenated verbatim, otherwise a client can inject arbitrary SQL over the MySQL wire.
    client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )

    # SHOW TABLE STATUS LIKE: a benign pattern reads only system.tables.
    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("SHOW TABLE STATUS LIKE 'one'")
    rows = cursor.fetchall()
    assert [r["Name"] for r in rows] == ["one"], rows

    # SHOW TABLE STATUS LIKE: an injected UNION must not leak system.users. The whole argument is
    # parsed as one string literal, so the UNION tail becomes part of the LIKE pattern and matches
    # nothing (the literal is unterminated -> parse fails -> empty pattern).
    cursor.execute(
        "SHOW TABLE STATUS LIKE '' "
        "UNION ALL SELECT name,'','','',0,0,0,0,0,0,'',now(),now(),now(),'','','','' "
        "FROM system.users"
    )
    leaked = [r["Name"] for r in cursor.fetchall()]
    assert "default" not in leaked, "SHOW TABLE STATUS LIKE injection leaked system.users: %s" % leaked

    # SHOW TABLE STATUS LIKE: a single trailing ';' is a statement terminator that programmatic
    # MySQL clients pass through (the mysql CLI strips it). executeQuery treats it as end-of-query,
    # so it must be accepted and produce the same result as without it.
    cursor.execute("SHOW TABLE STATUS LIKE 'one'")
    no_semicolon = [r["Name"] for r in cursor.fetchall()]
    cursor.execute("SHOW TABLE STATUS LIKE 'one';")
    with_semicolon = [r["Name"] for r in cursor.fetchall()]
    assert with_semicolon == no_semicolon and "one" in with_semicolon, with_semicolon

    # A second statement after the ';' must still be rejected (only a single terminator is allowed),
    # so the UNION tail cannot leak system.users.
    cursor.execute(
        "SHOW TABLE STATUS LIKE '' ; "
        "UNION ALL SELECT name,'','','',0,0,0,0,0,0,'',now(),now(),now(),'','','','' "
        "FROM system.users"
    )
    leaked = [r["Name"] for r in cursor.fetchall()]
    assert "default" not in leaked, "SHOW TABLE STATUS LIKE injection (trailing statement) leaked: %s" % leaked

    # SET <mysql_setting>: an injected value tail must be REJECTED, and a malformed input must not
    # change session state. Before the fix "SET SQL_SELECT_LIMIT=1, max_threads=42" became
    # "SET limit=1, max_threads=42" (injecting max_threads); an intermediate fix instead silently
    # reset the mapped `limit` to DEFAULT. Now any tail that is not exactly "= <literal|DEFAULT>"
    # (with an optional single trailing ';') is rejected, leaving both `limit` and `max_threads`
    # untouched. Use a fresh connection so the prior valid `limit` value is known to survive.
    set_client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )
    set_cursor = set_client.cursor(pymysql.cursors.DictCursor)
    set_cursor.execute("SET SQL_SELECT_LIMIT=4242")
    set_cursor.execute("SELECT changed FROM system.settings WHERE name = 'limit'")
    assert set_cursor.fetchone()["changed"] == 1, "SET SQL_SELECT_LIMIT did not apply"
    for injected in (
        "SET SQL_SELECT_LIMIT=1, max_threads=42",
        "SET SQL_SELECT_LIMIT=1, max_threads=42;",
    ):
        with pytest.raises(pymysql.Error):
            set_cursor.execute(injected)
        set_cursor.execute("SELECT value FROM system.settings WHERE name = 'max_threads'")
        assert set_cursor.fetchone()["value"] != "42", "SET replacement injected max_threads: %s" % injected
        set_cursor.execute("SELECT changed FROM system.settings WHERE name = 'limit'")
        assert set_cursor.fetchone()["changed"] == 1, "rejected SET silently reset `limit`: %s" % injected
    set_client.close()

    # A normal SET still works (value is re-serialized, not dropped).
    cursor.execute("SET SQL_SELECT_LIMIT=1234567")
    cursor.execute("SET NET_WRITE_TIMEOUT=60")
    cursor.execute("SET SQL_SELECT_LIMIT=DEFAULT")

    # SET name boundary: the prefix check is byte-wise, so a longer variable that merely starts
    # with a mapped name (SQL_SELECT_LIMITED vs SQL_SELECT_LIMIT) must NOT be translated and must
    # not touch the mapped `limit` setting. Without the boundary check the value parser failed on
    # the "ED=1" tail and the fallback emitted "SET limit = DEFAULT", silently resetting limit.
    # (`limit` reads back as 0 regardless of value, so assert on `changed`, not `value`.) Use a
    # fresh connection so the earlier SETs on `client` do not bleed into this check.
    boundary_client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )
    boundary = boundary_client.cursor(pymysql.cursors.DictCursor)
    boundary.execute("SET SQL_SELECT_LIMIT=4242")
    boundary.execute("SELECT changed FROM system.settings WHERE name = 'limit'")
    assert boundary.fetchone()["changed"] == 1, "SET SQL_SELECT_LIMIT did not apply"
    try:
        boundary.execute("SET SQL_SELECT_LIMITED=1")
    except pymysql.Error:
        pass  # an unrelated variable passes through untranslated and errors as an unknown setting
    boundary.execute("SELECT changed FROM system.settings WHERE name = 'limit'")
    assert boundary.fetchone()["changed"] == 1, "SET SQL_SELECT_LIMITED reset the unrelated `limit` setting"
    boundary_client.close()

    # SET <mysql_setting> with a trailing ';' must still apply the value. Before accepting the
    # terminator, "SET SQL_SELECT_LIMIT=2;" failed the value parser and reset limit to DEFAULT.
    # Use a fresh connection so `limit` starts unchanged and we can assert on `changed`.
    semicolon_client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )
    semicolon = semicolon_client.cursor(pymysql.cursors.DictCursor)
    semicolon.execute("SET SQL_SELECT_LIMIT=2;")
    semicolon.execute("SELECT changed FROM system.settings WHERE name = 'limit'")
    assert semicolon.fetchone()["changed"] == 1, "SET SQL_SELECT_LIMIT=2; did not apply"
    semicolon_client.close()

    # KILL QUERY: a numeric (possibly multi-digit) connection id is translated; previously the
    # "^[0-9]" check silently dropped every multi-digit id. A single trailing ';' is accepted
    # (programmatic clients pass it through), but a non-numeric tail or a second statement is not,
    # so it stays injection-safe and never falls through to the unsupported raw "KILL QUERY <id>".
    cursor.execute("KILL QUERY 12")
    cursor.execute("KILL QUERY 12;")
    with pytest.raises(pymysql.Error):
        cursor.execute("KILL QUERY 12; SELECT 1")
    cursor.execute("KILL QUERY WHERE query_id = 'mysql:0'")

    # KILL QUERY separator: the dispatcher matches the key "KILL QUERY" (no separator) but the
    # handler slices at len("KILL QUERY ") (with the space). Without a separator check a malformed
    # command would skip the non-space byte and be coerced into a valid cancel: "KILL QUERY;12" and
    # "KILL QUERYx12" both reached the digit regex as "12". They must instead be rejected (passed
    # through and erroring), so the id must be preceded by a real separator.
    for malformed in ("KILL QUERY;12", "KILL QUERYx12", "KILL QUERY12"):
        with pytest.raises(pymysql.Error):
            cursor.execute(malformed)

    # SHOW TABLE STATUS LIKE separator: same dispatcher-key vs prefix-length mismatch. Without the
    # separator check "SHOW TABLE STATUS LIKEx'one'" would skip the stray byte and look up 'one'.
    # It must instead match nothing (the malformed command is not coerced into a lookup).
    cursor.execute("SHOW TABLE STATUS LIKEx'one'")
    assert [r["Name"] for r in cursor.fetchall()] == [], "SHOW TABLE STATUS LIKE accepted a missing separator"
    client.close()


def test_mysql_select_user(started_cluster):
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "select user();"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() in ["currentUser()\ndefault\n", "user()\ndefault\n"]


def test_mysql_explain(started_cluster):
    # EXPLAIN SELECT 1
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "EXPLAIN SELECT 1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # EXPLAIN AST SELECT 1
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "EXPLAIN AST SELECT 1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # EXPLAIN PLAN SELECT 1
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "EXPLAIN PLAN SELECT 1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    # EXPLAIN PIPELINE graph=1 SELECT 1
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e "EXPLAIN PIPELINE graph=1 SELECT 1;"
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0


def test_mysql_federated(started_cluster):
    # For some reason it occasionally fails without retries.
    retries = 100
    for try_num in range(retries):
        node.query(
            """DROP DATABASE IF EXISTS mysql_federated""", settings={"password": "123"}
        )
        node.query("""CREATE DATABASE mysql_federated""", settings={"password": "123"})
        node.query(
            """CREATE TABLE mysql_federated.test (col UInt32) ENGINE = Log""",
            settings={"password": "123"},
        )
        node.query(
            """INSERT INTO mysql_federated.test VALUES (0), (1), (5)""",
            settings={"password": "123"},
        )

        def check_retryable_error_in_stderr(stderr):
            stderr = stderr.decode()
            return (
                "Can't connect to local MySQL server through socket" in stderr
                or "MySQL server has gone away" in stderr
                or "Server shutdown in progress" in stderr
            )

        code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
            """
            mysql
            -e "DROP SERVER IF EXISTS clickhouse;"
            -e "CREATE SERVER clickhouse FOREIGN DATA WRAPPER mysql
            OPTIONS (USER 'default', PASSWORD '123', HOST '{host}', PORT {port}, DATABASE 'mysql_federated');"
            -e "DROP DATABASE IF EXISTS mysql_federated;"
            -e "CREATE DATABASE mysql_federated;"
        """.format(
                host=started_cluster.get_instance_ip("node"), port=server_port
            ),
            demux=True,
        )

        if code != 0:
            print(("stdout", stdout))
            print(("stderr", stderr))
            if try_num + 1 < retries and check_retryable_error_in_stderr(stderr):
                time.sleep(1)
                continue
        assert code == 0

        code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
            """
            mysql
            -e "CREATE TABLE mysql_federated.test(`col` int UNSIGNED) ENGINE=FEDERATED CONNECTION='clickhouse';"
            -e "SELECT * FROM mysql_federated.test ORDER BY col;"
            """,
            demux=True,
        )

        if code != 0:
            print(("stdout", stdout))
            print(("stderr", stderr))
            if try_num + 1 < retries and check_retryable_error_in_stderr(stderr):
                time.sleep(1)
                continue
        assert code == 0

        assert stdout.decode() == "\n".join(["col", "0", "1", "5", ""])

        code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
            """
            mysql
            -e "INSERT INTO mysql_federated.test VALUES (0), (1), (5);"
            -e "SELECT * FROM mysql_federated.test ORDER BY col;"
            """,
            demux=True,
        )

        if code != 0:
            print(("stdout", stdout))
            print(("stderr", stderr))
            if try_num + 1 < retries and check_retryable_error_in_stderr(stderr):
                time.sleep(1)
                continue
        assert code == 0

        assert stdout.decode() == "\n".join(["col", "0", "0", "1", "1", "5", "5", ""])


def test_mysql_set_variables(started_cluster):
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u default --password=123
        -e
        "
        SET NAMES=default;
        SET character_set_results=default;
        SET FOREIGN_KEY_CHECKS=false;
        SET AUTOCOMMIT=1;
        SET sql_mode='strict';
        SET @@wait_timeout = 2147483;
        SET SESSION TRANSACTION ISOLATION LEVEL READ;
        "
    """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0


def test_mysql_boolean_format(started_cluster):
    node.query(
        """
            CREATE OR REPLACE TABLE mysql_boolean_format_test
            (
                `a` Bool,
                `b` Nullable(Bool),
                `c` LowCardinality(Nullable(Bool))
            ) ENGINE MergeTree ORDER BY a;
        """,
        settings={"password": "123", "allow_suspicious_low_cardinality_types": 1},
    )
    node.query(
        "INSERT INTO mysql_boolean_format_test VALUES (false, true, false), (true, false, true);",
        settings={"password": "123"},
    )
    code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
        """
        mysql --protocol tcp -h {host} -P {port} default -u user_with_double_sha1 --password=abacaba
        -e "SELECT * FROM mysql_boolean_format_test;"
        """.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    logging.debug(
        f"test_mysql_boolean_format code:{code} stdout:{stdout}, stderr:{stderr}"
    )
    assert stdout.decode() == "a\tb\tc\n" + "0\t1\t0\n" + "1\t0\t1\n"


def test_python_client(started_cluster):
    client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="user_with_double_sha1",
        password="abacaba",
        database="default",
        port=server_port,
    )

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.query("select name from tables")

    resp = exc_info.value.args[1]
    assert fnmatch.fnmatch(resp, "*DB::Exception:*tables*UNKNOWN_TABLE*"), resp

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select 1 as a, 'тест' as b")
    assert cursor.fetchall() == [{"a": 1, "b": "тест"}]

    with pytest.raises(pymysql.InternalError) as exc_info:
        pymysql.connections.Connection(
            host=started_cluster.get_instance_ip("node"),
            user="default",
            password="abacab",
            database="default",
            port=server_port,
        )

    assert exc_info.value.args[0] == 516
    assert (
        "default: Authentication failed: password is incorrect, or there is no user with such name"
        in exc_info.value.args[1]
    )

    client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.query("select name from tables")

    resp = exc_info.value.args[1]
    assert fnmatch.fnmatch(resp, "*DB::Exception:*tables*UNKNOWN_TABLE*"), resp

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select 1 as a, 'тест' as b")
    assert cursor.fetchall() == [{"a": 1, "b": "тест"}]

    client.select_db("system")

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.select_db("system2")

    assert exc_info.value.args[1].startswith(
        "Code: 81. DB::Exception: Database system2 does not exist"
    ), exc_info.value.args[1]

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("CREATE DATABASE x")
    client.select_db("x")
    cursor.execute("CREATE TABLE table1 (a UInt32) ENGINE = Memory")
    cursor.execute("INSERT INTO table1 VALUES (1), (3)")
    cursor.execute("INSERT INTO table1 VALUES (1), (4)")
    cursor.execute("SELECT * FROM table1 ORDER BY a")
    assert cursor.fetchall() == [{"a": 1}, {"a": 1}, {"a": 3}, {"a": 4}]
    cursor.execute("DROP DATABASE x")


def test_golang_client(started_cluster, golang_container):
    with open(os.path.join(SCRIPT_DIR, "golang.reference"), "rb") as fp:
        reference = fp.read()

    code, (stdout, stderr) = golang_container.exec_run(
        "./main --host {host} --port {port} --user default --password 123 --database "
        "abc".format(host=started_cluster.get_instance_ip("node"), port=server_port),
        demux=True,
    )

    assert code == 1
    assert stderr.decode() == "Error 81: Database abc does not exist\n"

    code, (stdout, stderr) = golang_container.exec_run(
        "./main --host {host} --port {port} --user default --password 123 --database "
        "default".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )

    assert code == 0
    assert stdout == reference

    code, (stdout, stderr) = golang_container.exec_run(
        "./main --host {host} --port {port} --user user_with_double_sha1 --password abacaba --database "
        "default".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout == reference


def test_php_client(started_cluster, php_container):
    code, (stdout, stderr) = php_container.exec_run(
        "php -f test.php {host} {port} default 123".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test_ssl.php {host} {port} default 123".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test.php {host} {port} user_with_double_sha1 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test_ssl.php {host} {port} user_with_double_sha1 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\ntables\n"


def test_mysqljs_client(started_cluster, nodejs_container):
    code, (_, stderr) = nodejs_container.exec_run(
        "node test.js {host} {port} user_with_sha256 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 1
    assert (
        "MySQL is requesting the sha256_password authentication method, which is not supported."
        in stderr.decode()
    )

    code, (_, stderr) = nodejs_container.exec_run(
        'node test.js {host} {port} user_with_empty_password ""'.format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    code, (_, _) = nodejs_container.exec_run(
        "node test.js {host} {port} user_with_double_sha1 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0

    code, (_, _) = nodejs_container.exec_run(
        "node test.js {host} {port} user_with_empty_password 123".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 1


def test_java_client_text(started_cluster, java_container):
    command = setup_java_client(started_cluster, "false")
    code, (stdout, stderr) = java_container.exec_run(
        command,
        demux=True,
    )

    with open(os.path.join(SCRIPT_DIR, "java_client.reference")) as fp:
        reference = fp.read()

    assert stdout.decode() == reference
    assert code == 0


def test_java_client_binary(started_cluster, java_container):
    command = setup_java_client(started_cluster, "true")
    code, (stdout, stderr) = java_container.exec_run(
        command,
        demux=True,
    )

    with open(os.path.join(SCRIPT_DIR, "java_client.reference")) as fp:
        reference = fp.read()

    assert stdout.decode() == reference
    assert code == 0


def test_types(started_cluster):
    client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="default",
        password="123",
        database="default",
        port=server_port,
    )

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute(
        "select "
        "toInt8(-pow(2, 7)) as Int8_column, "
        "toUInt8(pow(2, 8) - 1) as UInt8_column, "
        "toInt16(-pow(2, 15)) as Int16_column, "
        "toUInt16(pow(2, 16) - 1) as UInt16_column, "
        "toInt32(-pow(2, 31)) as Int32_column, "
        "toUInt32(pow(2, 32) - 1) as UInt32_column, "
        "toInt64('-9223372036854775808') as Int64_column, "  # -2^63
        "toUInt64('18446744073709551615') as UInt64_column, "  # 2^64 - 1
        "'тест' as String_column, "
        "toFixedString('тест', 8) as FixedString_column, "
        "toFloat32(1.5) as Float32_column, "
        "toFloat64(1.5) as Float64_column, "
        "toFloat32(NaN) as Float32_NaN_column, "
        "-Inf as Float64_Inf_column, "
        "toDate('2019-12-08') as Date_column, "
        "toDate('1970-01-01') as Date_min_column, "
        "toDate('1970-01-02') as Date_after_min_column, "
        "toDateTime('2019-12-08 08:24:03') as DateTime_column"
    )

    result = cursor.fetchall()[0]
    expected = [
        ("Int8_column", -(2**7)),
        ("UInt8_column", 2**8 - 1),
        ("Int16_column", -(2**15)),
        ("UInt16_column", 2**16 - 1),
        ("Int32_column", -(2**31)),
        ("UInt32_column", 2**32 - 1),
        ("Int64_column", -(2**63)),
        ("UInt64_column", 2**64 - 1),
        ("String_column", "тест"),
        ("FixedString_column", "тест"),
        ("Float32_column", 1.5),
        ("Float64_column", 1.5),
        ("Float32_NaN_column", float("nan")),
        ("Float64_Inf_column", float("-inf")),
        ("Date_column", datetime.date(2019, 12, 8)),
        ("Date_min_column", datetime.date(1970, 1, 1)),
        ("Date_after_min_column", datetime.date(1970, 1, 2)),
        ("DateTime_column", datetime.datetime(2019, 12, 8, 8, 24, 3)),
    ]

    for key, value in expected:
        if isinstance(value, float) and math.isnan(value):
            assert math.isnan(result[key])
        else:
            assert result[key] == value


def setup_java_client(started_cluster, binary: Literal["true", "false"]):
    with open(os.path.join(SCRIPT_DIR, "java_client_test.sql")) as sql:
        statements = list(
            filter(
                lambda s: s != "",
                map(lambda s: s.strip().replace("\n", " "), sql.read().split(";")),
            )
        )

    for statement in statements:
        node.query(
            statement,
            settings={"password": "123", "allow_suspicious_low_cardinality_types": 1},
        )

    return (
        "java MySQLJavaClientTest "
        "--host {host} "
        "--port {port} "
        "--user user_with_double_sha1 "
        "--password abacaba "
        "--database default "
        "--binary {binary}"
    ).format(
        host=started_cluster.get_instance_ip("node"), port=server_port, binary=binary
    )


def test_mysql_dotnet_client(started_cluster):
    node = cluster.instances["node"]

    with open(os.path.join(SCRIPT_DIR, "dotnet.reference")) as fp:
        reference = fp.read()

    res = started_cluster.exec_in_container(
        started_cluster.mysql_dotnet_client_docker_id,
        [
            "bash",
            "-c",
            f"cd /testapp && dotnet run -- --host {node.hostname} --port {server_port} --username default --password 123",
        ],
    )
    # there is some thrash at the beggining of output, so it's better to use `in` instead of `==``
    assert reference in res


def _com_field_list(client, table):
    # COM_FIELD_LIST returns one ColumnDefinition41 packet per column, terminated by an EOF/OK
    # packet. On access denial the server sends an ERR packet, which pymysql raises from read_packet().
    # The payload is a NUL-terminated table name followed by an (empty) field wildcard.
    client._execute_command(0x04, table + "\x00")
    columns = []
    packet = client._read_packet()
    while not (packet.is_eof_packet() or packet.is_ok_packet()):
        # ColumnDefinition41 layout: catalog, schema, table, org_table, name, org_name, ...
        # The column name is the 5th length-encoded string.
        for _ in range(4):
            packet.read_length_coded_string()
        columns.append(packet.read_length_coded_string().decode())
        packet = client._read_packet()
    return columns


def test_mysql_metadata_commands_access_control(started_cluster):
    # COM_FIELD_LIST (mysql_list_fields) and COM_INIT_DB (USE db) over the MySQL wire protocol
    # must enforce the same access control as the equivalent SQL statements (DESCRIBE / USE).
    node = cluster.instances["node"]
    # The `default` user in this test's users.xml has password 123, so native queries need it.
    creds = {"password": "123"}
    node.query("DROP USER IF EXISTS mysql_lowpriv", settings=creds)
    node.query(
        "CREATE TABLE IF NOT EXISTS default.mysql_acl_employees "
        "(id UInt64, name String, salary UInt64, ssn String) ENGINE=MergeTree ORDER BY id",
        settings=creds,
    )
    node.query("CREATE USER mysql_lowpriv IDENTIFIED WITH no_password", settings=creds)
    node.query(
        "GRANT SELECT(id, name) ON default.mysql_acl_employees TO mysql_lowpriv",
        settings=creds,
    )

    client = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="mysql_lowpriv",
        password="",
        database="default",
        port=server_port,
    )

    # COM_FIELD_LIST on a table the user lacks SHOW COLUMNS on must be rejected (used to leak all
    # column names regardless of column-level grants).
    with pytest.raises(pymysql.err.MySQLError) as exc_info:
        _com_field_list(client, "mysql_acl_employees")
    assert "ACCESS_DENIED" in str(exc_info.value), str(exc_info.value)
    assert "SHOW COLUMNS" in str(exc_info.value), str(exc_info.value)

    # COM_INIT_DB to a database the user has no grants on must be rejected (used to switch the
    # current database with no check, then COM_FIELD_LIST would leak system table schemas).
    with pytest.raises(pymysql.err.MySQLError) as exc_info:
        client.select_db("system")
    assert "ACCESS_DENIED" in str(exc_info.value), str(exc_info.value)
    assert "SHOW DATABASES" in str(exc_info.value), str(exc_info.value)

    client.close()

    # A user that is granted the table fully can still use both commands.
    node.query(
        "GRANT SHOW COLUMNS ON default.mysql_acl_employees TO mysql_lowpriv",
        settings=creds,
    )
    granted = pymysql.connections.Connection(
        host=started_cluster.get_instance_ip("node"),
        user="mysql_lowpriv",
        password="",
        database="default",
        port=server_port,
    )
    assert sorted(_com_field_list(granted, "mysql_acl_employees")) == [
        "id",
        "name",
        "salary",
        "ssn",
    ]
    granted.close()

    node.query("DROP USER mysql_lowpriv", settings=creds)
    node.query("DROP TABLE default.mysql_acl_employees", settings=creds)


def _mysql_recv_packet(sock):
    header = b""
    while len(header) < 4:
        chunk = sock.recv(4 - len(header))
        if not chunk:
            return None
        header += chunk
    length = header[0] | (header[1] << 8) | (header[2] << 16)
    body = b""
    while len(body) < length:
        chunk = sock.recv(length - len(body))
        if not chunk:
            return None
        body += chunk
    return body


def _mysql_send_packet(sock, seq, body):
    length = len(body)
    sock.sendall(bytes([length & 0xFF, (length >> 8) & 0xFF, (length >> 16) & 0xFF, seq]) + body)


def test_mysql_handshake_database_does_not_leak_columns(started_cluster):
    # The initial database in the MySQL handshake (CLIENT_CONNECT_WITH_DB) behaves like the
    # connection-time database of every other protocol (native --database, HTTP database=,
    # PostgreSQL startup): it is set without a SHOW_DATABASES check, so a connection succeeds even
    # to a database the user cannot otherwise see (only the explicit `USE` / COM_INIT_DB statement
    # is access-checked, matching InterpreterUseQuery). Selecting a database in the handshake must
    # not become a way to bypass COM_FIELD_LIST access control: even when connected to `system`,
    # COM_FIELD_LIST on a system table the user lacks SHOW COLUMNS on must still be rejected.
    # We drive the handshake over a raw socket so we can request a database with no grants on it.
    node = cluster.instances["node"]
    creds = {"password": "123"}
    node.query("DROP USER IF EXISTS mysql_handshake_lowpriv", settings=creds)
    node.query(
        "CREATE USER mysql_handshake_lowpriv IDENTIFIED WITH no_password", settings=creds
    )

    CLIENT_LONG_PASSWORD = 0x1
    CLIENT_LONG_FLAG = 0x4
    CLIENT_CONNECT_WITH_DB = 0x8
    CLIENT_PROTOCOL_41 = 0x200
    CLIENT_SECURE_CONNECTION = 0x8000
    CLIENT_PLUGIN_AUTH = 0x80000

    def open_handshake(database):
        sock = socket.create_connection(
            (started_cluster.get_instance_ip("node"), server_port), timeout=10
        )
        # Read and discard the server greeting.
        assert _mysql_recv_packet(sock) is not None
        capabilities = (
            CLIENT_LONG_PASSWORD
            | CLIENT_LONG_FLAG
            | CLIENT_CONNECT_WITH_DB
            | CLIENT_PROTOCOL_41
            | CLIENT_SECURE_CONNECTION
            | CLIENT_PLUGIN_AUTH
        )
        body = struct.pack("<I", capabilities)
        body += struct.pack("<I", 16 * 1024 * 1024)  # max packet size
        body += bytes([0x21])  # charset utf8_general_ci
        body += b"\x00" * 23  # reserved
        body += b"mysql_handshake_lowpriv\x00"
        body += bytes([0])  # empty auth response (no_password)
        body += database.encode() + b"\x00"
        body += b"mysql_native_password\x00"
        _mysql_send_packet(sock, 1, body)
        # The handshake completes with an OK packet (no SHOW_DATABASES check on the initial db).
        ok = _mysql_recv_packet(sock)
        assert ok is not None and ok[0] == 0x00, ok
        return sock

    def com_field_list(sock, table):
        # COM_FIELD_LIST (0x04): NUL-terminated table name + empty field wildcard.
        _mysql_send_packet(sock, 0, bytes([0x04]) + table.encode() + b"\x00")
        # On access denial the server replies with a single ERR packet (first byte 0xFF).
        pkt = _mysql_recv_packet(sock)
        assert pkt is not None, "connection closed instead of replying to COM_FIELD_LIST"
        return pkt

    # Connecting to `system` in the handshake succeeds (parity with other protocols).
    sock = open_handshake("system")
    # ... but COM_FIELD_LIST on a system table the user cannot see is still rejected: the handshake
    # database is not a way around the COM_FIELD_LIST access check.
    pkt = com_field_list(sock, "users")
    assert pkt[0] == 0xFF, pkt  # ERR packet
    assert b"ACCESS_DENIED" in pkt or b"SHOW COLUMNS" in pkt, pkt
    sock.close()

    # Connecting to `default` (the user's accessible database) also succeeds.
    sock = open_handshake("default")
    sock.close()

    node.query("DROP USER mysql_handshake_lowpriv", settings=creds)
