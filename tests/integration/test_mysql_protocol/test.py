# coding: utf-8

import datetime
import math
import os
import time

import logging
import docker
import pymysql.connections
import pytest
from docker.models.containers import Container
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
    env_variables={"UBSAN_OPTIONS": "print_stacktrace=1"},
    with_mysql_client=True,
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
        [
            "docker-compose",
            "-p",
            cluster.project_name,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        ]
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.project_name + "_golang1_1")


@pytest.fixture(scope="module")
def php_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_php_client.yml"
    )
    run_and_check(
        [
            "docker-compose",
            "--env-file",
            cluster.instances["node"].env_file,
            "-p",
            cluster.project_name,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        ]
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.project_name + "_php1_1")


@pytest.fixture(scope="module")
def nodejs_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_js_client.yml"
    )
    run_and_check(
        [
            "docker-compose",
            "--env-file",
            cluster.instances["node"].env_file,
            "-p",
            cluster.project_name,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        ]
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.project_name + "_mysqljs1_1")


@pytest.fixture(scope="module")
def java_container():
    docker_compose = os.path.join(
        DOCKER_COMPOSE_PATH, "docker_compose_mysql_java_client.yml"
    )
    run_and_check(
        [
            "docker-compose",
            "--env-file",
            cluster.instances["node"].env_file,
            "-p",
            cluster.project_name,
            "-f",
            docker_compose,
            "up",
            "--force-recreate",
            "-d",
            "--no-build",
        ]
    )
    yield docker.DockerClient(
        base_url="unix:///var/run/docker.sock",
        version=cluster.docker_api_version,
        timeout=600,
    ).containers.get(cluster.project_name + "_java1_1")


def test_mysql_client(started_cluster):
    # type: (Container, str) -> None
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
        stderr.decode()
        == "mysql: [Warning] Using a password on the command line interface can be insecure.\n"
        "ERROR 516 (00000): default: Authentication failed: password is incorrect or there is no user with such name\n"
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
            "ERROR 81 (00000) at line 1: Code: 81. DB::Exception: Database system2 doesn't exist",
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
            "ERROR 1000 (00000) at line 1: Poco::Exception. Code: 1000, e.code() = 0, Exception: Connections to all replicas failed: default@127.0.0.1:10086 as user default",
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
    assert stdout.decode() == "DATABASE()\ndefault\n"

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
    assert stdout.decode() == "DATABASE()\ndefault\n"


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
    assert stdout.decode() == "currentUser()\ndefault\n"


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

        assert stdout.decode() == "\n".join(["col", "0", "1", "5", ""])

        code, (stdout, stderr) = started_cluster.mysql_client_container.exec_run(
            """
            mysql
            -e "INSERT INTO mysql_federated.test VALUES (0), (1), (5);"
            -e "SELECT * FROM mysql_federated.test ORDER BY col;"
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

    assert exc_info.value.args[1].startswith(
        "Code: 60. DB::Exception: Table default.tables doesn't exist"
    ), exc_info.value.args[1]

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

    assert exc_info.value.args == (
        516,
        "default: Authentication failed: password is incorrect or there is no user with such name",
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

    assert exc_info.value.args[1].startswith(
        "Code: 60. DB::Exception: Table default.tables doesn't exist"
    ), exc_info.value.args[1]

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("select 1 as a, 'тест' as b")
    assert cursor.fetchall() == [{"a": 1, "b": "тест"}]

    client.select_db("system")

    with pytest.raises(pymysql.InternalError) as exc_info:
        client.select_db("system2")

    assert exc_info.value.args[1].startswith(
        "Code: 81. DB::Exception: Database system2 doesn't exist"
    ), exc_info.value.args[1]

    cursor = client.cursor(pymysql.cursors.DictCursor)
    cursor.execute("CREATE DATABASE x")
    client.select_db("x")
    cursor.execute("CREATE TABLE table1 (a UInt32) ENGINE = Memory")
    cursor.execute("INSERT INTO table1 VALUES (1), (3)")
    cursor.execute("INSERT INTO table1 VALUES (1), (4)")
    cursor.execute("SELECT * FROM table1 ORDER BY a")
    assert cursor.fetchall() == [{"a": 1}, {"a": 1}, {"a": 3}, {"a": 4}]


def test_golang_client(started_cluster, golang_container):
    # type: (str, Container) -> None
    with open(os.path.join(SCRIPT_DIR, "golang.reference"), "rb") as fp:
        reference = fp.read()

    code, (stdout, stderr) = golang_container.exec_run(
        "./main --host {host} --port {port} --user default --password 123 --database "
        "abc".format(host=started_cluster.get_instance_ip("node"), port=server_port),
        demux=True,
    )

    assert code == 1
    assert stderr.decode() == "Error 81: Database abc doesn't exist\n"

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
    # type: (str, Container) -> None
    code, (stdout, stderr) = php_container.exec_run(
        "php -f test.php {host} {port} default 123".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test_ssl.php {host} {port} default 123".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test.php {host} {port} user_with_double_sha1 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\n"

    code, (stdout, stderr) = php_container.exec_run(
        "php -f test_ssl.php {host} {port} user_with_double_sha1 abacaba".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == "tables\ntables\n"


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


def test_java_client(started_cluster, java_container):
    # type: (str, Container) -> None
    with open(os.path.join(SCRIPT_DIR, "java.reference")) as fp:
        reference = fp.read()

    # database not exists exception.
    code, (stdout, stderr) = java_container.exec_run(
        "java JavaConnectorTest --host {host} --port {port} --user user_with_empty_password --database "
        "abc".format(host=started_cluster.get_instance_ip("node"), port=server_port),
        demux=True,
    )
    assert code == 1

    # empty password passed.
    code, (stdout, stderr) = java_container.exec_run(
        "java JavaConnectorTest --host {host} --port {port} --user user_with_empty_password --database "
        "default".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == reference

    # non-empty password passed.
    code, (stdout, stderr) = java_container.exec_run(
        "java JavaConnectorTest --host {host} --port {port} --user default --password 123 --database "
        "default".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == reference

    # double-sha1 password passed.
    code, (stdout, stderr) = java_container.exec_run(
        "java JavaConnectorTest --host {host} --port {port} --user user_with_double_sha1 --password abacaba  --database "
        "default".format(
            host=started_cluster.get_instance_ip("node"), port=server_port
        ),
        demux=True,
    )
    assert code == 0
    assert stdout.decode() == reference


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
