import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance('node', user_configs=["configs/users.xml"])


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        node.query("CREATE DATABASE test;")
        yield cluster
    finally:
        cluster.shutdown()


def test_user_zero_database_access(start_cluster):
    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'no_access' --query 'DROP DATABASE test'"], user='root')
        assert False, "user with no access rights dropped database test"
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'has_access' --query 'DROP DATABASE test'"], user='root')
    except Exception as ex:
        assert False, "user with access rights can't drop database test"

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'has_access' --query 'CREATE DATABASE test'"],
            user='root')
    except Exception as ex:
        assert False, "user with access rights can't create database test"

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'no_access' --query 'CREATE DATABASE test2'"],
            user='root')
        assert False, "user with no access rights created database test2"
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'has_access' --query 'CREATE DATABASE test2'"],
            user='root')
        assert False, "user with limited access rights created database test2 which is outside of his scope of rights"
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'default' --query 'CREATE DATABASE test2'"], user='root')
    except Exception as ex:
        assert False, "user with full access rights can't create database test2"

    try:
        node.exec_in_container(
            ["bash", "-c", "/usr/bin/clickhouse client --user 'default' --query 'DROP DATABASE test2'"], user='root')
    except Exception as ex:
        assert False, "user with full access rights can't drop database test2"
