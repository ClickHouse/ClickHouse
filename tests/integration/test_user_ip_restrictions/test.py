import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

node_ipv4 = cluster.add_instance(
    "node_ipv4",
    main_configs=[],
    user_configs=["configs/users_ipv4.xml"],
    ipv4_address="10.5.172.77",  # Never copy-paste this line
)
client_ipv4_ok = cluster.add_instance(
    "client_ipv4_ok",
    main_configs=[],
    user_configs=[],
    ipv4_address="10.5.172.10",  # Never copy-paste this line
)
client_ipv4_ok_direct = cluster.add_instance(
    "client_ipv4_ok_direct",
    main_configs=[],
    user_configs=[],
    ipv4_address="10.5.173.1",  # Never copy-paste this line
)
client_ipv4_ok_full_mask = cluster.add_instance(
    "client_ipv4_ok_full_mask",
    main_configs=[],
    user_configs=[],
    ipv4_address="10.5.175.77",  # Never copy-paste this line
)
client_ipv4_bad = cluster.add_instance(
    "client_ipv4_bad",
    main_configs=[],
    user_configs=[],
    ipv4_address="10.5.173.10",  # Never copy-paste this line
)

node_ipv6 = cluster.add_instance(
    "node_ipv6",
    main_configs=["configs/config_ipv6.xml"],
    user_configs=["configs/users_ipv6.xml"],
    ipv6_address="2001:3984:3989::1:1000",  # Never copy-paste this line
)
client_ipv6_ok = cluster.add_instance(
    "client_ipv6_ok",
    main_configs=[],
    user_configs=[],
    ipv6_address="2001:3984:3989::5555",  # Never copy-paste this line
)
client_ipv6_ok_direct = cluster.add_instance(
    "client_ipv6_ok_direct",
    main_configs=[],
    user_configs=[],
    ipv6_address="2001:3984:3989::1:1111",  # Never copy-paste this line
)
client_ipv6_bad = cluster.add_instance(
    "client_ipv6_bad",
    main_configs=[],
    user_configs=[],
    ipv6_address="2001:3984:3989::1:1112",  # Never copy-paste this line
)


@pytest.fixture(scope="module")
def setup_cluster():
    try:
        cluster.start()
        yield cluster

    finally:
        cluster.shutdown()


def test_ipv4(setup_cluster):
    try:
        client_ipv4_ok.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 10.5.172.77 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
    except Exception as ex:
        assert (
            False
        ), "allowed client with 10.5.172.10 cannot connect to server with allowed mask '10.5.172.0/24'"

    try:
        client_ipv4_ok_direct.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 10.5.172.77 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
    except Exception as ex:
        assert (
            False
        ), "allowed client with 10.5.173.1 cannot connect to server with allowed ip '10.5.173.1'"

    try:
        client_ipv4_ok_full_mask.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 10.5.172.77 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
    except Exception as ex:
        assert (
            False
        ), "allowed client with 10.5.175.77 cannot connect to server with allowed ip '10.5.175.0/255.255.255.0'"

    try:
        client_ipv4_bad.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 10.5.172.77 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
        assert (
            False
        ), "restricted client with 10.5.173.10 can connect to server with allowed mask '10.5.172.0/24'"
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)


def test_ipv6(setup_cluster):
    try:
        client_ipv6_ok.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 2001:3984:3989::1:1000 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
    except Exception as ex:
        print(ex)
        assert (
            False
        ), "allowed client with 2001:3984:3989:0:0:0:1:1111 cannot connect to server with allowed mask '2001:3984:3989:0:0:0:0:0/112'"

    try:
        client_ipv6_ok_direct.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 2001:3984:3989:0:0:0:1:1000 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
    except Exception as ex:
        assert (
            False
        ), "allowed client with 2001:3984:3989:0:0:0:1:1111 cannot connect to server with allowed ip '2001:3984:3989:0:0:0:1:1111'"

    try:
        client_ipv6_bad.exec_in_container(
            [
                "bash",
                "-c",
                "/usr/bin/clickhouse client --host 2001:3984:3989:0:0:0:1:1000 --query 'select 1'",
            ],
            privileged=True,
            user="root",
        )
        assert (
            False
        ), "restricted client with 2001:3984:3989:0:0:0:1:1112 can connect to server with allowed mask '2001:3984:3989:0:0:0:0:0/112'"
    except AssertionError:
        raise
    except Exception as ex:
        print(ex)
