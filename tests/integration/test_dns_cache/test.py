import logging
import time

import pytest

from concurrent.futures import ThreadPoolExecutor

from helpers.client import QueryRuntimeException
from helpers.cluster import ClickHouseCluster
from helpers.test_tools import TSV, assert_eq_with_retry

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/listen_host.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1111",
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/listen_host.xml", "configs/dns_update_long.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1112",
)
node3 = cluster.add_instance(
    "node3",
    main_configs=["configs/listen_host.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1113",
)
node4 = cluster.add_instance(
    "node4",
    main_configs=[
        "configs/remote_servers.xml",
        "configs/listen_host.xml",
        "configs/dns_update_short.xml",
    ],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1114",
)
# Check SYSTEM DROP DNS CACHE on node5 and background cache update on node6
node5 = cluster.add_instance(
    "node5",
    main_configs=["configs/listen_host.xml", "configs/dns_update_long.xml"],
    user_configs=["configs/users_with_hostname.xml"],
    ipv6_address="2001:3984:3989::1:1115",
)
node6 = cluster.add_instance(
    "node6",
    main_configs=["configs/listen_host.xml", "configs/dns_update_short.xml"],
    user_configs=["configs/users_with_hostname.xml"],
    ipv6_address="2001:3984:3989::1:1116",
)
node7 = cluster.add_instance(
    "node7",
    main_configs=["configs/listen_host.xml", "configs/dns_update_long.xml"],
    with_zookeeper=True,
    ipv6_address="2001:3984:3989::1:1117",
    ipv4_address="10.5.95.17",
)
node8 = cluster.add_instance(
    "node8",
    main_configs=[
        "configs/remote_servers_with_disable_dns_setting.xml",
        "configs/listen_host.xml"
    ],
    stay_alive=True,
    ipv6_address="2001:3984:3989::1:1118",
)


def _fill_nodes(nodes, table_name):
    for node in nodes:
        node.query(
            """
            CREATE DATABASE IF NOT EXISTS test;
            CREATE TABLE IF NOT EXISTS {0}(date Date, id UInt32)
            ENGINE = ReplicatedMergeTree('/clickhouse/tables/test/{0}', '{1}')
            ORDER BY id PARTITION BY toYYYYMM(date);
            """.format(
                table_name, node.name
            )
        )


@pytest.fixture(scope="module", autouse=True)
def cluster_start():
    try:
        cluster.start()

        _fill_nodes([node1, node2], "test_table_drop")
        _fill_nodes([node3, node4], "test_table_update")

        yield cluster

    except Exception as ex:
        print(ex)
        raise

    finally:
        cluster.shutdown()


@pytest.fixture(scope="function", autouse=True)
def cluster_ready(cluster_start):
    """
    A fixture to check that all nodes in the cluster is up and running.
    Many failures were found after the random-order + flaky testing were run on the file
    """
    try:
        for node in cluster_start.instances.values():
            node.wait_for_start(10)

        yield cluster

    except Exception as ex:
        print(ex)
        raise


# node1 is a source, node2 downloads data
# node2 has long dns_cache_update_period, so dns cache update wouldn't work
def test_ip_change_drop_dns_cache(cluster_ready):
    # Preserve node1 ipv6 before changing it
    node1_ipv6 = node1.ipv6_address
    # In this case we should manually set up the static DNS entries on the source host
    # to exclude resplving addresses automatically added by docker.
    # We use ipv6 for hosts, but resolved DNS entries may contain an unexpected ipv4 address.
    node2.set_hosts([(node1_ipv6, "node1")])
    # drop DNS cache
    node2.query("SYSTEM DROP DNS CACHE")
    node2.query("SYSTEM DROP CONNECTIONS CACHE")

    # First we check, that normal replication works
    node1.query(
        "INSERT INTO test_table_drop VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
    )
    assert node1.query("SELECT count(*) from test_table_drop") == "3\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "3")

    # We change source node ip
    node1_new_ipv6 = "2001:3984:3989::1:7777"
    cluster.restart_instance_with_ip_change(node1, node1_new_ipv6)
    node2.set_hosts([(node1_new_ipv6, "node1")])

    # Put some data to source node1
    node1.query(
        "INSERT INTO test_table_drop VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)"
    )
    # Check that data is placed on node1
    assert node1.query("SELECT count(*) from test_table_drop") == "6\n"

    # Because of DNS cache dest node2 cannot download data from node1
    with pytest.raises(Exception):
        assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "6")

    # drop DNS cache
    node2.query("SYSTEM DROP DNS CACHE")
    node2.query("SYSTEM DROP CONNECTIONS CACHE")
    # Data is downloaded
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "6")

    # Just to be sure check one more time
    node1.query("INSERT INTO test_table_drop VALUES ('2018-10-01', 8)")
    assert node1.query("SELECT count(*) from test_table_drop") == "7\n"
    assert_eq_with_retry(node2, "SELECT count(*) from test_table_drop", "7")

    # Reset the nodes state
    node1.query("TRUNCATE TABLE test_table_drop")
    node2.query("TRUNCATE TABLE test_table_drop")
    cluster.restart_service("node2")
    cluster.restart_instance_with_ip_change(node1, node1_ipv6)


# node3 is a source, node4 downloads data
# node4 has short dns_cache_update_period, so testing update of dns cache
def test_ip_change_update_dns_cache(cluster_ready):
    # Preserve original IP before change
    node3_ipv6 = node3.ipv6_address
    # First we check, that normal replication works
    node3.query(
        "INSERT INTO test_table_update VALUES ('2018-10-01', 1), ('2018-10-02', 2), ('2018-10-03', 3)"
    )
    assert node3.query("SELECT count(*) from test_table_update") == "3\n"
    assert_eq_with_retry(node4, "SELECT count(*) from test_table_update", "3")

    # We change source node ip
    cluster.restart_instance_with_ip_change(node3, "2001:3984:3989::1:8888")

    # Put some data to source node3
    node3.query(
        "INSERT INTO test_table_update VALUES ('2018-10-01', 5), ('2018-10-02', 6), ('2018-10-03', 7)"
    )

    # Check that data is placed on node3
    assert node3.query("SELECT count(*) from test_table_update") == "6\n"

    curl_result = node4.exec_in_container(["bash", "-c", "curl -s 'node3:8123'"])
    assert curl_result == "Ok.\n"
    cat_resolv = node4.exec_in_container(["bash", "-c", "cat /etc/resolv.conf"])
    print(("RESOLV {}".format(cat_resolv)))

    assert_eq_with_retry(
        node4, "SELECT * FROM remote('node3', 'system', 'one')", "0", sleep_time=0.5
    )

    # Because of DNS cache update, ip of node3 would be updated
    assert_eq_with_retry(
        node4, "SELECT count(*) from test_table_update", "6", sleep_time=3
    )

    # Just to be sure check one more time
    node3.query("INSERT INTO test_table_update VALUES ('2018-10-01', 8)")
    assert node3.query("SELECT count(*) from test_table_update") == "7\n"
    assert_eq_with_retry(node4, "SELECT count(*) from test_table_update", "7")

    # Reset the test state
    node3.query("TRUNCATE TABLE test_table_update")
    node4.query("TRUNCATE TABLE test_table_update")
    cluster.restart_instance_with_ip_change(node3, node3_ipv6)


def test_dns_cache_update(cluster_ready):
    node4.set_hosts([("127.255.255.255", "lost_host")])

    with pytest.raises(QueryRuntimeException):
        node4.query("SELECT * FROM remote('lost_host', 'system', 'one')")

    node4.query(
        "CREATE TABLE distributed_lost_host (dummy UInt8) ENGINE = Distributed(lost_host_cluster, 'system', 'one')"
    )
    with pytest.raises(QueryRuntimeException):
        node4.query("SELECT * FROM distributed_lost_host")

    node4.set_hosts([("127.0.0.1", "lost_host")])

    # Wait a bit until dns cache will be updated
    assert_eq_with_retry(
        node4, "SELECT * FROM remote('lost_host', 'system', 'one')", "0"
    )
    assert_eq_with_retry(node4, "SELECT * FROM distributed_lost_host", "0")

    assert TSV(
        node4.query(
            "SELECT DISTINCT host_name, host_address FROM system.clusters WHERE cluster='lost_host_cluster'"
        )
    ) == TSV("lost_host\t127.0.0.1\n")
    assert TSV(node4.query("SELECT hostName()")) == TSV("node4")

    # Reset the node4 state
    node4.set_hosts([])
    node4.query("DROP TABLE distributed_lost_host")
    # Probably a bug: `SYSTEM DROP DNS CACHE` doesn't work with distributed engine
    cluster.restart_service("node4")


@pytest.mark.parametrize("node_name", ["node5", "node6"])
def test_user_access_ip_change(cluster_ready, node_name):
    node = cluster.instances[node_name]
    node_num = node.name[-1]
    # getaddrinfo(...) may hang for a log time without this options
    node.exec_in_container(
        [
            "bash",
            "-c",
            'echo -e "options timeout:1\noptions attempts:2" >> /etc/resolv.conf',
        ],
        privileged=True,
        user="root",
    )

    assert_eq_with_retry(
        node3,
        f"SELECT * FROM remote('{node_name}', 'system', 'one')",
        "0",
        retry_count=10,
        sleep_time=10,
    )

    assert_eq_with_retry(
        node4,
        f"SELECT * FROM remote('{node_name}', 'system', 'one')",
        "0",
        retry_count=10,
        sleep_time=10,
    )

    node3_ipv6 = node3.ipv6_address
    node4_ipv6 = node4.ipv6_address

    node.set_hosts(
        [
            ("127.255.255.255", "node3"),
            (f"2001:3984:3989::1:88{node_num}4", "unknown_host"),
        ],
    )

    # restart the node and return the time taken for restart
    def restart_with_timing(node, new_ip):
        """Restart a single node and return the time taken"""
        start_time = time.time()
        cluster.restart_instance_with_ip_change(node, new_ip)
        elapsed = time.time() - start_time
        return node.name, elapsed

    # restart the nodes concurrently and time each node separately
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda x: restart_with_timing(x[0], x[1]),
                [
                    (node3, f"2001:3984:3989::1:88{node_num}3"),
                    (node4, f"2001:3984:3989::1:88{node_num}4"),
                ],
            )
        )

    # choose the maximum individual restart time for the timing decision
    max_individual_restart = max(elapsed for _, elapsed in results)
    logging.info(f"Slowest node restart: {max_individual_restart:.2f}s")

    if max_individual_restart < 5:
        # Add small buffer to ensure container networking is stable
        time.sleep(1)
        with pytest.raises(QueryRuntimeException):
            node3.query(f"SELECT * FROM remote('{node_name}', 'system', 'one')")
    else:
        # The server restart took more time than expected, so it's probable that the DNS cache has already been reloaded
        logging.warning("Spent too much time on restart, skip short-update test")

    with pytest.raises(QueryRuntimeException):
        node4.query(f"SELECT * FROM remote('{node_name}', 'system', 'one')")
    # now wrong addresses are cached

    node.set_hosts([])
    retry_count = 60
    if node_name == "node5":
        # client is not allowed to connect, so execute it directly in container to send query from localhost
        node.exec_in_container(
            ["bash", "-c", 'clickhouse client -q "SYSTEM DROP DNS CACHE"'],
            privileged=True,
            user="root",
        )
        node.exec_in_container(
            ["bash", "-c", 'clickhouse client -q "SYSTEM DROP CONNECTIONS CACHE"'],
            privileged=True,
            user="root",
        )
        retry_count = 1

    assert_eq_with_retry(
        node3,
        f"SELECT * FROM remote('{node_name}', 'system', 'one')",
        "0",
        retry_count=retry_count,
        sleep_time=1,
    )
    assert_eq_with_retry(
        node4,
        f"SELECT * FROM remote('{node_name}', 'system', 'one')",
        "0",
        retry_count=retry_count,
        sleep_time=1,
    )
    # Reset the test state to the initial
    cluster.restart_instance_with_ip_change(node3, node3_ipv6)
    cluster.restart_instance_with_ip_change(node4, node4_ipv6)
    if node_name == "node5":
        # Node 5 has internal state changed and must be restarted
        cluster.restart_service(node_name)


def test_host_is_drop_from_cache_after_consecutive_failures(cluster_ready):
    with pytest.raises(QueryRuntimeException):
        node4.query(
            "SELECT * FROM remote('InvalidHostThatDoesNotExist', 'system', 'one')"
        )

    # Note that the list of hosts in variable since lost_host will be there too (and it's dropped and added back)
    # dns_update_short -> dns_max_consecutive_failures set to 6
    assert node4.wait_for_log_line(
        regexp="Code: 198. DB::NetException: Not found address of host: InvalidHostThatDoesNotExist.",
        # There's noize in a normal log, let's search the error log for the exception
        filename="/var/log/clickhouse-server/clickhouse-server.err.log",
    )
    assert node4.wait_for_log_line(
        "Cached hosts not found:.*InvalidHostThatDoesNotExist**",
        repetitions=6,
        timeout=60,
        # <Test> log level could break it, so we're looking far behind
        look_behind_lines=15000,
    )
    assert node4.wait_for_log_line(
        "Cached hosts dropped:.*InvalidHostThatDoesNotExist.*",
        # Again, another fuze for <Test> noize in normal log after possible restart
        look_behind_lines=15000,
    )


def _render_filter_config(allow_ipv4, allow_ipv6):
    config = f"""
    <clickhouse>
        <dns_allow_resolve_names_to_ipv4>{int(allow_ipv4)}</dns_allow_resolve_names_to_ipv4>
        <dns_allow_resolve_names_to_ipv6>{int(allow_ipv6)}</dns_allow_resolve_names_to_ipv6>
    </clickhouse>
    """
    return config


@pytest.mark.parametrize(
    "allow_ipv4, allow_ipv6",
    [
        (True, False),
        (False, True),
        (False, False),
    ],
)
def test_dns_resolver_filter(cluster_ready, allow_ipv4, allow_ipv6):
    node = node7
    host_ipv6 = node.ipv6_address
    host_ipv4 = node.ipv4_address

    node.set_hosts(
        [
            (host_ipv6, "test_host"),
            (host_ipv4, "test_host"),
        ]
    )
    node.replace_config(
        "/etc/clickhouse-server/config.d/dns_filter.xml",
        _render_filter_config(allow_ipv4, allow_ipv6),
    )

    node.query("SYSTEM RELOAD CONFIG")
    node.query("SYSTEM DROP DNS CACHE")
    node.query("SYSTEM DROP CONNECTIONS CACHE")

    if not allow_ipv4 and not allow_ipv6:
        with pytest.raises(QueryRuntimeException):
            node.query("SELECT * FROM remote('lost_host', 'system', 'one')")
    else:
        node.query("SELECT * FROM remote('test_host', system, one)")
        assert (
            node.query(
                "SELECT ip_address FROM system.dns_cache WHERE hostname='test_host'"
            )
            == f"{host_ipv4 if allow_ipv4 else host_ipv6}\n"
        )

    node.exec_in_container(
        [
            "bash",
            "-c",
            "rm /etc/clickhouse-server/config.d/dns_filter.xml",
        ],
        privileged=True,
        user="root",
    )
    node.query("SYSTEM RELOAD CONFIG")


@pytest.mark.parametrize("disable_internal_dns_cache", [1, 0])
def test_setting_disable_internal_dns_cache(cluster_ready, disable_internal_dns_cache):
    node = node8
    # DNSCacheUpdater has to be created before any scenario that requires
    # DNS resolution (e.g. the loading of tables and clusters config).
    node.replace_in_config(
        "/etc/clickhouse-server/config.d/remote_servers_with_disable_dns_setting.xml",
        "<disable_internal_dns_cache>[10]</disable_internal_dns_cache>",
        f"<disable_internal_dns_cache>{disable_internal_dns_cache}</disable_internal_dns_cache>"
    )
    node.restart_clickhouse()

    if disable_internal_dns_cache == 1:
        assert node.query("SELECT count(*) from system.dns_cache;") == "0\n"
    else:
        assert node.query("SELECT count(*) from system.dns_cache;") != "0\n"
