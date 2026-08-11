import time

import pytest

from helpers.cluster import ClickHouseCluster

from .common import check_on_cluster

cluster = ClickHouseCluster(__file__)

nodes = {
    "node0": cluster.add_instance(
        "node0",
        main_configs=["config/config_reload_discovery.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
    "node1": cluster.add_instance(
        "node1",
        main_configs=["config/config_reload_discovery.xml"],
        user_configs=["config/users.d/users_with_pwd.xml"],
        stay_alive=True,
        with_zookeeper=True,
    ),
}

CONFIG_PATH = "/etc/clickhouse-server/config.d/config_reload_discovery.xml"

CONFIG_WITH_PWD = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
                <user>user1</user>
                <password>password123</password>
            </discovery>
        </test_reload_cluster>
    </remote_servers>
</clickhouse>
"""

CONFIG_WITH_WRONG_PWD = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
                <user>user1</user>
                <password>wrongpass1234</password>
            </discovery>
        </test_reload_cluster>
    </remote_servers>
</clickhouse>
"""

CONFIG_PASSWORD_AND_SECRET = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
                <user>user1</user>
                <password>password123</password>
                <secret>cluster_secret_value</secret>
            </discovery>
        </test_reload_cluster>
        <test_partial_apply_marker>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_partial_apply_marker>
    </remote_servers>
</clickhouse>
"""

CONFIG_PASSWORD_AND_SECRET_ALLOW_OFF = """
<clickhouse>
    <allow_experimental_cluster_discovery>0</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
                <user>user1</user>
                <password>password123</password>
                <secret>cluster_secret_value</secret>
            </discovery>
        </test_reload_cluster>
        <test_partial_apply_marker_allow_off>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </test_partial_apply_marker_allow_off>
    </remote_servers>
</clickhouse>
"""

CONFIG_NO_DISCOVERY = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
    </remote_servers>
</clickhouse>
"""

CONFIG_WITH_CLUSTER_B = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster_b>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster_b</path>
            </discovery>
        </test_reload_cluster_b>
    </remote_servers>
</clickhouse>
"""

CONFIG_MULTICLUSTER_ROOT = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
            </discovery>
        </test_reload_cluster>
        <dynamic_roots>
            <discovery>
                <observer/>
                <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            </discovery>
        </dynamic_roots>
    </remote_servers>
</clickhouse>
"""

CONFIG_NO_MULTICLUSTER_ROOT = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_reload_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_reload_cluster</path>
            </discovery>
        </test_reload_cluster>
    </remote_servers>
</clickhouse>
"""

CONFIG_PARTICIPANT = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_observer_transition>
            <discovery>
                <path>/clickhouse/discovery/test_observer_transition</path>
            </discovery>
        </test_observer_transition>
    </remote_servers>
</clickhouse>
"""

CONFIG_OBSERVER = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_observer_transition>
            <discovery>
                <path>/clickhouse/discovery/test_observer_transition</path>
                <observer/>
            </discovery>
        </test_observer_transition>
    </remote_servers>
</clickhouse>
"""

CONFIG_INVISIBLE = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_invisible_transition>
            <discovery>
                <path>/clickhouse/discovery/test_invisible_transition</path>
                <invisible/>
            </discovery>
        </test_invisible_transition>
    </remote_servers>
</clickhouse>
"""

CONFIG_VISIBLE = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_invisible_transition>
            <discovery>
                <path>/clickhouse/discovery/test_invisible_transition</path>
            </discovery>
        </test_invisible_transition>
    </remote_servers>
</clickhouse>
"""


@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def wait_cluster_query(node, cluster_name, password="passwordAbc", should_succeed=True, retries=10):
    query = (
        f"SELECT sum(number) FROM clusterAllReplicas('{cluster_name}', numbers(3)) "
        f"GROUP BY hostname()"
    )
    last_error = ""
    for retry in range(retries):
        if should_succeed:
            try:
                result = node.query(query, password=password)
                if result.count("\n") >= 2:
                    return result
            except Exception as e:
                last_error = str(e)
        else:
            try:
                error = node.query_and_get_error(query, password=password)
                if "Authentication failed" in error or error:
                    return error
            except Exception as e:
                last_error = str(e)
        time.sleep(1 + retry)
    raise AssertionError(
        f"wait_cluster_query failed (should_succeed={should_succeed}): {last_error}"
    )


def reload_config_on_all(config_body):
    for node in nodes.values():
        node.replace_config(CONFIG_PATH, config_body)
        node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")


def reload_config_on_node(node, config_body):
    node.replace_config(CONFIG_PATH, config_body)
    node.query("SYSTEM RELOAD CONFIG", password="passwordAbc")


def test_reload_discovery_credentials(start_cluster):
    reload_config_on_all(CONFIG_WITH_PWD)

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Wrong nodes count after credential config apply",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)

    reload_config_on_all(CONFIG_WITH_WRONG_PWD)
    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=False)

    reload_config_on_all(CONFIG_WITH_PWD)
    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)


def test_reload_invalid_discovery_does_not_partially_apply(start_cluster):
    """Invalid discovery must fail the reload before Clusters / discovery diverge."""
    reload_config_on_all(CONFIG_WITH_PWD)
    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)

    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_PASSWORD_AND_SECRET)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG", password="passwordAbc")
        assert "password" in error and "secret" in error, error

    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)

    for node in nodes.values():
        count = int(
            node.query(
                "SELECT count() FROM system.clusters WHERE cluster = 'test_partial_apply_marker'",
                password="passwordAbc",
            )
        )
        assert count == 0, "Static cluster from rejected config was partially applied"

    reload_config_on_all(CONFIG_WITH_PWD)


def test_reload_invalid_discovery_allow_off_does_not_partially_apply(start_cluster):
    """Existing discovery must still validate when allow is turned off on reload."""
    reload_config_on_all(CONFIG_WITH_PWD)
    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)

    for node in nodes.values():
        node.replace_config(CONFIG_PATH, CONFIG_PASSWORD_AND_SECRET_ALLOW_OFF)
        error = node.query_and_get_error("SYSTEM RELOAD CONFIG", password="passwordAbc")
        assert "password" in error and "secret" in error, error

    wait_cluster_query(nodes["node0"], "test_reload_cluster", should_succeed=True)

    for node in nodes.values():
        count = int(
            node.query(
                "SELECT count() FROM system.clusters "
                "WHERE cluster = 'test_partial_apply_marker_allow_off'",
                password="passwordAbc",
            )
        )
        assert count == 0, "Static cluster from rejected allow=0 config was partially applied"

    reload_config_on_all(CONFIG_WITH_PWD)


def test_reload_add_remove_discovery_cluster(start_cluster):
    reload_config_on_all(CONFIG_NO_DISCOVERY)
    time.sleep(2)

    for node in nodes.values():
        count = int(
            node.query(
                "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster_b'",
                password="passwordAbc",
            )
        )
        assert count == 0

    reload_config_on_all(CONFIG_WITH_CLUSTER_B)
    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster_b",
        what="count()",
        msg="Cluster was not added after config reload",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    reload_config_on_all(CONFIG_NO_DISCOVERY)
    for retry in range(10):
        counts = [
            int(
                node.query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster_b'",
                    password="passwordAbc",
                )
            )
            for node in nodes.values()
        ]
        if all(c == 0 for c in counts):
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Cluster was not removed after config reload: {counts}")


def test_reload_remove_retries_failed_unregister(start_cluster):
    """Keeper unregister failure must not drop the remove; ephemeral cleanup is retried."""
    reload_config_on_all(CONFIG_WITH_PWD)
    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Cluster not ready before unregister-retry test",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    node0 = nodes["node0"]
    node1 = nodes["node1"]
    node0.query(
        "SYSTEM ENABLE FAILPOINT cluster_discovery_unregister_fail",
        password="passwordAbc",
    )
    try:
        reload_config_on_node(node0, CONFIG_NO_DISCOVERY)

        # Local config apply must succeed despite the failed Keeper remove.
        for retry in range(10):
            count = int(
                node0.query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                    password="passwordAbc",
                )
            )
            if count == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("node0 still exposes removed discovery cluster after reload")

        # Peer still sees node0 while the ephemeral linger is forced by the failpoint.
        for retry in range(10):
            hosts = int(
                node1.query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                    password="passwordAbc",
                )
            )
            if hosts == len(nodes):
                break
            time.sleep(1)
        else:
            raise AssertionError(
                "Expected node0 ephemeral to remain visible on node1 while unregister failpoint is on"
            )
    finally:
        node0.query(
            "SYSTEM DISABLE FAILPOINT cluster_discovery_unregister_fail",
            password="passwordAbc",
        )

    # After failpoint is cleared, worker retries remove the ephemeral.
    for retry in range(20):
        hosts = int(
            node1.query(
                "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                password="passwordAbc",
            )
        )
        if hosts == 1:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"node0 ephemeral was not cleaned up after unregister retry; hosts on node1={hosts}"
        )

    reload_config_on_all(CONFIG_WITH_PWD)


def test_reload_remove_readd_cancels_pending_unregister(start_cluster):
    """Re-adding the same discovery path must cancel a queued pending unregister."""
    reload_config_on_all(CONFIG_WITH_PWD)
    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Cluster not ready before remove/re-add unregister test",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    node0 = nodes["node0"]
    node1 = nodes["node1"]
    node0.query(
        "SYSTEM ENABLE FAILPOINT cluster_discovery_unregister_fail",
        password="passwordAbc",
    )
    try:
        reload_config_on_node(node0, CONFIG_NO_DISCOVERY)

        for retry in range(10):
            count = int(
                node0.query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                    password="passwordAbc",
                )
            )
            if count == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError("node0 still exposes removed discovery cluster after reload")

        # Re-add while unregister is still failing so pending cleanup remains queued.
        reload_config_on_node(node0, CONFIG_WITH_PWD)
        check_on_cluster(
            [node0, node1],
            len(nodes),
            cluster_name="test_reload_cluster",
            what="count()",
            msg="Cluster was not restored on node0 after re-add",
            query_params={"password": "passwordAbc"},
            retries=6,
        )
    finally:
        node0.query(
            "SYSTEM DISABLE FAILPOINT cluster_discovery_unregister_fail",
            password="passwordAbc",
        )

    # Pending retry must not delete the live ephemeral after re-add.
    for _ in range(15):
        hosts = int(
            node1.query(
                "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                password="passwordAbc",
            )
        )
        if hosts != len(nodes):
            raise AssertionError(
                f"Pending unregister deleted re-registered ephemeral; hosts on node1={hosts}"
            )
        time.sleep(1)

    reload_config_on_all(CONFIG_WITH_PWD)


def test_reload_add_remove_multicluster_root(start_cluster):
    reload_config_on_all(CONFIG_MULTICLUSTER_ROOT)

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Static discovery cluster missing",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    # Observer root should discover the static cluster under /clickhouse/discovery
    for retry in range(15):
        counts = [
            int(
                node.query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_reload_cluster'",
                    password="passwordAbc",
                )
            )
            for node in nodes.values()
        ]
        if all(c == len(nodes) for c in counts):
            break
        time.sleep(1)

    reload_config_on_all(CONFIG_NO_MULTICLUSTER_ROOT)
    # Static cluster must remain after multicluster root removal
    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Static cluster disappeared after multicluster root removal",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    reload_config_on_all(CONFIG_MULTICLUSTER_ROOT)
    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_reload_cluster",
        what="count()",
        msg="Static cluster missing after restoring multicluster root",
        query_params={"password": "passwordAbc"},
        retries=6,
    )


def test_reload_participant_to_observer_unregisters(start_cluster):
    """Participant -> observer reload must remove this node's ephemeral ZK registration."""
    reload_config_on_all(CONFIG_PARTICIPANT)

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_observer_transition",
        what="count()",
        msg="Both participants should be visible before observer transition",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    reload_config_on_node(nodes["node0"], CONFIG_OBSERVER)

    # node0 must disappear from node1's view without waiting for ZK session expiry.
    for retry in range(15):
        hosts = (
            nodes["node1"]
            .query(
                "SELECT host_name FROM system.clusters "
                "WHERE cluster = 'test_observer_transition' ORDER BY host_name",
                password="passwordAbc",
            )
            .strip()
            .split("\n")
        )
        hosts = [h for h in hosts if h]
        if hosts == ["node1"]:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"node0 still advertised after observer reload; hosts on node1: {hosts}"
        )

    # Observer still sees the remaining participant.
    check_on_cluster(
        [nodes["node0"]],
        1,
        cluster_name="test_observer_transition",
        what="count()",
        msg="Observer should still see the remaining participant",
        query_params={"password": "passwordAbc"},
        retries=6,
    )


def test_reload_invisible_to_visible_populates_cluster(start_cluster):
    """Invisible -> visible reload must upsert and publish nodes promptly."""
    reload_config_on_all(CONFIG_INVISIBLE)

    for retry in range(10):
        counts = [
            int(
                node.query(
                    "SELECT count() FROM system.clusters "
                    "WHERE cluster = 'test_invisible_transition'",
                    password="passwordAbc",
                )
            )
            for node in nodes.values()
        ]
        if all(c == 0 for c in counts):
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"Invisible cluster should not appear in system.clusters: {counts}"
        )

    reload_config_on_all(CONFIG_VISIBLE)

    check_on_cluster(
        list(nodes.values()),
        len(nodes),
        cluster_name="test_invisible_transition",
        what="count()",
        msg="Cluster did not appear after becoming visible",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    reload_config_on_all(CONFIG_INVISIBLE)

    for retry in range(15):
        counts = [
            int(
                node.query(
                    "SELECT count() FROM system.clusters "
                    "WHERE cluster = 'test_invisible_transition'",
                    password="passwordAbc",
                )
            )
            for node in nodes.values()
        ]
        if all(c == 0 for c in counts):
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"Cluster still visible after invisible reload: {counts}"
        )


def test_reload_static_replaces_dynamic_same_name(start_cluster):
    """Static <path> for a name already discovered via multicluster must replace it cleanly."""
    config_participant = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_collision_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_collision_cluster</path>
            </discovery>
        </test_collision_cluster>
    </remote_servers>
</clickhouse>
"""
    config_multicluster_observer = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <dynamic_roots>
            <discovery>
                <observer/>
                <multicluster_root_path>/clickhouse/discovery</multicluster_root_path>
            </discovery>
        </dynamic_roots>
    </remote_servers>
</clickhouse>
"""
    config_static_observer = """
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_collision_cluster>
            <discovery>
                <path>/clickhouse/discovery/test_collision_cluster</path>
                <observer/>
            </discovery>
        </test_collision_cluster>
    </remote_servers>
</clickhouse>
"""

    reload_config_on_node(nodes["node1"], config_participant)
    reload_config_on_node(nodes["node0"], config_multicluster_observer)

    check_on_cluster(
        [nodes["node0"]],
        1,
        cluster_name="test_collision_cluster",
        what="count()",
        msg="Observer should discover dynamic test_collision_cluster",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    # Replace dynamic discovery with static config of the same name.
    reload_config_on_node(nodes["node0"], config_static_observer)

    check_on_cluster(
        [nodes["node0"]],
        1,
        cluster_name="test_collision_cluster",
        what="count()",
        msg="Static observer should still see the participant after replacing dynamic",
        query_params={"password": "passwordAbc"},
        retries=6,
    )

    # Watches must still work: stopping the participant removes it from the static observer view.
    # start_clickhouse/wait_start cannot auth with users_with_pwd; use wait_for_start (TCP) instead.
    nodes["node1"].stop_clickhouse()
    try:
        for retry in range(15):
            count = int(
                nodes["node0"].query(
                    "SELECT count() FROM system.clusters WHERE cluster = 'test_collision_cluster'",
                    password="passwordAbc",
                )
            )
            if count == 0:
                break
            time.sleep(1)
        else:
            raise AssertionError(
                "Static observer did not drop participant after stop; watches likely broken"
            )
    finally:
        nodes["node1"].start_clickhouse(wait_start=False)
        nodes["node1"].wait_for_start(60)
        nodes["node1"].query("SELECT 1", password="passwordAbc")


def _registration_config(hostname, shard):
    return f"""
<clickhouse>
    <allow_experimental_cluster_discovery>1</allow_experimental_cluster_discovery>
    <remote_servers>
        <test_registration_reload>
            <discovery>
                <path>/clickhouse/discovery/test_registration_reload</path>
                <my_hostname>{hostname}</my_hostname>
                <shard>{shard}</shard>
            </discovery>
        </test_registration_reload>
    </remote_servers>
</clickhouse>
"""


def _registration_rows(node):
    return node.query(
        "SELECT host_name, shard_num FROM system.clusters "
        "WHERE cluster = 'test_registration_reload' ORDER BY host_name, shard_num "
        "FORMAT TSV",
        password="passwordAbc",
    ).strip()


def test_reload_my_hostname_and_shard_updates_local_and_peer(start_cluster):
    """Registration field reload must refresh payloads locally and on peers without membership churn."""
    reload_config_on_node(nodes["node0"], _registration_config("reg-host-node0", 1))
    reload_config_on_node(nodes["node1"], _registration_config("reg-host-node1", 1))

    expected_initial = "reg-host-node0\t1\nreg-host-node1\t1"
    for retry in range(15):
        rows = {_registration_rows(node) for node in nodes.values()}
        if rows == {expected_initial}:
            break
        time.sleep(1)
    else:
        raise AssertionError(f"Initial registration view not ready: {rows}")

    # Change hostname and shard on node1 only; UUID set stays the same.
    reload_config_on_node(nodes["node1"], _registration_config("reg-host-node1-renamed", 2))

    expected_updated = "reg-host-node0\t1\nreg-host-node1-renamed\t2"
    for retry in range(15):
        rows = {_registration_rows(node) for node in nodes.values()}
        if rows == {expected_updated}:
            break
        time.sleep(1)
    else:
        raise AssertionError(
            f"Hostname/shard reload did not propagate to local and peer system.clusters: {rows}"
        )