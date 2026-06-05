import inspect
from dataclasses import dataclass
from os import path as p

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.keeper_utils import (
    get_active_zk_connections,
    replace_zookeeper_config,
    reset_zookeeper_config,
)
from helpers.test_tools import TSV, assert_eq_with_retry

default_zk_config = p.join(p.dirname(p.realpath(__file__)), "configs/zookeeper.xml")
cluster = ClickHouseCluster(__file__, zookeeper_config_path="configs/zookeeper.xml")

node1 = cluster.add_instance(
    "node1",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/config.xml"],
    with_zookeeper=True,
    stay_alive=True,
)

all_nodes = [node1, node2]


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


@dataclass(frozen=True)
class Entity:
    keyword: str
    name: str
    options: str = ""


entities = [
    Entity(keyword="USER", name="theuser"),
    Entity(keyword="ROLE", name="therole"),
    Entity(keyword="ROW POLICY", name="thepolicy", options=" ON default.t1"),
    Entity(keyword="QUOTA", name="thequota"),
    Entity(keyword="SETTINGS PROFILE", name="theprofile"),
]


def get_entity_id(entity):
    return entity.keyword.replace(" ", "_")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
        in node2.query_and_get_error_with_retry(
            f"CREATE {entity.keyword} {entity.name} {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_and_delete_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    node2.query_with_retry(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_on_cluster(started_cluster, entity):
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
        in node1.query_and_get_error(
            f"CREATE {entity.keyword} {entity.name} ON CLUSTER default {entity.options}"
        )
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_on_cluster_ignore(started_cluster, entity):
    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_access_entities_queries>true</ignore_on_cluster_for_replicated_access_entities_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")

    node1.query(
        f"CREATE {entity.keyword} {entity.name} ON CLUSTER default {entity.options}"
    )
    assert (
        f"cannot insert because {entity.keyword.lower()} `{entity.name}{entity.options}` already exists in `replicated`"
        in node2.query_and_get_error_with_retry(
            f"CREATE {entity.keyword} {entity.name} {entity.options}"
        )
    )

    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")

    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default/>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")


@pytest.mark.parametrize(
    "use_on_cluster",
    [
        pytest.param(False, id="Without_on_cluster"),
        pytest.param(True, id="With_ignored_on_cluster"),
    ],
)
def test_grant_revoke_replicated(started_cluster, use_on_cluster: bool):
    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default>
                        <ignore_on_cluster_for_replicated_access_entities_queries>{int(use_on_cluster)}</ignore_on_cluster_for_replicated_access_entities_queries>
                    </default>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")
    on_cluster = "ON CLUSTER default" if use_on_cluster else ""

    node1.query(f"CREATE USER theuser2 {on_cluster}")

    assert node1.query(f"GRANT {on_cluster} SELECT ON *.* to theuser2") == ""

    assert node2.query(f"SHOW GRANTS FOR theuser2") == "GRANT SELECT ON *.* TO theuser2\n"

    assert node1.query(f"REVOKE {on_cluster} SELECT ON *.* from theuser2") == ""
    node1.query(f"DROP USER theuser2 {on_cluster}")

    node1.replace_config(
        "/etc/clickhouse-server/users.d/users.xml",
        inspect.cleandoc(
            f"""
            <clickhouse>
                <profiles>
                    <default/>
                </profiles>
            </clickhouse>
            """
        ),
    )
    node1.query("SYSTEM RELOAD CONFIG")



@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_create_replicated_if_not_exists_on_cluster(started_cluster, entity):
    node1.query(
        f"CREATE {entity.keyword} IF NOT EXISTS {entity.name} ON CLUSTER default {entity.options}"
    )
    node1.query(f"DROP {entity.keyword} {entity.name} {entity.options}")


@pytest.mark.parametrize("entity", entities, ids=get_entity_id)
def test_rename_replicated(started_cluster, entity):
    node1.query(f"CREATE {entity.keyword} {entity.name} {entity.options}")
    node2.query_with_retry(
        f"ALTER {entity.keyword} {entity.name} {entity.options} RENAME TO {entity.name}2"
    )
    node1.query("SYSTEM RELOAD USERS")
    node1.query(f"DROP {entity.keyword} {entity.name}2 {entity.options}")


# ReplicatedAccessStorage must be able to continue working after reloading ZooKeeper.
def test_reload_zookeeper(started_cluster):
    node1.query("CREATE USER u1")
    assert_eq_with_retry(
        node2, "SELECT name FROM system.users WHERE name ='u1'", "u1\n"
    )

    ## remove zoo2, zoo3 from configs
    replace_zookeeper_config(
        (node1, node2),
        """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo1</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
""",
    )

    ## config reloads, but can still work
    node1.query("CREATE USER u2")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name",
        TSV(["u1", "u2"]),
    )

    ## stop all zookeepers, users will be readonly
    cluster.stop_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## start zoo2, zoo3, users will be readonly too, because it only connect to zoo1
    cluster.start_zookeeper_nodes(["zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo2", "zoo3"])
    assert node2.query(
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2'] ORDER BY name"
    ) == TSV(["u1", "u2"])
    assert "ZooKeeper" in node1.query_and_get_error("CREATE USER u3")

    ## set config to zoo2, server will be normal
    replace_zookeeper_config(
        (node1, node2),
        """
<clickhouse>
    <zookeeper>
        <node index="1">
            <host>zoo2</host>
            <port>2181</port>
        </node>
        <session_timeout_ms>2000</session_timeout_ms>
    </zookeeper>
</clickhouse>
""",
    )

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    node1.query("CREATE USER u3")
    assert_eq_with_retry(
        node2,
        "SELECT name FROM system.users WHERE name IN ['u1', 'u2', 'u3'] ORDER BY name",
        TSV(["u1", "u2", "u3"]),
    )

    active_zk_connections = get_active_zk_connections(node1)
    assert (
        len(active_zk_connections) == 1
    ), "Total connections to ZooKeeper not equal to 1, {}".format(active_zk_connections)

    # Restore the test state
    node1.query("DROP USER u1, u2, u3")
    cluster.start_zookeeper_nodes(["zoo1", "zoo2", "zoo3"])
    cluster.wait_zookeeper_nodes_to_start(["zoo1", "zoo2", "zoo3"])
    reset_zookeeper_config((node1, node2), default_zk_config)


# Stress parameters for the UNKNOWN_ROLE race regression test below.
RACE_SEED_ENTITIES = 1500
RACE_CHURNERS = 6
RACE_VICTIMS = 6
RACE_DURATION_SECONDS = 45

# In-container driver. Numeric parameters (SEED/CHURNERS/VICTIMS/DURATION) are
# prepended as shell variable assignments by the test, so this body needs no
# Python string substitution (keeps the literal shell braces intact).
#
# Why an in-container bash driver instead of Python threads or `xargs -P`:
#
#   * The race window is narrow, so reproduction reliability is a function of
#     iterations-per-second. Each worker iteration spawns a `clickhouse-client`
#     process; running the hot loop inside the container keeps that cost local.
#     Driving it from the host with `node.query` would add a host<->container
#     round trip to every iteration, lowering the iteration rate and forcing a
#     longer DURATION to stay reliable -- trading proven reproduction (it hits
#     UNKNOWN_ROLE in ~17s on a buggy build) for stylistic tidiness.
#
#   * `xargs -P` only replaces the `& ... wait` fan-out (two lines). The bulk
#     here is the worker bodies (time-bounded loop + shared FOUND stop-flag),
#     the multi-statement victim SQL and the seeding -- none of which `xargs`
#     removes. Worse, `xargs` runs an external command, not a shell function,
#     so each worker body would have to be re-nested inside another `bash -c`
#     string (a third level of quoting). These are long-lived workers with
#     shared state and early exit, not a command mapped over a list, so
#     background jobs + `wait` are the idiomatic and more readable fit.
_RACE_DRIVER_BODY = r"""
set -u
CLIENT="clickhouse-client"

# Bloat the /uuid children list so the (previously lock-free) snapshot read in
# refreshEntities is slow and the eviction window is wide.
for i in $(seq 0 $((SEED - 1))); do
    echo "CREATE SETTINGS PROFILE IF NOT EXISTS seed_$i SETTINGS max_execution_time=$((i % 100 + 1));"
done | $CLIENT --multiquery

FOUND=/tmp/race_found
rm -f "$FOUND"
END=$(( $(date +%s) + DURATION ))

# Keep the /uuid children list volatile so a stale-snapshot watch refresh is
# almost always in flight.
churn() {
    local w=$1 i=0 n
    while [ "$(date +%s)" -lt "$END" ] && [ ! -e "$FOUND" ]; do
        n="churn_${w}_$((i % 4))"
        $CLIENT --multiquery -q "CREATE SETTINGS PROFILE IF NOT EXISTS $n SETTINGS max_execution_time=$((i % 50 + 1)); DROP SETTINGS PROFILE IF EXISTS $n;" >/dev/null 2>&1
        i=$((i + 1))
    done
}

# Create a role and immediately reference it by name (CREATE USER ... DEFAULT
# ROLE, CREATE QUOTA ... TO). On the buggy build the role gets evicted from the
# local cache in between and one of these fails with UNKNOWN_ROLE.
victim() {
    local w=$1 name="race_$w" out
    while [ "$(date +%s)" -lt "$END" ] && [ ! -e "$FOUND" ]; do
        out=$($CLIENT --multiquery -q "
            DROP SETTINGS PROFILE IF EXISTS $name;
            DROP QUOTA IF EXISTS $name;
            DROP USER IF EXISTS $name;
            DROP ROLE IF EXISTS $name;
            CREATE SETTINGS PROFILE $name SETTINGS max_execution_time=60;
            CREATE ROLE $name SETTINGS PROFILE $name;
            CREATE USER $name IDENTIFIED BY 'p' DEFAULT ROLE $name;
            CREATE QUOTA $name KEYED BY user_name FOR INTERVAL 1 hour NO LIMITS TO $name;
        " 2>&1)
        case "$out" in
            *UNKNOWN_ROLE* | *"There is no role"*)
                echo "$out" > "$FOUND"
                return ;;
        esac
    done
}

for w in $(seq 1 $CHURNERS); do churn "$w" & done
for w in $(seq 1 $VICTIMS); do victim "$w" & done
wait

if [ -e "$FOUND" ]; then
    echo "RACE_DETECTED"
    cat "$FOUND"
    exit 0
fi
echo "NO_RACE"
"""


def _race_cleanup(node):
    # Drop everything the race regression test may have created.
    stmts = [
        f"DROP SETTINGS PROFILE IF EXISTS seed_{i}" for i in range(RACE_SEED_ENTITIES)
    ]
    for w in range(RACE_CHURNERS):
        for s in range(4):
            stmts.append(f"DROP SETTINGS PROFILE IF EXISTS churn_{w + 1}_{s}")
    for w in range(RACE_VICTIMS):
        stmts += [
            f"DROP QUOTA IF EXISTS race_{w + 1}",
            f"DROP USER IF EXISTS race_{w + 1}",
            f"DROP ROLE IF EXISTS race_{w + 1}",
            f"DROP SETTINGS PROFILE IF EXISTS race_{w + 1}",
        ]
    node.query(";\n".join(stmts))


# Regression test for a TOCTOU race in ReplicatedAccessStorage: the background
# watch thread used to read the /uuid children list without holding the mutex and
# then evict everything not in that (stale) snapshot via removeAllExcept. A role
# created and immediately referenced by name on the same connection could be
# evicted from the local cache in between, so the next statement that resolves the
# role failed with "There is no role ... (UNKNOWN_ROLE)" even though CREATE ROLE
# had just succeeded. https://github.com/ClickHouse/ClickHouse/issues/106521
#
# This is a probabilistic stress test: it reliably fails on the buggy build and
# never false-positives on the fixed build (within a victim the role is created
# and used with nothing dropping it in between, so the only thing that could
# remove it was the racing watch eviction).
def test_create_role_then_reference_it_no_unknown_role(started_cluster):
    node = node1
    params = (
        f"SEED={RACE_SEED_ENTITIES}\n"
        f"CHURNERS={RACE_CHURNERS}\n"
        f"VICTIMS={RACE_VICTIMS}\n"
        f"DURATION={RACE_DURATION_SECONDS}\n"
    )
    driver = params + _RACE_DRIVER_BODY
    try:
        result = node.exec_in_container(["bash", "-c", driver])
        assert "RACE_DETECTED" not in result, (
            "ReplicatedAccessStorage evicted a freshly created role "
            "(UNKNOWN_ROLE race):\n" + result
        )
        assert "NO_RACE" in result, result
    finally:
        _race_cleanup(node)
