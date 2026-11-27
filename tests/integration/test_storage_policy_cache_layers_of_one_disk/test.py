import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node", main_configs=["configs/storage_conf.xml"], stay_alive=True
)

BAD_POLICY_CONFIG = "/etc/clickhouse-server/config.d/bad_policy.xml"
RESTART_POLICY_CONFIG = "/etc/clickhouse-server/config.d/restart_policy.xml"


@pytest.fixture(scope="module", autouse=True)
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_cache_layers_of_one_disk_in_separate_policies():
    assert (
        node.query(
            "SELECT DISTINCT policy_name FROM system.storage_policies WHERE policy_name IN ('cache_1_only', 'cache_2_only', 'blob_and_encrypted_subdir') ORDER BY policy_name"
        )
        == "blob_and_encrypted_subdir\ncache_1_only\ncache_2_only\n"
    )
    node.query(
        "CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'cache_2_only'"
    )
    node.query("INSERT INTO t SELECT number FROM numbers(100)")
    assert node.query("SELECT count() FROM t") == "100\n"
    node.query("DROP TABLE t SYNC")


@pytest.mark.parametrize(
    "volumes, first_disk, second_disk",
    [
        ("<main><disk>cache_1</disk><disk>cache_2</disk></main>", "cache_1", "cache_2"),
        (
            "<hot><disk>blob</disk></hot><cold><disk>cache_1</disk></cold>",
            "blob",
            "cache_1",
        ),
        (
            "<main><disk>cache_1</disk><disk>cache_of_cache_1</disk></main>",
            "cache_1",
            "cache_of_cache_1",
        ),
        (
            "<main><disk>encrypted_1</disk><disk>cache_2</disk></main>",
            "encrypted_1",
            "cache_2",
        ),
        (
            "<main><disk>encrypted_1</disk><disk>encrypted_2</disk></main>",
            "encrypted_1",
            "encrypted_2",
        ),
        (
            "<main><disk>encrypted_subdir</disk><disk>encrypted_subdir_dot</disk></main>",
            "encrypted_subdir",
            "encrypted_subdir_dot",
        ),
    ],
)
def test_policy_with_two_disks_over_one_disk_is_rejected(
    volumes, first_disk, second_disk
):
    count_bad_policy = (
        "SELECT count() FROM system.storage_policies WHERE policy_name = 'bad_policy'"
    )
    assert node.query(count_bad_policy) == "0\n"

    node.replace_config(
        BAD_POLICY_CONFIG,
        "<clickhouse><storage_configuration><policies><bad_policy>"
        f"<volumes>{volumes}</volumes>"
        "</bad_policy></policies></storage_configuration></clickhouse>",
    )
    try:
        node.query("SYSTEM RELOAD CONFIG")
        assert node.query(count_bad_policy) == "0\n"
        assert (
            f"Disks `{first_disk}` and `{second_disk}` in storage policy `bad_policy` store data in the same directory"
            in node.grep_in_log("store data in the same directory")
        )
    finally:
        node.exec_in_container(["rm", "-f", BAD_POLICY_CONFIG])
        node.query("SYSTEM RELOAD CONFIG")


def restart_policy(disks):
    return (
        "<clickhouse><storage_configuration><policies><restart_policy><volumes><main>"
        + "".join(f"<disk>{disk}</disk>" for disk in disks)
        + "</main></volumes></restart_policy></policies></storage_configuration></clickhouse>"
    )


def test_server_does_not_start_with_bad_policy_and_keeps_parts():
    node.replace_config(RESTART_POLICY_CONFIG, restart_policy(["cache_1"]))
    node.query("SYSTEM RELOAD CONFIG")
    node.query(
        "CREATE TABLE t_restart (x UInt64) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = 'restart_policy'"
    )
    node.query("INSERT INTO t_restart SELECT number FROM numbers(100)")
    node.query("INSERT INTO t_restart SELECT number FROM numbers(100, 100)")

    node.stop_clickhouse()
    node.replace_config(RESTART_POLICY_CONFIG, restart_policy(["cache_1", "cache_2"]))
    node.start_clickhouse(expected_to_fail=True)
    assert (
        "Disks `cache_1` and `cache_2` in storage policy `restart_policy` store data in the same directory"
        in node.grep_in_log("store data in the same directory")
    )

    node.replace_config(RESTART_POLICY_CONFIG, restart_policy(["cache_1"]))
    node.start_clickhouse()
    assert node.query("SELECT count(), sum(x) FROM t_restart") == "200\t19900\n"
    node.query("DROP TABLE t_restart SYNC")
