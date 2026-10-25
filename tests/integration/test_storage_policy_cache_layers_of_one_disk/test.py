import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", main_configs=["configs/storage_conf.xml"])

BAD_POLICY_CONFIG = "/etc/clickhouse-server/config.d/bad_policy.xml"


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
            "SELECT policy_name FROM system.storage_policies WHERE policy_name IN ('cache_1_only', 'cache_2_only') ORDER BY policy_name"
        )
        == "cache_1_only\ncache_2_only\n"
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
            f"Disks `{first_disk}` and `{second_disk}` in storage policy `bad_policy` store data on the same disk `blob`"
            in node.grep_in_log("store data on the same disk")
        )
    finally:
        node.exec_in_container(["rm", "-f", BAD_POLICY_CONFIG])
        node.query("SYSTEM RELOAD CONFIG")
