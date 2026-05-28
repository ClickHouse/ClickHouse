import uuid

import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Any released version prior to 26.6 works as the "old" binary.
node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag="25.12",
    with_installed_binary=True,
    stay_alive=True,
)


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def active_part_count():
    return int(node.query(
        "SELECT count() FROM system.parts "
        "WHERE database = 'default' AND table = 't_compat' AND active"
    ).strip())


def active_part_name():
    return node.query(
        "SELECT name FROM system.parts "
        "WHERE database = 'default' AND table = 't_compat' AND active "
        "ORDER BY name LIMIT 1"
    ).strip()


def read_serialization_json(part_name):
    return node.exec_in_container(
        ["bash", "-c", f"cat /var/lib/clickhouse/data/default/t_compat/{part_name}/serialization.json"],
        user="root",
    )


def count_with_trivial_count_setting(setting_value):
    query_id = str(uuid.uuid4())
    count = node.query(
        "SELECT count() FROM t_compat WHERE x != 0 "
        f"SETTINGS optimize_trivial_count_with_sparsity_filter = {setting_value}",
        query_id=query_id,
    ).strip()
    node.query("SYSTEM FLUSH LOGS query_log")
    read_rows = int(node.query(
        "SELECT read_rows FROM system.query_log "
        f"WHERE query_id = '{query_id}' AND type = 'QueryFinish' "
        "ORDER BY event_time_microseconds DESC LIMIT 1"
    ).strip())
    return count, read_rows


def test_exact_num_defaults_compat(started_cluster):
    node.restart_with_latest_version()

    node.query("DROP TABLE IF EXISTS t_compat SYNC")
    # `basic` format only carries `version` and `columns`. The `with_types` format
    # gained a top level `propagate_types_serialization_versions_to_nested_types`
    # key after 25.12, and 25.12's reader strict rejects unknown top level keys.
    # `exact_num_defaults` lives inside each column object, which 25.12 reads by
    # named lookup and silently ignores extras, so the compatibility property
    # under test is preserved.
    node.query(
        """
        CREATE TABLE t_compat (id UInt64, x UInt32)
        ENGINE = MergeTree ORDER BY id
        SETTINGS index_granularity = 512,
                 ratio_of_defaults_for_sparse_serialization = 0.5,
                 serialization_info_version = 'basic',
                 min_bytes_for_wide_part = 0
        """
    )
    node.query("SYSTEM STOP MERGES t_compat")
    node.query(
        "INSERT INTO t_compat SELECT number, if(number < 3000, 0, 1)::UInt32 "
        "FROM numbers(5000) SETTINGS optimize_on_insert = 0"
    )
    node.query(
        "INSERT INTO t_compat SELECT number + 5000, if(number < 3000, 0, 1)::UInt32 "
        "FROM numbers(5000) SETTINGS optimize_on_insert = 0"
    )
    assert active_part_count() == 2
    BASELINE = "4000"

    # When the rewrite fires, `read_rows` is just the single prepared row
    # emitted by `ReadFromPreparedSource`. When it doesn't fire, the count comes
    # from an actual scan of the column data, so `read_rows` matches what running
    # the same query with the setting disabled would scan.
    def assert_trivial_count_was_used(used):
        count_on,  read_rows_on  = count_with_trivial_count_setting(1)
        count_off, read_rows_off = count_with_trivial_count_setting(0)
        assert count_on == BASELINE and count_off == BASELINE
        if used:
            assert read_rows_on <= 1 and read_rows_on < read_rows_off
        else:
            assert read_rows_on == read_rows_off

    assert '"exact_num_defaults":true' in read_serialization_json("all_1_1_0")
    assert_trivial_count_was_used(used=True)

    node.query("SYSTEM START MERGES t_compat")
    node.query("OPTIMIZE TABLE t_compat FINAL")
    assert active_part_count() == 1
    assert '"exact_num_defaults":true' in read_serialization_json(active_part_name())
    assert_trivial_count_was_used(used=True)

    node.restart_with_original_version()
    assert node.query("SELECT count() FROM t_compat WHERE x != 0").strip() == BASELINE

    node.query("OPTIMIZE TABLE t_compat FINAL")
    assert active_part_count() == 1
    assert "exact_num_defaults" not in read_serialization_json(active_part_name())

    node.restart_with_latest_version()
    assert "exact_num_defaults" not in read_serialization_json(active_part_name())
    assert_trivial_count_was_used(used=False)

    node.query("DROP TABLE t_compat SYNC")
