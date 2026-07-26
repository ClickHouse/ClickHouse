import pytest

from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)

# Any released version prior to 26.6 works as the "old" binary.
node = cluster.add_instance(
    "node",
    image="clickhouse/clickhouse-server",
    tag="25.12",
    # System logs are disabled to avoid pulling in newer server-side additions
    # (like the `table_readonly` MergeTree setting on rotated log tables, or
    # `with_types` `SerializationInfo` fields that gained keys after 25.12)
    # that the older binary started via `restart_with_original_version` would
    # not know and would fail to attach.
    main_configs=["configs/zz_disable_system_logs.xml"],
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


def rewrite_fires():
    # `EXPLAIN` labels the rewrite step with this description in `PlannerJoinTree`,
    # which lets us check whether the trivial-count-with-sparsity-filter step is
    # in the plan without depending on `system.query_log` (disabled above).
    plan = node.query(
        "EXPLAIN SELECT count() FROM t_compat WHERE x != 0 "
        "SETTINGS optimize_trivial_count_with_sparsity_filter = 1"
    )
    return "Optimized trivial count with sparsity filter" in plan


def scan_count():
    return node.query("SELECT count() FROM t_compat WHERE x != 0").strip()


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
                 compute_exact_num_defaults_for_sparse_columns = 1,
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

    assert '"exact_num_defaults":true' in read_serialization_json("all_1_1_0")
    assert rewrite_fires()
    assert scan_count() == BASELINE

    node.query("SYSTEM START MERGES t_compat")
    node.query("OPTIMIZE TABLE t_compat FINAL")
    assert active_part_count() == 1
    assert '"exact_num_defaults":true' in read_serialization_json(active_part_name())
    assert rewrite_fires()
    assert scan_count() == BASELINE

    # 25.12 does not know `compute_exact_num_defaults_for_sparse_columns` and would
    # refuse to load the CREATE TABLE that still mentions it. The parts are already
    # written with the `exact_num_defaults` flag, which is what the old binary needs
    # to tolerate. Strip the setting from the on-disk metadata before downgrading so
    # the old reader sees a CREATE TABLE it can parse.
    node.exec_in_container(
        ["bash", "-c",
         "sed -i 's/,[[:space:]]*compute_exact_num_defaults_for_sparse_columns[[:space:]]*=[[:space:]]*[0-9]\\+//g' "
         "/var/lib/clickhouse/metadata/default/t_compat.sql"],
        user="root",
    )

    node.restart_with_original_version()
    assert scan_count() == BASELINE

    node.query("OPTIMIZE TABLE t_compat FINAL")
    assert active_part_count() == 1
    assert "exact_num_defaults" not in read_serialization_json(active_part_name())

    node.restart_with_latest_version()
    assert "exact_num_defaults" not in read_serialization_json(active_part_name())
    # The rewrite must not fire on parts written by the old binary because the
    # flag is missing; the count still comes from an actual scan.
    assert not rewrite_fires()
    assert scan_count() == BASELINE

    node.query("DROP TABLE t_compat SYNC")
