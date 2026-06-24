import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node", stay_alive=True)


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


def setup_table(name, extra_settings=""):
    node.query(f"DROP TABLE IF EXISTS {name} SYNC")
    node.query("SYSTEM STOP MERGES")  # keep a single, predictable part
    settings = "min_bytes_for_wide_part = 0"
    if extra_settings:
        settings += ", " + extra_settings
    node.query(
        f"""CREATE TABLE {name} (key UInt64, id UInt64, value String,
            PROJECTION p (SELECT key, id, value ORDER BY id))
            ENGINE = MergeTree ORDER BY key SETTINGS {settings}"""
    )
    node.query(
        f"INSERT INTO {name} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )


def part_dir(name):  # absolute container path, no trailing slash: .../all_1_1_0
    return (
        node.query(
            f"SELECT path FROM system.parts WHERE table = '{name}' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )


def proj_query(name):
    return node.query(
        f"SELECT count(), sum(key) FROM {name} WHERE id < 200 SETTINGS optimize_use_projections = 1"
    ).strip()


def path_exists(p):
    return (
        node.exec_in_container(
            ["bash", "-c", f"test -e {p} && echo 1 || echo 0"],
            privileged=True,
            user="root",
        ).strip()
        == "1"
    )


def active_parts(name):
    return node.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1"
    ).strip()


def active_projection_parts(name):
    return node.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND active = 1"
    ).strip()


def test_default_nested_layout():
    setup_table("t_nested")
    p = part_dir("t_nested")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")
    baseline = proj_query("t_nested")
    node.restart_clickhouse()
    assert proj_query("t_nested") == baseline
    assert active_parts("t_nested") == "1"


def test_flat_layout_setting():
    setup_table("t_flat_setting", "projection_storage_format = 'flat'")
    p = part_dir("t_flat_setting")
    # server wrote the projection as a flat sibling, not nested
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    baseline = proj_query("t_flat_setting")

    # merge keeps the flat layout
    node.query(
        "INSERT INTO t_flat_setting SELECT number, number * 2, toString(number) FROM numbers(1000, 1000)"
    )
    node.query("SYSTEM START MERGES")
    node.query("OPTIMIZE TABLE t_flat_setting FINAL")
    merged = part_dir("t_flat_setting")
    assert path_exists(f"{merged}.p.proj")
    assert not path_exists(f"{merged}/p.proj")
    assert active_parts("t_flat_setting") == "1"
    assert int(active_projection_parts("t_flat_setting")) >= 1
    assert proj_query("t_flat_setting") == baseline

    # survives restart
    node.restart_clickhouse()
    assert active_parts("t_flat_setting") == "1"
    assert proj_query("t_flat_setting") == baseline


def test_flat_layout_after_relocation():
    setup_table("t_flat")
    p = part_dir("t_flat")
    baseline = proj_query("t_flat")
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {p}/p.proj {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_parts("t_flat") == "1"
    assert int(active_projection_parts("t_flat")) >= 1
    assert proj_query("t_flat") == baseline
