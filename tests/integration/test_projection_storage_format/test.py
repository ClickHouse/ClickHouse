import time

import pytest
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)
node2 = cluster.add_instance(
    "node2",
    main_configs=["configs/storage_conf.xml", "configs/backups.xml"],
    with_minio=True,
    with_zookeeper=True,
    stay_alive=True,
)

# Each known issue links to its PR review comment.
REVIEW = "https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r"


@pytest.fixture(scope="module", autouse=True)
def start_cluster():
    try:
        cluster.start()
        yield
    finally:
        cluster.shutdown()


# ==============================================================================
# Shared helpers
# ==============================================================================
def setup_table(name, extra_settings="", n=node):
    n.query(f"DROP TABLE IF EXISTS {name} SYNC")
    n.query("SYSTEM STOP MERGES")  # keep a single, predictable part
    settings = "min_bytes_for_wide_part = 0"
    if extra_settings:
        settings += ", " + extra_settings
    n.query(
        f"""CREATE TABLE {name} (key UInt64, id UInt64, value String,
            PROJECTION p (SELECT key, id, value ORDER BY id))
            ENGINE = MergeTree ORDER BY key SETTINGS {settings}"""
    )
    n.query(
        f"INSERT INTO {name} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )


def part_dir(name, n=node):  # absolute container path, no trailing slash: .../all_1_1_0
    return (
        n.query(
            f"SELECT path FROM system.parts WHERE table = '{name}' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )


def part_name(name, n=node):
    return part_dir(name, n).split("/")[-1]


def table_path(name, n=node):
    return (
        n.query(f"SELECT data_paths[1] FROM system.tables WHERE name = '{name}'")
        .strip()
        .rstrip("/")
    )


def proj_query(name, n=node, extra_settings=""):
    settings = "optimize_use_projections = 1"
    if extra_settings:
        settings += ", " + extra_settings
    return n.query(
        f"SELECT count(), sum(key) FROM {name} WHERE id < 200 SETTINGS {settings}"
    ).strip()


def check_table(name, n=node):
    return n.query(
        f"CHECK TABLE {name} SETTINGS check_query_single_value_result = 1"
    ).strip()


def path_exists(p, n=node):
    return (
        n.exec_in_container(
            ["bash", "-c", f"test -e {p} && echo 1 || echo 0"],
            privileged=True,
            user="root",
        ).strip()
        == "1"
    )


def active_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 1"
    ).strip()


def active_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND active = 1"
    ).strip()


def broken_projection_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.projection_parts WHERE table = '{name}' AND is_broken"
    ).strip()


def outdated_parts(name, n=node):
    return n.query(
        f"SELECT count() FROM system.parts WHERE table = '{name}' AND active = 0"
    ).strip()


def wait_for(predicate, timeout=60):
    for _ in range(timeout * 2):
        if predicate():
            return
        time.sleep(0.5)
    raise AssertionError(f"wait_for timed out after {timeout}s")


def block_until_tables_loaded(name, n=node):
    # Reads the real table to wait out its async load (async_load_databases = 1); a system.parts read does not.
    # Call after a (re)start before asserting on system.* so the assert does not race the background loader.
    n.query(f"SELECT count() FROM {name}")


def plant_stale_tmp_dir(stale, n=node):
    """Simulate leftovers of a failed operation: a stale temporary part dir plus its
    flat projection sibling, both containing a marker file. chmod so the server
    (clickhouse user) can remove root-created content."""
    n.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {stale} {stale}.p.proj"
            f" && touch {stale}/stale_marker.txt {stale}.p.proj/stale_marker.txt"
            f" && chmod -R 777 {stale} {stale}.p.proj",
        ],
        privileged=True,
        user="root",
    )


def plant_stale_live_sibling(path):
    """A flat projection sibling under a LIVE part name whose parent never committed:
    the residue of a publish that crashed between the sibling and parent renames."""
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {path} && touch {path}/stale_marker.txt && chmod -R 777 {path}",
        ],
        privileged=True,
        user="root",
    )


def minio_keys():
    return {
        o.object_name
        for o in cluster.minio_client.list_objects(
            cluster.minio_bucket, "data/", recursive=True
        )
    }


def sibling_blob_keys(uuid, part, n=node):
    # local_path is relative to the disk root; the store/<prefix>/<uuid>/ segment is unique per table
    return set(
        n.query(
            f"""SELECT remote_path FROM system.remote_data_paths
                WHERE local_path LIKE '%/{uuid}/{part}.p.proj/%'"""
        ).split()
    )


# ==============================================================================
# A. Layout basics and format selection
# ==============================================================================

# This test checks that a projection defaults to the nested layout when the format setting is
# unset, so a flat sibling is never written by accident.
# Scenario:
# - create table with a projection and the default (unset) storage format
# - assert structure: the projection is nested inside the part, no flat sibling
# - assert SELECT through the projection
# - restart, assert SELECT unchanged and the part is intact
def test_layout_default_is_nested():
    # create table with the default (unset) storage format
    setup_table("t_nested")

    # assert structure: nested inside the part, no flat sibling
    p = part_dir("t_nested")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")

    # assert SELECT through the projection
    baseline = proj_query("t_nested")

    # restart, assert SELECT unchanged and the part intact
    node.restart_clickhouse()
    assert proj_query("t_nested") == baseline
    assert active_parts("t_nested") == "1"


# This test checks that projection_storage_format = 'flat' writes the projection as a flat sibling
# and that the layout survives a merge and a restart, not just the initial insert.
# Scenario:
# - create table with a 'flat' projection
# - assert structure: the projection is a flat sibling, not nested
# - INSERT a second part and MERGE (OPTIMIZE TABLE FINAL)
# - assert structure: the merged part keeps the flat layout, SELECT matches
# - restart, assert the part and SELECT survive
def test_layout_flat_setting_persists():
    # create table with a 'flat' projection
    setup_table("t_flat_setting", "projection_storage_format = 'flat'")

    # assert structure: the server wrote the projection as a flat sibling, not nested
    p = part_dir("t_flat_setting")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    baseline = proj_query("t_flat_setting")

    # insert a second part and merge; the merge keeps the flat layout
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

    # restart, assert the part and SELECT survive
    node.restart_clickhouse()
    block_until_tables_loaded("t_flat_setting")
    assert active_parts("t_flat_setting") == "1"
    assert proj_query("t_flat_setting") == baseline


# This test checks that a nested part manually relocated to the flat sibling name is accepted by the
# server on load, so an operator can migrate a part's layout out of band.
# Scenario:
# - create table with a nested projection, capture SELECT baseline
# - stop the server and move <part>/p.proj to the flat sibling name
# - start the server
# - assert structure: only the flat sibling exists
# - assert the projection is active and SELECT matches baseline
def test_layout_flat_after_manual_relocation():
    # create table with a nested projection, capture baseline
    setup_table("t_flat")
    p = part_dir("t_flat")
    baseline = proj_query("t_flat")

    # move the nested dir to the flat sibling name while the server is down
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {p}/p.proj {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_flat")

    # assert structure: only the flat sibling exists, and the projection is usable
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_parts("t_flat") == "1"
    assert int(active_projection_parts("t_flat")) >= 1
    assert proj_query("t_flat") == baseline


# This test checks that the flat layout is applied to COMPACT parts too, not only WIDE ones: a
# part-type gate would silently keep the projection nested on compact parts.
# Scenario:
# - create table with a 'flat' projection and a large min_bytes_for_wide_part (forces a compact part)
# - assert the part is compact
# - assert structure: the projection is a flat sibling, not nested
# - assert SELECT through the projection
def test_layout_flat_compact_part():
    # create table whose threshold forces a compact part
    node.query("DROP TABLE IF EXISTS t_compact SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_compact (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 1000000000, projection_storage_format = 'flat'"""
    )
    node.query(
        "INSERT INTO t_compact SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # assert the part is compact
    part_type = node.query(
        "SELECT part_type FROM system.parts WHERE table = 't_compact' AND active = 1"
    ).strip()
    assert part_type == "Compact", part_type

    # assert structure: flat sibling, not nested
    p = part_dir("t_compact")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")

    # assert SELECT through the projection
    assert (
        proj_query("t_compact", extra_settings="force_optimize_projection = 1")
        == "100\t4950"
    )


# This test checks that switching projection_storage_format to 'flat' rewrites an existing nested
# part on the next merge: the setting must govern newly written parts, the source layout must not stick.
# Scenario:
# - create table with a default (nested) projection, insert two parts
# - assert structure: nested
# - MODIFY SETTING projection_storage_format = 'flat'
# - MERGE (OPTIMIZE TABLE FINAL)
# - assert structure: the merged part is a flat sibling, no nested dir
# - assert SELECT and CHECK TABLE
def test_layout_convert_nested_to_flat():
    # create table with a default (nested) projection, insert two parts
    node.query("DROP TABLE IF EXISTS t_n2f SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_n2f (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key SETTINGS min_bytes_for_wide_part = 0"""
    )
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_n2f SELECT number, number * 2, toString(number) FROM {rng}"
        )

    # assert structure: nested
    assert path_exists(f"{part_dir('t_n2f')}/p.proj")

    # switch the layout to 'flat'
    node.query("ALTER TABLE t_n2f MODIFY SETTING projection_storage_format = 'flat'")

    # merge rewrites the part in the current (flat) layout
    node.query("SYSTEM START MERGES t_n2f")
    node.query("OPTIMIZE TABLE t_n2f FINAL")

    # assert structure: merged part is a flat sibling, no nested dir
    p = part_dir("t_n2f")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")

    # assert SELECT and CHECK TABLE
    assert active_parts("t_n2f") == "1"
    assert broken_projection_parts("t_n2f") == "0"
    assert proj_query("t_n2f", extra_settings="force_optimize_projection = 1") == "100\t4950"
    assert check_table("t_n2f") == "1"


# This test checks the reverse (downgrade) conversion: switching back to 'legacy_nested' and merging
# must rewrite a flat part into the nested layout, leaving no flat sibling behind.
# Scenario:
# - create table with a 'flat' projection, insert two parts
# - assert structure: flat siblings
# - MODIFY SETTING projection_storage_format = 'legacy_nested'
# - MERGE (OPTIMIZE TABLE FINAL)
# - assert structure: the merged part is nested, no flat sibling
# - assert SELECT and CHECK TABLE
def test_layout_convert_flat_to_nested():
    # create table with a 'flat' projection, insert two parts
    node.query("DROP TABLE IF EXISTS t_f2n SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_f2n (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_f2n SELECT number, number * 2, toString(number) FROM {rng}"
        )

    # assert structure: flat siblings
    assert path_exists(f"{part_dir('t_f2n')}.p.proj")

    # switch the layout back to nested
    node.query(
        "ALTER TABLE t_f2n MODIFY SETTING projection_storage_format = 'legacy_nested'"
    )

    # merge rewrites the part in the current (nested) layout
    node.query("SYSTEM START MERGES t_f2n")
    node.query("OPTIMIZE TABLE t_f2n FINAL")

    # assert structure: merged part is nested, no flat sibling
    p = part_dir("t_f2n")
    assert path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}.p.proj")

    # assert SELECT and CHECK TABLE
    assert active_parts("t_f2n") == "1"
    assert broken_projection_parts("t_f2n") == "0"
    assert proj_query("t_f2n", extra_settings="force_optimize_projection = 1") == "100\t4950"
    assert check_table("t_f2n") == "1"


# ==============================================================================
# B. Lifecycle operations carry the flat sibling
# ==============================================================================

# This test checks that a replicated fetch materializes the projection in the flat layout on the
# receiving replica, instead of a nested or missing sibling. (Issue #5)
# Scenario:
# - create the replicated 'flat' table on both replicas
# - insert on replica 1, SYNC replica 2 (replica 2 fetches the part)
# - assert structure: the fetched part carries a flat sibling
# - assert SELECT matches across replicas
# @pytest.mark.xfail(reason=REVIEW + "3473479560", strict=False)
def test_carry_replicated_fetch():
    # create the replicated 'flat' table on both replicas
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_repl SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_repl (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_repl', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )

    # insert on replica 1, sync replica 2 (which fetches the part)
    node.query(
        "INSERT INTO t_repl SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_repl")

    # assert structure: the fetched part carries a flat sibling, and SELECT matches across replicas
    p = part_dir("t_repl", node2)
    assert path_exists(f"{p}.p.proj", node2)
    assert proj_query("t_repl", node2) == proj_query("t_repl", node)


# This test checks that a merge on a ReplicatedMergeTree writes the flat layout on each replica
# (merges execute locally): a replica must not end up with a nested or missing sibling.
# Scenario:
# - create the replicated 'flat' table on both replicas
# - insert two parts on replica 1, SYNC replica 2
# - MERGE (OPTIMIZE TABLE FINAL), SYNC replica 2
# - assert structure: both replicas expose a single flat-sibling part
# - assert SELECT matches across replicas
def test_carry_replicated_merge():
    # create the replicated 'flat' table on both replicas
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_rmerge SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_rmerge (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_rmerge', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )

    # insert two parts on replica 1, sync replica 2
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_rmerge SELECT number, number * 2, toString(number) FROM {rng}"
        )
    node2.query("SYSTEM SYNC REPLICA t_rmerge")

    # merge: each replica lands the merged part in its own flat layout
    for n in (node, node2):
        n.query("SYSTEM START MERGES t_rmerge")
    node.query("OPTIMIZE TABLE t_rmerge FINAL")
    node2.query("SYSTEM SYNC REPLICA t_rmerge")

    # assert structure: both replicas expose one flat-sibling part
    for n in (node, node2):
        wait_for(lambda n=n: active_parts("t_rmerge", n) == "1")
        p = part_dir("t_rmerge", n)
        assert path_exists(f"{p}.p.proj", n)
        assert not path_exists(f"{p}/p.proj", n)
        assert broken_projection_parts("t_rmerge", n) == "0"

    # assert SELECT matches across replicas
    assert proj_query("t_rmerge", node2) == proj_query("t_rmerge", node)


# This test checks that ATTACH PARTITION FROM (clonePart) copies the flat projection sibling into
# the destination table, instead of dropping it. (Issue #2)
# Scenario:
# - create 'flat' source and destination tables
# - TRUNCATE the destination, then ATTACH PARTITION FROM the source
# - assert structure: the destination part carries a flat sibling
# - assert SELECT matches the source
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_carry_attach_partition_from():
    # create 'flat' source and destination tables
    setup_table("t_src", "projection_storage_format = 'flat'")
    setup_table("t_dst", "projection_storage_format = 'flat'")

    # clear the destination and attach the source partition into it
    node.query("TRUNCATE TABLE t_dst")
    node.query("ALTER TABLE t_dst ATTACH PARTITION tuple() FROM t_src")

    # assert structure: the destination part carries a flat sibling, SELECT matches the source
    p = part_dir("t_dst")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_dst") == proj_query("t_src")


# This test checks that DETACH/ATTACH PART moves the flat sibling with the part on disk and in
# memory: after ATTACH the projection part's root must point at the attached location, not detached/.
# Scenario:
# - create table with a 'flat' projection, capture SELECT baseline
# - DETACH PART, then ATTACH PART
# - assert structure: the flat sibling is at the attached location and nothing is left under detached/
# - assert the projection is healthy and SELECT matches baseline
# @pytest.mark.xfail(reason=REVIEW + "3473543140", strict=False)
def test_carry_detach_attach_part():
    # create table with a 'flat' projection, capture baseline
    setup_table("t_da", "projection_storage_format = 'flat'")
    baseline = proj_query("t_da")
    name = part_name("t_da")

    # DETACH then ATTACH the part
    node.query(f"ALTER TABLE t_da DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_da ATTACH PART '{name}'")

    # assert structure: sibling at the attached location, nothing left under detached/
    p = part_dir("t_da")
    table_root = p.rsplit("/", 1)[0]
    assert path_exists(f"{p}.p.proj")
    leftover = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftover == "0"

    # fail closed: the attached part must serve the projection from its new location
    assert broken_projection_parts("t_da") == "0"
    assert (
        proj_query("t_da", extra_settings="force_optimize_projection = 1") == baseline
    )


# This test checks that DETACH/ATTACH PART never leaves a mixed on-disk state: a live-named parent
# without its sibling, or a sibling stranded under detached/.
# Scenario:
# - create table with a 'flat' projection, capture SELECT baseline
# - DETACH PART; assert nothing of the part stays at live names, both parent and sibling under detached/
# - ATTACH PART; assert parent and sibling live again, nothing left under detached/
# - assert the projection is healthy and SELECT matches baseline
def test_carry_detach_attach_no_mixed_state():
    # create table with a 'flat' projection, capture baseline
    setup_table("t_mix", "projection_storage_format = 'flat'")
    baseline = proj_query("t_mix")
    live = part_dir("t_mix")
    name = part_name("t_mix")
    table_root = live.rsplit("/", 1)[0]

    # DETACH: nothing stays at live names, parent + sibling both under detached/
    node.query(f"ALTER TABLE t_mix DETACH PART '{name}'")
    assert not path_exists(live)
    assert not path_exists(f"{live}.p.proj")
    assert path_exists(f"{table_root}/detached/{name}")
    assert path_exists(f"{table_root}/detached/{name}.p.proj")

    # ATTACH: parent + sibling live again, nothing left under detached/
    node.query(f"ALTER TABLE t_mix ATTACH PART '{name}'")
    p = part_dir("t_mix")
    assert path_exists(f"{p}.p.proj")
    leftover = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftover == "0"

    # assert the projection is healthy and SELECT matches baseline
    assert broken_projection_parts("t_mix") == "0"
    assert (
        proj_query("t_mix", extra_settings="force_optimize_projection = 1") == baseline
    )


# This test checks that a part loaded from disk after a restart keeps its flat layout for later
# operations: a reloaded part must still DETACH/ATTACH as flat. (Issue #3)
# Scenario:
# - create table with a 'flat' projection, capture SELECT baseline
# - restart the server (the part is reloaded from disk)
# - DETACH PART, then ATTACH PART
# - assert structure: the flat sibling is present and SELECT matches baseline
# @pytest.mark.xfail(reason=REVIEW + "3472535414", strict=False)
def test_carry_after_restart_detach_attach():
    # create table with a 'flat' projection, capture baseline
    setup_table("t_reload", "projection_storage_format = 'flat'")
    baseline = proj_query("t_reload")

    # restart, then round-trip the reloaded part through DETACH/ATTACH
    node.restart_clickhouse()
    block_until_tables_loaded("t_reload")
    name = part_name("t_reload")
    node.query(f"ALTER TABLE t_reload DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_reload ATTACH PART '{name}'")

    # assert structure: flat sibling present, SELECT matches baseline
    p = part_dir("t_reload")
    assert path_exists(f"{p}.p.proj")
    assert proj_query("t_reload") == baseline


# This test checks that after DETACH PART, system.detached_parts shows exactly one entry (no junk
# row for the sibling) whose bytes_on_disk includes the sibling, and DROP DETACHED PART removes both.
# Scenario:
# - create table with a 'flat' projection, DETACH PART
# - assert the surface: exactly one detached entry, bytes_on_disk covers parent + sibling
# - DROP DETACHED PART
# - assert both parent and sibling are gone and nothing is left under detached/
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019447
def test_carry_detached_parts_surface():
    # create table with a 'flat' projection, DETACH PART
    setup_table("t_det_surf", "projection_storage_format = 'flat'")
    name = part_name("t_det_surf")
    table_root = part_dir("t_det_surf").rsplit("/", 1)[0]
    node.query(f"ALTER TABLE t_det_surf DETACH PART '{name}'")

    # assert the surface: exactly one detached entry, no row for the sibling
    rows = node.query(
        "SELECT name FROM system.detached_parts WHERE table = 't_det_surf'"
    ).strip()
    assert rows == name  # exactly one entry, no row for the sibling
    bytes_on_disk = int(
        node.query(
            f"SELECT bytes_on_disk FROM system.detached_parts WHERE table = 't_det_surf' AND name = '{name}'"
        ).strip()
    )

    def files_size(path):
        return int(
            node.exec_in_container(
                [
                    "bash",
                    "-c",
                    f"find {path} -type f -printf '%s\\n' | awk '{{s+=$1}} END {{print s+0}}'",
                ],
                privileged=True,
                user="root",
            ).strip()
        )

    # assert bytes_on_disk covers the parent + the sibling
    parent_size = files_size(f"{table_root}/detached/{name}")
    sibling_size = files_size(f"{table_root}/detached/{name}.p.proj")
    assert sibling_size > 0
    assert bytes_on_disk >= parent_size + sibling_size

    # DROP DETACHED PART removes parent and sibling, nothing left under detached/
    node.query(
        f"ALTER TABLE t_det_surf DROP DETACHED PART '{name}' SETTINGS allow_drop_detached = 1"
    )
    assert not path_exists(f"{table_root}/detached/{name}")
    assert not path_exists(f"{table_root}/detached/{name}.p.proj")
    leftovers = node.exec_in_container(
        ["bash", "-c", f"find {table_root}/detached -maxdepth 1 -name '*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"


# This test checks that MATERIALIZE PROJECTION inside a mutation finalizes the winner tmp_proj into
# a flat sibling, leaves no tmp residue, and survives the part's final rename.
# Scenario:
# - create a 'flat' table without a projection, insert data
# - ADD PROJECTION, then MATERIALIZE PROJECTION (mutation)
# - assert structure: a flat sibling, no nested dir, no *.tmp_proj residue
# - assert CHECK TABLE and SELECT survive a restart
def test_carry_materialize_projection():
    # create a 'flat' table without a projection, insert data
    node.query("DROP TABLE IF EXISTS t_mat SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_mat (key UInt64, id UInt64, value String)
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    node.query(
        "INSERT INTO t_mat SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # add and materialize the projection
    node.query("ALTER TABLE t_mat ADD PROJECTION p (SELECT key, id, value ORDER BY id)")
    node.query(
        "ALTER TABLE t_mat MATERIALIZE PROJECTION p SETTINGS mutations_sync = 2"
    )

    # assert structure: flat sibling, no nested dir, no tmp residue
    p = part_dir("t_mat")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    tmp_residue = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/data/default/t_mat -maxdepth 1 -name '*.tmp_proj' | wc -l",
        ],
        privileged=True,
        user="root",
    ).strip()
    assert tmp_residue == "0"
    assert check_table("t_mat") == "1"

    # assert SELECT survives a restart
    baseline = proj_query("t_mat")
    node.restart_clickhouse()
    assert proj_query("t_mat") == baseline
    assert int(active_projection_parts("t_mat")) >= 1


# This test checks that a lightweight DELETE in 'rebuild' projection mode re-materializes the
# projection in the flat layout: the rebuilt part must carry a flat sibling, not a nested dir.
# Scenario:
# - create table with a 'flat' projection and lightweight_mutation_projection_mode = 'rebuild'
# - lightweight DELETE of some rows (mutation rebuilds the projection)
# - assert structure: the new part carries a flat sibling, no nested dir
# - assert SELECT and CHECK TABLE
def test_carry_lightweight_delete_rebuild():
    # create table with a 'flat' projection that rebuilds on lightweight delete
    node.query("DROP TABLE IF EXISTS t_lwd SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_lwd (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               lightweight_mutation_projection_mode = 'rebuild'"""
    )
    node.query(
        "INSERT INTO t_lwd SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # lightweight DELETE rebuilds the projection into the current (flat) layout
    node.query("DELETE FROM t_lwd WHERE key >= 500 SETTINGS mutations_sync = 2")

    # assert structure: the rebuilt part carries a flat sibling, no nested dir
    p = part_dir("t_lwd")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")

    # assert SELECT and CHECK TABLE
    assert node.query("SELECT count() FROM t_lwd").strip() == "500"
    assert broken_projection_parts("t_lwd") == "0"
    assert proj_query("t_lwd", extra_settings="force_optimize_projection = 1") == "100\t4950"
    assert check_table("t_lwd") == "1"


# This test checks that a part with two projections in the flat layout gets one sibling per
# projection (<part>.p.proj and <part>.q.proj) and that both survive a merge.
# Scenario:
# - create a 'flat' table with two projections p and q, insert two parts
# - MERGE (OPTIMIZE TABLE FINAL)
# - assert structure: both flat siblings present, neither nested
# - assert both projection parts are active and CHECK TABLE passes
def test_carry_multiple_projections():
    # create a 'flat' table with two projections, insert two parts
    node.query("DROP TABLE IF EXISTS t_multi SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_multi (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id),
           PROJECTION q (SELECT id, key ORDER BY key))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_multi SELECT number, number * 2, toString(number) FROM {rng}"
        )

    # merge into a single part
    node.query("SYSTEM START MERGES t_multi")
    node.query("OPTIMIZE TABLE t_multi FINAL")

    # assert structure: both flat siblings present, neither nested
    p = part_dir("t_multi")
    assert path_exists(f"{p}.p.proj")
    assert path_exists(f"{p}.q.proj")
    assert not path_exists(f"{p}/p.proj")
    assert not path_exists(f"{p}/q.proj")

    # assert both projection parts are active and CHECK TABLE passes
    assert active_parts("t_multi") == "1"
    assert int(active_projection_parts("t_multi")) >= 2
    assert broken_projection_parts("t_multi") == "0"
    assert check_table("t_multi") == "1"
    assert proj_query("t_multi", extra_settings="force_optimize_projection = 1") == "100\t4950"


# This test checks that a partitioned 'flat' table keeps a per-partition flat sibling through a
# merge: each partition's merged part must have its own sibling and stay healthy.
# Scenario:
# - create a partitioned table with a 'flat' projection, insert several parts across partitions
# - MERGE (OPTIMIZE TABLE FINAL)
# - assert structure: each active part has a flat sibling, none nested
# - assert SELECT and CHECK TABLE
def test_carry_partitioned_merge():
    # create a partitioned 'flat' table, insert across partitions
    node.query("DROP TABLE IF EXISTS t_part SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_part (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree PARTITION BY key % 2 ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    for rng in ("numbers(1000)", "numbers(1000, 1000)"):
        node.query(
            f"INSERT INTO t_part SELECT number, number * 2, toString(number) FROM {rng}"
        )

    # merge each partition
    node.query("SYSTEM START MERGES t_part")
    node.query("OPTIMIZE TABLE t_part FINAL")

    # assert structure: one part per partition, each with a flat sibling, none nested
    assert active_parts("t_part") == "2"
    paths = node.query(
        "SELECT path FROM system.parts WHERE table = 't_part' AND active = 1"
    ).split()
    for path in paths:
        p = path.rstrip("/")
        assert path_exists(f"{p}.p.proj")
        assert not path_exists(f"{p}/p.proj")

    # assert SELECT and CHECK TABLE
    assert broken_projection_parts("t_part") == "0"
    assert proj_query("t_part", extra_settings="force_optimize_projection = 1") == "100\t4950"
    assert check_table("t_part") == "1"


# This test checks that RENAME TABLE repoints the loaded projection sub-part storages in memory, so
# the projection stays readable from the new table path without a restart. Only an Ordinary database
# moves data on rename (an Atomic database keeps the UUID path), so the table lives in one here.
# Scenario:
# - create an Ordinary database with a 'flat' table and a projection, capture SELECT baseline
# - RENAME TABLE to a new name (the data directory moves on disk)
# - assert structure: the flat sibling exists under the new table path
# - without a restart, assert the projection still serves the query and CHECK TABLE passes
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3595946442
def test_carry_rename_table():
    # create an Ordinary database: Atomic keeps the UUID path, so only Ordinary moves data on rename
    node.query("DROP DATABASE IF EXISTS t_ren_db SYNC")
    node.query(
        "CREATE DATABASE t_ren_db ENGINE = Ordinary",
        settings={"allow_deprecated_database_ordinary": 1},
    )

    # create a 'flat' table with a projection and capture the baseline before the rename
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_ren_db.src (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    node.query(
        "INSERT INTO t_ren_db.src SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    baseline = proj_query("t_ren_db.src", extra_settings="force_optimize_projection = 1")

    # rename the table; the data directory moves on disk
    node.query("RENAME TABLE t_ren_db.src TO t_ren_db.dst")

    # assert structure: the flat sibling moved under the new table path
    p = (
        node.query(
            "SELECT path FROM system.parts WHERE database = 't_ren_db' AND table = 'dst' AND active = 1 LIMIT 1"
        )
        .strip()
        .rstrip("/")
    )
    assert "/dst/" in p
    assert path_exists(f"{p}.p.proj")

    # without a restart, the repointed storage must serve the projection from the new path
    broken = node.query(
        "SELECT count() FROM system.projection_parts WHERE database = 't_ren_db' AND table = 'dst' AND is_broken"
    ).strip()
    assert broken == "0"
    assert (
        proj_query("t_ren_db.dst", extra_settings="force_optimize_projection = 1")
        == baseline
    )
    assert check_table("t_ren_db.dst") == "1"


# ==============================================================================
# C. Sibling cleanup on removal
# ==============================================================================

# This test checks that outdated-part cleanup removes the flat projection sibling too, instead of
# leaving it stranded after the part is dropped. (Issue #1)
# Scenario:
# - create table with a 'flat' projection
# - DROP PART
# - assert the flat sibling is removed
# @pytest.mark.xfail(reason=REVIEW + "3472535408", strict=False)
def test_cleanup_drop_part_removes_sibling():
    # create table with a 'flat' projection
    setup_table("t_rm", "projection_storage_format = 'flat'")
    p = part_dir("t_rm")
    assert path_exists(f"{p}.p.proj")

    # drop the part; the flat sibling must be removed with it
    node.query(f"ALTER TABLE t_rm DROP PART '{part_name('t_rm')}'")
    wait_for(lambda: not path_exists(f"{p}.p.proj"))
    assert not path_exists(f"{p}.p.proj")


# This test checks that DROP PROJECTION on a live 'flat' table removes the flat sibling from the
# rewritten parts instead of leaving an orphan dir behind.
# Scenario:
# - create table with a 'flat' projection
# - assert structure: the flat sibling exists
# - DROP PROJECTION p (mutation)
# - assert structure: no flat sibling on the new part and no active projection part
# - assert the base data reads and CHECK TABLE passes
def test_cleanup_drop_projection_removes_sibling():
    # create table with a 'flat' projection
    setup_table("t_drop_proj", "projection_storage_format = 'flat'")

    # assert structure: the flat sibling exists
    p = part_dir("t_drop_proj")
    assert path_exists(f"{p}.p.proj")

    # DROP PROJECTION rewrites the affected parts
    node.query("ALTER TABLE t_drop_proj DROP PROJECTION p SETTINGS mutations_sync = 2")

    # assert structure: no flat sibling on the new part, no active projection part
    new_p = part_dir("t_drop_proj")
    assert not path_exists(f"{new_p}.p.proj")
    assert not path_exists(f"{new_p}/p.proj")
    assert active_projection_parts("t_drop_proj") == "0"

    # assert the base data reads and CHECK TABLE passes
    assert node.query("SELECT count() FROM t_drop_proj").strip() == "1000"
    assert check_table("t_drop_proj") == "1"


# This test checks that a leftover delete_tmp_ pair from an interrupted removal does not block a new
# removal of the same part name, and that the flat sibling is cleaned with it.
# Scenario:
# - create table with a 'flat' projection
# - plant a stale delete_tmp_<part> pair (parent + sibling)
# - DROP PART
# - assert the part, its sibling, and both delete_tmp_ leftovers are gone
def test_cleanup_delete_tmp_leftovers():
    # create table with a 'flat' projection
    setup_table("t_dtmp", "projection_storage_format = 'flat'")
    name = part_name("t_dtmp")
    root = part_dir("t_dtmp").rsplit("/", 1)[0]

    # plant a stale delete_tmp_ pair, then drop the part
    plant_stale_tmp_dir(f"{root}/delete_tmp_{name}")
    node.query(f"ALTER TABLE t_dtmp DROP PART '{name}'")

    # assert the part, its sibling, and both delete_tmp_ leftovers are gone
    wait_for(lambda: not path_exists(f"{root}/{name}"))
    wait_for(lambda: not path_exists(f"{root}/{name}.p.proj"))
    wait_for(lambda: not path_exists(f"{root}/delete_tmp_{name}"))
    wait_for(lambda: not path_exists(f"{root}/delete_tmp_{name}.p.proj"))
    assert not path_exists(f"{root}/delete_tmp_{name}.p.proj")


# This test checks that DROP PART of a part whose flat sibling has vanished still removes the part
# completely, instead of aborting the cleanup halfway.
# Scenario:
# - create table with a 'flat' projection
# - stop the server and delete the flat sibling, then start it
# - assert the base data still reads
# - DROP PART
# - assert the part is gone and no delete_tmp_ leftover remains
def test_cleanup_tolerates_missing_sibling():
    # create table with a 'flat' projection
    setup_table("t_nosib", "projection_storage_format = 'flat'")
    p = part_dir("t_nosib")
    name = part_name("t_nosib")
    root = p.rsplit("/", 1)[0]

    # delete the flat sibling out of band while the server is down
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"rm -rf {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    assert node.query("SELECT count() FROM t_nosib").strip() == "1000"

    # drop the part; removal must complete despite the missing sibling
    node.query(f"ALTER TABLE t_nosib DROP PART '{name}'")
    wait_for(lambda: not path_exists(p))
    assert not path_exists(p)
    leftovers = node.exec_in_container(
        ["bash", "-c", f"find {root} -maxdepth 1 -name 'delete_tmp_*' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"


# ==============================================================================
# D. Stale residue and destination-clearing
# ==============================================================================

# This test checks that when an insert reuses a failed insert's tmp dir, collision cleanup wipes the
# stale flat sibling too; else the fresh projection is written into the leftover and published live.
# Scenario:
# - create a 'flat' table
# - plant a stale tmp_insert_<part> pair at the name the first insert will reuse
# - INSERT (the collision branch runs)
# - assert neither the part nor its projection adopted stale files, and no stale sibling is left
# - assert the projection is healthy, SELECT and CHECK TABLE pass
# @pytest.mark.xfail(reason=REVIEW + "3544856348", strict=False)
def test_residue_insert_tmp_collision():
    # create a 'flat' table
    node.query("DROP TABLE IF EXISTS t_ins SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_ins (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )

    # plant a stale tmp dir at the name the first insert reuses (tmp_insert_all_1_1_0)
    stale = f"{table_path('t_ins')}/tmp_insert_all_1_1_0"
    plant_stale_tmp_dir(stale)
    node.query(
        "INSERT INTO t_ins SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # assert the collision branch ran and nothing adopted the stale files
    p = part_dir("t_ins")
    assert p.endswith("all_1_1_0")  # the collision branch really ran
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/stale_marker.txt")
    assert not path_exists(f"{p}.p.proj/stale_marker.txt")
    assert not path_exists(f"{stale}.p.proj")

    # assert the projection is healthy, SELECT and CHECK TABLE pass
    assert broken_projection_parts("t_ins") == "0"
    assert (
        proj_query("t_ins", extra_settings="force_optimize_projection = 1")
        == "100\t4950"
    )
    assert check_table("t_ins") == "1"


# This test checks that a retried fetch (removeSharedRecursive of the previous tmp-fetch_ dir) wipes
# the stale flat sibling with it; else the retried download mixes stale files into the fetched part.
# Scenario:
# - create the replicated 'flat' table on both replicas, stop fetches on replica 2
# - insert on replica 1, plant a stale tmp-fetch_<part> pair on replica 2
# - start fetches and SYNC replica 2
# - assert the fetched part adopted no stale files and no stale sibling is left
# - assert the projection is healthy, SELECT and CHECK TABLE pass
# @pytest.mark.xfail(reason=REVIEW + "3534142472", strict=False)
def test_residue_fetch_tmp_collision():
    # create the replicated 'flat' table on both replicas
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_fetch SYNC")
        n.query("SYSTEM STOP MERGES")
        n.query(
            f"""CREATE TABLE t_fetch (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id, value ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_fetch', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
        )

    # insert on replica 1, plant a stale tmp-fetch_ pair on replica 2
    node2.query("SYSTEM STOP FETCHES t_fetch")
    node.query(
        "INSERT INTO t_fetch SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    name = part_name("t_fetch", node)
    stale = f"{table_path('t_fetch', node2)}/tmp-fetch_{name}"
    plant_stale_tmp_dir(stale, node2)

    # start fetches and sync replica 2
    node2.query("SYSTEM START FETCHES t_fetch")
    node2.query("SYSTEM SYNC REPLICA t_fetch")

    # assert the fetched part adopted no stale files and no stale sibling is left
    p2 = part_dir("t_fetch", node2)
    assert path_exists(f"{p2}.p.proj", node2)
    assert not path_exists(f"{p2}/stale_marker.txt", node2)
    assert not path_exists(f"{p2}.p.proj/stale_marker.txt", node2)
    assert not path_exists(f"{stale}.p.proj", node2)

    # assert the projection is healthy, SELECT and CHECK TABLE pass
    assert broken_projection_parts("t_fetch", node2) == "0"
    assert proj_query(
        "t_fetch", node2, extra_settings="force_optimize_projection = 1"
    ) == proj_query("t_fetch", node)
    assert check_table("t_fetch", node2) == "1"


# This test checks that a stale flat sibling left at a LIVE part name is not adopted by a later part
# reusing that name: publishing the name again removes the leftover.
# Scenario:
# - create a 'flat' table with materialize_projections_on_insert = 0
# - plant a stale sibling at the future live part name
# - INSERT a part WITHOUT a projection at that name
# - restart the server
# - assert no projection part is active, no flat sibling remains, and the data reads
def test_residue_live_not_adopted():
    # create a 'flat' table that does not materialize the projection on insert
    node.query("DROP TABLE IF EXISTS t_adopt SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_adopt (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )

    # plant a stale sibling at the future live name, then insert a projection-less part there
    plant_stale_live_sibling(f"{table_path('t_adopt')}/all_1_1_0.p.proj")
    node.query(
        "INSERT INTO t_adopt SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    p = part_dir("t_adopt")
    assert p.endswith("all_1_1_0")  # the name collision really happened

    # restart; the part was written without the projection, so nothing may serve one
    node.restart_clickhouse()
    block_until_tables_loaded("t_adopt")
    assert active_projection_parts("t_adopt") == "0"
    assert not path_exists(f"{p}.p.proj")
    assert node.query("SELECT count() FROM t_adopt").strip() == "1000"
    assert check_table("t_adopt") == "1"


# This test checks that publishing a part WITH a projection over a stale sibling at the destination
# name clears the leftover instead of failing the sibling rename.
# Scenario:
# - create a 'flat' table
# - plant a stale sibling at the future live part name
# - INSERT a part WITH a projection at that name
# - assert the real sibling replaced the stale one and CHECK TABLE passes
def test_residue_live_replaced_by_real():
    # create a 'flat' table
    node.query("DROP TABLE IF EXISTS t_repl_sib SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_repl_sib (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )

    # plant a stale sibling at the future live name, then insert a real projection there
    plant_stale_live_sibling(f"{table_path('t_repl_sib')}/all_1_1_0.p.proj")
    node.query(
        "INSERT INTO t_repl_sib SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # assert the real sibling replaced the stale one and CHECK TABLE passes
    p = part_dir("t_repl_sib")
    assert p.endswith("all_1_1_0")
    assert path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}.p.proj/stale_marker.txt")
    assert (
        proj_query("t_repl_sib", extra_settings="force_optimize_projection = 1")
        == "100\t4950"
    )
    assert check_table("t_repl_sib") == "1"


# This test checks destination-clearing in the detached namespace: a stale sibling under detached/
# must not fail DETACH of a real part carrying the same name.
# Scenario:
# - create table with a 'flat' projection, capture SELECT baseline
# - plant a stale sibling under detached/<part>
# - DETACH PART (over the stale sibling), then ATTACH PART
# - assert the stale marker is gone, the sibling is present, and SELECT matches baseline
def test_residue_detached_cleared_on_detach():
    # create table with a 'flat' projection, capture baseline
    setup_table("t_det_sib", "projection_storage_format = 'flat'")
    baseline = proj_query("t_det_sib")
    name = part_name("t_det_sib")
    table_root = part_dir("t_det_sib").rsplit("/", 1)[0]

    # plant a stale sibling under detached/, then DETACH the real part over it
    plant_stale_live_sibling(f"{table_root}/detached/{name}.p.proj")
    node.query(f"ALTER TABLE t_det_sib DETACH PART '{name}'")
    assert path_exists(f"{table_root}/detached/{name}.p.proj")
    assert not path_exists(f"{table_root}/detached/{name}.p.proj/stale_marker.txt")

    # ATTACH the part back and verify the projection serves from its new location
    node.query(f"ALTER TABLE t_det_sib ATTACH PART '{name}'")
    p = part_dir("t_det_sib")
    assert path_exists(f"{p}.p.proj")
    assert broken_projection_parts("t_det_sib") == "0"
    assert (
        proj_query("t_det_sib", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# This test checks the publish crash window (tmp -> live): a crash after the sibling moved but before
# the parent moved leaves a live-named sibling and a tmp-named parent; startup must clear both.
# Scenario:
# - create table with a 'flat' projection
# - plant a tmp_merge_<part> parent + a live-named <part>.p.proj sibling
# - restart the server
# - assert both planted dirs are gone and the real part is untouched
def test_residue_crash_window_publish():
    # create table with a 'flat' projection
    setup_table("t_cw_pub", "projection_storage_format = 'flat'")
    root = table_path("t_cw_pub")

    # plant the half-published state: tmp parent + committed live sibling
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {root}/tmp_merge_all_1_1_1 {root}/all_1_1_1.p.proj"
            f" && touch {root}/tmp_merge_all_1_1_1/stale_marker.txt {root}/all_1_1_1.p.proj/stale_marker.txt"
            f" && chmod -R 777 {root}/tmp_merge_all_1_1_1 {root}/all_1_1_1.p.proj",
        ],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()

    # assert the interrupted publish is rolled back completely: tmp parent and committed sibling gone
    wait_for(lambda: not path_exists(f"{root}/tmp_merge_all_1_1_1"))
    assert not path_exists(f"{root}/tmp_merge_all_1_1_1")
    wait_for(lambda: not path_exists(f"{root}/all_1_1_1.p.proj"))
    assert not path_exists(f"{root}/all_1_1_1.p.proj")

    # assert the real part and its projection are untouched
    assert active_parts("t_cw_pub") == "1"
    assert broken_projection_parts("t_cw_pub") == "0"
    assert check_table("t_cw_pub") == "1"


# This test checks the removal crash window (live -> delete_tmp_): a crash after the parent moved but
# before the sibling moved leaves a delete_tmp_ parent and a live-named orphan sibling; startup clears both.
# Scenario:
# - create table with a 'flat' projection
# - plant a delete_tmp_<part> parent + a live-named <part>.p.proj sibling
# - restart the server
# - assert both planted dirs are gone and the real part is untouched
def test_residue_crash_window_remove():
    # create table with a 'flat' projection
    setup_table("t_cw_rm", "projection_storage_format = 'flat'")
    root = table_path("t_cw_rm")

    # plant the half-removed state: delete_tmp_ parent + live-named orphan sibling
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {root}/delete_tmp_gone_0_0_0 {root}/gone_0_0_0.p.proj"
            f" && touch {root}/delete_tmp_gone_0_0_0/stale_marker.txt {root}/gone_0_0_0.p.proj/stale_marker.txt"
            f" && chmod -R 777 {root}/delete_tmp_gone_0_0_0 {root}/gone_0_0_0.p.proj",
        ],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()

    # assert startup cleared both the delete_tmp_ parent and the orphan sibling
    wait_for(lambda: not path_exists(f"{root}/delete_tmp_gone_0_0_0"))
    assert not path_exists(f"{root}/delete_tmp_gone_0_0_0")
    wait_for(lambda: not path_exists(f"{root}/gone_0_0_0.p.proj"))
    assert not path_exists(f"{root}/gone_0_0_0.p.proj")

    # assert the real part and its projection are untouched
    assert active_parts("t_cw_rm") == "1"
    assert broken_projection_parts("t_cw_rm") == "0"
    assert check_table("t_cw_rm") == "1"


# This test checks that a failed ATTACH (occupied attaching_ destination) rolls whole parts back: the
# detached part and its sibling stay intact and a retry succeeds.
# Scenario:
# - create table with a 'flat' projection, DETACH PARTITION
# - occupy the attaching_<part> destination so the rename fails
# - ATTACH PARTITION and assert it raises, with the detached part + sibling intact
# - free the destination and ATTACH PARTITION again; assert it succeeds
def test_residue_attach_rollback():
    # create table with a 'flat' projection, DETACH the partition
    setup_table("t_rollback", "projection_storage_format = 'flat'")
    name = part_name("t_rollback")
    node.query("ALTER TABLE t_rollback DETACH PARTITION tuple()")
    detached_root = node.query(
        "SELECT path FROM system.detached_parts WHERE table = 't_rollback' LIMIT 1"
    ).strip()
    if not detached_root:
        detached_root = "/var/lib/clickhouse/data/default/t_rollback/detached"
    else:
        detached_root = detached_root.rstrip("/").rsplit("/", 1)[0]
    assert path_exists(f"{detached_root}/{name}.p.proj")

    # occupy the temporary rename destination to make tryRenameAll fail
    node.exec_in_container(
        ["bash", "-c", f"mkdir -p {detached_root}/attaching_{name}"],
        privileged=True,
        user="root",
    )
    assert "Exception" in node.query_and_get_error(
        "ALTER TABLE t_rollback ATTACH PARTITION tuple()"
    )

    # assert nothing was lost or half-renamed
    assert path_exists(f"{detached_root}/{name}")
    assert path_exists(f"{detached_root}/{name}.p.proj")

    # free the destination and retry; the attach now succeeds
    node.exec_in_container(
        ["bash", "-c", f"rmdir {detached_root}/attaching_{name}"],
        privileged=True,
        user="root",
    )
    node.query("ALTER TABLE t_rollback ATTACH PARTITION tuple()")
    assert active_parts("t_rollback") == "1"
    assert proj_query("t_rollback") != ""


# A throw after the first committed sibling move rolls the whole ATTACH rename back: part + BOTH siblings return to detached/<name> and no attaching_ residue remains, exercising the reverse-order unwind test_residue_attach_rollback misses (it dies in the existsDirectory(to) preflight, before any move commits).
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3603799704
def test_flat_projection_sibling_move_rollback():
    # create a 'flat' table with two projections (so a part has two sibling dirs), DETACH the partition
    node.query("DROP TABLE IF EXISTS t_sib_rollback SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_sib_rollback (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id),
           PROJECTION q (SELECT key, value ORDER BY value))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    node.query(
        "INSERT INTO t_sib_rollback SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    name = part_name("t_sib_rollback")
    node.query("ALTER TABLE t_sib_rollback DETACH PARTITION tuple()")
    detached = f"{table_path('t_sib_rollback')}/detached"
    assert path_exists(f"{detached}/{name}")
    assert path_exists(f"{detached}/{name}.p.proj")
    assert path_exists(f"{detached}/{name}.q.proj")

    # ATTACH renames detached/<name> -> detached/attaching_<name>; a throw after the first sibling move forces the partial-rename rollback
    node.query("SYSTEM ENABLE FAILPOINT throw_after_flat_projection_sibling_move")
    try:
        assert "Exception" in node.query_and_get_error(
            "ALTER TABLE t_sib_rollback ATTACH PARTITION tuple()"
        )
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT throw_after_flat_projection_sibling_move"
        )

    # the rollback returned the part and BOTH siblings to detached/<name>, leaving no attaching_ residue
    assert active_parts("t_sib_rollback") == "0"
    assert path_exists(f"{detached}/{name}")
    assert path_exists(f"{detached}/{name}.p.proj")
    assert path_exists(f"{detached}/{name}.q.proj")
    assert not path_exists(f"{detached}/attaching_{name}")
    assert not path_exists(f"{detached}/attaching_{name}.p.proj")
    assert not path_exists(f"{detached}/attaching_{name}.q.proj")

    # free of the failpoint, the attach now succeeds and the data reads
    node.query("ALTER TABLE t_sib_rollback ATTACH PARTITION tuple()")
    assert active_parts("t_sib_rollback") == "1"
    assert node.query("SELECT count() FROM t_sib_rollback").strip() == "1000"


# ==============================================================================
# E. Unowned-sibling filter (residue that a part does not own must not be carried)
# ==============================================================================

# This test checks that MOVE PART (cross-disk clonePart) copies only the part's own projections, so
# an unowned residue sibling is structurally excluded from the moved part.
# Scenario:
# - create a 'flat' table with materialize_projections_on_insert = 0 on a multi-disk policy, insert
# - plant an unowned residue sibling at the source part name
# - MOVE PART to the s3 disk
# - assert the destination has no projection sibling and no active projection part, data reads
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019427
def test_unowned_move_part_skips():
    # create a projection-less-on-insert 'flat' table on a multi-disk policy
    node.query("DROP TABLE IF EXISTS t_move SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_move (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0, storage_policy = 'default_and_s3'"""
    )
    node.query(
        "INSERT INTO t_move SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # plant an unowned residue sibling at the source part name, then move the part
    src = part_dir("t_move")
    name = part_name("t_move")
    plant_stale_live_sibling(f"{src}.p.proj")
    node.query(f"ALTER TABLE t_move MOVE PART '{name}' TO DISK 's3'")

    # assert the destination excluded the unowned residue and the data reads
    dst = part_dir("t_move")
    assert dst != src  # the part really moved
    assert not path_exists(f"{dst}.p.proj")
    assert not path_exists(f"{dst}/p.proj")
    assert active_projection_parts("t_move") == "0"
    assert node.query("SELECT count() FROM t_move").strip() == "1000"


# This test checks that cross-disk ATTACH PARTITION FROM (freezeRemote path) applies the same
# owned-projections filter as the same-disk freeze path, excluding an unowned residue sibling.
# Scenario:
# - create 'flat' source (default disk) and destination (s3) tables, projection-less on insert, insert into source
# - plant an unowned residue sibling at the source part name
# - ATTACH PARTITION FROM the source into the destination
# - assert the destination has no projection sibling and no active projection part, data reads, CHECK passes
# https://github.com/ClickHouse/ClickHouse/pull/108443#discussion_r3569019441
def test_unowned_attach_cross_disk_skips():
    # create 'flat' source (default disk) and destination (s3) tables
    node.query("DROP TABLE IF EXISTS t_att_src SYNC")
    node.query("DROP TABLE IF EXISTS t_att_dst SYNC")
    node.query("SYSTEM STOP MERGES")
    for tname, policy in (("t_att_src", ""), ("t_att_dst", ", storage_policy = 's3'")):
        node.query(
            f"""CREATE TABLE {tname} (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id ORDER BY id))
                ENGINE = MergeTree ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    materialize_projections_on_insert = 0{policy}"""
        )
    node.query(
        "INSERT INTO t_att_src SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # plant an unowned residue sibling at the source part name, then attach cross-disk
    plant_stale_live_sibling(f"{part_dir('t_att_src')}.p.proj")
    node.query("ALTER TABLE t_att_dst ATTACH PARTITION tuple() FROM t_att_src")

    # assert the destination excluded the unowned residue, data reads, CHECK passes
    p = part_dir("t_att_dst")
    assert not path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_projection_parts("t_att_dst") == "0"
    assert node.query("SELECT count() FROM t_att_dst").strip() == "1000"
    assert check_table("t_att_dst") == "1"


# This test checks that the column-subset mutation (which discovers projections from disk) does not
# hardlink a sibling the source part's checksums do not reference into the mutated part.
# Scenario:
# - create a 'flat' table projection-less on insert, insert
# - plant an unowned residue sibling at the source part name
# - mutate a column (UPDATE ... WHERE 1)
# - assert the mutated part has no projection sibling and no active projection part, data reads
def test_unowned_mutation_skips():
    # create a projection-less-on-insert 'flat' table
    node.query("DROP TABLE IF EXISTS t_mut SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_mut (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )
    node.query(
        "INSERT INTO t_mut SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # plant an unowned residue sibling at the source part name, then mutate a column
    src = part_dir("t_mut")
    plant_stale_live_sibling(f"{src}.p.proj")
    node.query(
        "ALTER TABLE t_mut UPDATE value = concat(value, 'x') WHERE 1 SETTINGS mutations_sync = 1"
    )

    # assert the mutated part excluded the unowned residue and the data reads
    p = part_dir("t_mut")
    assert p != src  # the mutation produced a new part
    assert not path_exists(f"{p}.p.proj")
    assert not path_exists(f"{p}/p.proj")
    assert active_projection_parts("t_mut") == "0"
    assert node.query("SELECT count() FROM t_mut").strip() == "1000"


# ==============================================================================
# F. Manifest desync, repair, and adoption policy (trust-and-heal with a row check)
# ==============================================================================

# A projection dir present on disk and declared in metadata but missing from checksums.txt is either
# (L) this part's own dir after a manifest loss, or (F) residue of another same-named part
# generation. NESTED dirs are provably (L) (they live inside the part dir): warn + load, no rewrite.
# FLAT dirs are ambiguous: the row check against the parent decides - a pass heals the checksums
# record in place, a failure marks the projection broken.


def _make_desync_pair(
    prefix,
    extra="",
    donor_rows=1000,
    donor_offset=0,
    projection="p (SELECT key, id ORDER BY id)",
    donor_projection=None,
    donor_id_type="UInt64",
):
    """Victim table whose part has NO projection dir/record + a donor providing a real dir.
    The donor knobs shape the planted dir: row count/values, projection definition, column type."""
    donor_projection = donor_projection or projection
    node.query(f"DROP TABLE IF EXISTS {prefix} SYNC")
    node.query(f"DROP TABLE IF EXISTS {prefix}_donor SYNC")
    node.query("SYSTEM STOP MERGES")
    settings = "min_bytes_for_wide_part = 0"
    if extra:
        settings += ", " + extra
    node.query(
        f"""CREATE TABLE {prefix} (key UInt64, id UInt64, value String,
           PROJECTION {projection})
           ENGINE = MergeTree ORDER BY key
           SETTINGS {settings}, materialize_projections_on_insert = 0"""
    )
    node.query(
        f"""CREATE TABLE {prefix}_donor (key UInt64, id {donor_id_type}, value String,
           PROJECTION {donor_projection})
           ENGINE = MergeTree ORDER BY key
           SETTINGS {settings}"""
    )
    node.query(
        f"INSERT INTO {prefix} SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node.query(
        f"INSERT INTO {prefix}_donor SELECT number, number * 2, toString(number) FROM numbers({donor_offset}, {donor_rows})"
    )


def _manifest_mentions_projection(part_path):
    return (
        node.exec_in_container(
            ["bash", "-c", f"grep -aco 'p\\.proj' {part_path}/checksums.txt || true"],
            privileged=True,
            user="root",
        ).strip()
        != "0"
    )


# A projection dir present on disk but absent from checksums.txt (checksums is the commit point) is not
# adopted: loadProjections loads only what the manifest lists. The dir is ignored, data reads from the base part.
# Scenario:
# - create a 'flat' table, then a projection-less-on-insert twin, insert into both
# - stop the server and copy the source's flat sibling onto the twin, then start it
# - assert the twin's data reads, the unlisted dir is not adopted (not active, not in the manifest, not broken)
def test_desync_unlisted_dir_ignored():
    # create a 'flat' source table and a projection-less-on-insert twin
    setup_table("t_warn_src", "projection_storage_format = 'flat'")
    node.query("DROP TABLE IF EXISTS t_warn SYNC")
    node.query(
        """CREATE TABLE t_warn (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id, value ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               materialize_projections_on_insert = 0"""
    )
    node.query(
        "INSERT INTO t_warn SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # copy the source sibling onto the twin (an unlisted dir), then reload
    src_sib = f"{part_dir('t_warn_src')}.p.proj"
    dst_sib = f"{part_dir('t_warn')}.p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"cp -r {src_sib} {dst_sib} && chmod -R 777 {dst_sib}"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_warn")

    # assert the data reads and the unlisted dir is ignored, not adopted
    assert node.query("SELECT count() FROM t_warn").strip() == "1000"
    assert active_projection_parts("t_warn") == "0"
    assert broken_projection_parts("t_warn") == "0"
    assert not _manifest_mentions_projection(part_dir("t_warn"))


# This test checks that regenerating a lost manifest restores projection records: without the fix
# checkDataPart folds them only from the (empty-during-repair) projection map and drops every projection.
# Scenario (nested and flat):
# - create table with a projection, capture SELECT baseline
# - stop the server, delete checksums.txt, start it (manifest is regenerated)
# - assert the data reads, the projection is healthy, SELECT matches, CHECK TABLE passes
# - restart and assert the regenerated manifest still references the projection
def test_desync_repair_regenerates_records():
    for tname, extra in (
        ("t_fix_nested", ""),
        ("t_fix_flat", "projection_storage_format = 'flat'"),
    ):
        # create table with a projection, capture baseline
        setup_table(tname, extra)
        baseline = proj_query(tname)
        p = part_dir(tname)

        # delete checksums.txt so the manifest is regenerated on load
        node.stop_clickhouse()
        node.exec_in_container(
            ["bash", "-c", f"rm {p}/checksums.txt"], privileged=True, user="root"
        )
        node.start_clickhouse()

        # assert the data reads, the projection is healthy and served, CHECK passes
        assert node.query(f"SELECT count() FROM {tname}").strip() == "1000", tname
        assert broken_projection_parts(tname) == "0", tname
        assert (
            proj_query(tname, extra_settings="force_optimize_projection = 1")
            == baseline
        ), tname
        assert check_table(tname) == "1", tname

        # assert the regenerated manifest references the projection: it must survive a reload
        node.restart_clickhouse()
        block_until_tables_loaded(tname)
        assert active_projection_parts(tname) == "1", tname
        assert check_table(tname) == "1", tname


# This test checks that regenerating a lost checksums.txt restores records only for metadata-declared
# projections; an undeclared projection dir must not be legitimized by the regenerated manifest.
# Scenario:
# - create a 'flat' table, capture SELECT baseline
# - stop the server, copy the sibling to an undeclared q.proj name, delete checksums.txt, start it
# - assert the "Not restoring ... q.proj" message is logged
# - assert the regenerated manifest references p.proj but not q.proj, projection healthy, SELECT matches
def test_desync_repair_skips_undeclared_dir():
    # create a 'flat' table, capture baseline
    setup_table("t_undecl", "projection_storage_format = 'flat'")
    baseline = proj_query("t_undecl")
    p = part_dir("t_undecl")

    # add an undeclared sibling (q.proj) and drop checksums.txt so the manifest is regenerated
    node.stop_clickhouse()
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"cp -r {p}.p.proj {p}.q.proj && chmod -R 777 {p}.q.proj && rm {p}/checksums.txt",
        ],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_undecl")

    # assert the undeclared dir was refused and only the declared projection is in the regenerated manifest
    assert node.query("SELECT count() FROM t_undecl").strip() == "1000"
    manifest = node.exec_in_container(
        ["bash", "-c", f"grep -ao '[pq]\\.proj' {p}/checksums.txt | sort -u"],
        privileged=True,
        user="root",
    )
    assert "p.proj" in manifest
    assert "q.proj" not in manifest

    # assert the projection is healthy and SELECT matches baseline
    assert broken_projection_parts("t_undecl") == "0"
    assert (
        proj_query("t_undecl", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# A NESTED projection dir planted inside a part but absent from checksums.txt is not adopted either:
# checksums is the commit point for both layouts. The dir is ignored, the part reads from the base.
# Scenario:
# - build a desync pair whose victim has no projection record
# - stop the server, copy the donor's NESTED dir into the victim part, start it
# - assert the projection is not adopted (not active, not broken, not in the manifest) and data reads
def test_desync_nested_unlisted_ignored():
    # build a desync pair, then plant the donor's nested dir inside the victim part
    _make_desync_pair("t_ln")
    p = part_dir("t_ln")
    donor_proj = f"{part_dir('t_ln_donor')}/p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"cp -r {donor_proj} {p}/p.proj && chmod -R 777 {p}/p.proj"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_ln")

    # assert the unlisted nested dir is ignored, not adopted
    assert node.query("SELECT count() FROM t_ln").strip() == "1000"
    assert active_projection_parts("t_ln") == "0"
    assert broken_projection_parts("t_ln") == "0"
    assert not _manifest_mentions_projection(p)


# A foreign FLAT sibling planted next to a part but absent from checksums.txt is not adopted, whatever its
# shape - fail-close collapses the old heal/reject/tolerate branches into one rule: not listed, not loaded.
# Scenario:
# - build a FLAT desync pair, move the donor's flat sibling onto the victim
# - assert the sibling is ignored (not active, not broken, not in the manifest) and the data reads
def test_desync_flat_unlisted_ignored():
    # build a FLAT desync pair, plant the donor's flat sibling on the victim
    _make_desync_pair("t_flat_ign", "projection_storage_format = 'flat'")
    p = part_dir("t_flat_ign")
    donor_sib = f"{part_dir('t_flat_ign_donor')}.p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {donor_sib} {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_flat_ign")

    # assert the unlisted sibling is ignored, not adopted
    assert node.query("SELECT count() FROM t_flat_ign").strip() == "1000"
    assert active_projection_parts("t_flat_ign") == "0"
    assert broken_projection_parts("t_flat_ign") == "0"
    assert not _manifest_mentions_projection(p)


# The #3613153379 scenario: a foreign FLAT sibling of identical definition AND row count but DIFFERENT data
# (another generation of the same-named part) was adopted by the old shape gate and served foreign rows. Under
# fail-close it is not in checksums, so it is never loaded and the query reads correct rows from the base part.
# Scenario:
# - build a FLAT desync pair whose donor holds a different row range of the same shape
# - stop the server, move the donor's flat sibling to the victim, start it
# - assert the sibling is not adopted and the query returns the parent's rows, not the donor's
def test_desync_flat_same_shape_not_adopted():
    # build a FLAT desync pair whose donor holds a different row range of the same shape
    _make_desync_pair(
        "t_hole", "projection_storage_format = 'flat'", donor_offset=1000  # rows 1000..1999
    )
    p = part_dir("t_hole")
    donor_sib = f"{part_dir('t_hole_donor')}.p.proj"
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {donor_sib} {p}.p.proj"], privileged=True, user="root"
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_hole")

    # assert the foreign sibling is not adopted
    assert active_projection_parts("t_hole") == "0"
    assert broken_projection_parts("t_hole") == "0"
    assert not _manifest_mentions_projection(p)

    # assert no foreign rows are served: the base part answers, and enabling projections just falls back to it
    base = node.query(
        "SELECT count() FROM t_hole WHERE id < 200 SETTINGS optimize_use_projections = 0"
    ).strip()
    with_projections = node.query(
        "SELECT count() FROM t_hole WHERE id < 200 SETTINGS optimize_use_projections = 1"
    ).strip()
    assert base == "100"
    assert with_projections == "100"


# Recovery must regenerate a projection whose OWN checksums.txt is also gone: checkDataPart recomputes it from
# data, loadChecksums persists the regenerated projection manifest, and the projection loads healthy and usable.
# Scenario:
# - create a 'flat' table with a materialized projection, capture the projection SELECT baseline
# - stop the server, delete both the part's checksums.txt and the projection's own checksums.txt, start it
# - assert the projection manifest is regenerated on disk, the projection is active/healthy/served, CHECK passes
def test_recovery_flat_recompute_missing_projection_checksums():
    # create a 'flat' table with a materialized projection, capture baseline
    setup_table("t_recomp", "projection_storage_format = 'flat'")
    baseline = proj_query("t_recomp")
    p = part_dir("t_recomp")

    # delete both the part manifest and the projection's own manifest
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"rm {p}/checksums.txt {p}.p.proj/checksums.txt"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()
    block_until_tables_loaded("t_recomp")

    # assert recovery recomputed the projection, wrote its manifest back, and the projection is served
    assert path_exists(f"{p}.p.proj/checksums.txt")
    assert _manifest_mentions_projection(p)
    assert active_projection_parts("t_recomp") == "1"
    assert broken_projection_parts("t_recomp") == "0"
    assert proj_query("t_recomp", extra_settings="force_optimize_projection = 1") == baseline
    assert check_table("t_recomp") == "1"


# ==============================================================================
# G. Reload consistency
# ==============================================================================

# This test checks that reloading a part does not mark a present flat projection broken: the
# consistency check probed a nested dir for the "p.proj" entry, so a flat sibling was reported missing.
# Scenario:
# - create a 'flat' table, capture SELECT baseline, assert no broken projection part
# - restart the server
# - assert still no broken projection part
# - fail closed: the projection must actually be used, SELECT matches baseline
# @pytest.mark.xfail(reason=REVIEW + "3481208077", strict=False)
def test_reload_flat_not_broken():
    # create a 'flat' table, capture baseline
    setup_table("t_consist", "projection_storage_format = 'flat'")
    baseline = proj_query("t_consist")
    assert broken_projection_parts("t_consist") == "0"

    # restart; the reloaded flat projection must not be reported broken
    node.restart_clickhouse()
    block_until_tables_loaded("t_consist")
    assert broken_projection_parts("t_consist") == "0"

    # fail closed: the projection must actually be used, not silently skipped
    assert (
        proj_query("t_consist", extra_settings="force_optimize_projection = 1")
        == baseline
    )


# This test checks that CHECK TABLE classifies an unknown flat projection (left after DROP PROJECTION
# on a detached part) as a projection problem, not a broken part - the nested-only scan misses flat siblings.
# Scenario (nested and flat):
# - create table with a projection, DETACH PART, DROP PROJECTION, ATTACH PART
# - run CHECK TABLE
# - assert the result reports "unexpected projection" and the data is still readable
def test_reload_check_table_dropped_projection():
    for tname, extra in (
        ("t_chk_nested", ""),
        ("t_chk_flat", "projection_storage_format = 'flat'"),
    ):
        # create table with a projection, then drop the projection while the part is detached
        setup_table(tname, extra)
        name = part_name(tname)
        node.query(f"ALTER TABLE {tname} DETACH PART '{name}'")
        node.query(f"ALTER TABLE {tname} DROP PROJECTION p")
        node.query(f"ALTER TABLE {tname} ATTACH PART '{name}'")

        # assert CHECK TABLE flags the unknown projection and the data stays readable
        result = node.query(
            f"CHECK TABLE {tname} SETTINGS check_query_single_value_result = 0"
        )
        assert "unexpected projection" in result, (tname, result)
        assert node.query(f"SELECT count() FROM {tname}").strip() == "1000"


# ==============================================================================
# H. Zero-copy and blob lifecycle on object storage
# ==============================================================================

# This test checks that on zero-copy storage a mutation keeps blobs hardlinked by flat projections:
# the removal filters the keep-list by the logical dir name ("p.proj/..."), so entries under the
# physical name would mismatch and drop the blobs.
# Scenario:
# - create the replicated zero-copy 'flat' table on both replicas, insert on replica 1, SYNC replica 2
# - mutate on replica 1 and zero-copy-fetch the result on replica 2
# - drop replica 1 so replica 2's removal decides the fate of the shared blobs
# - assert no outdated parts, no broken projection, SELECT matches baseline, CHECK TABLE passes
def test_blob_zero_copy_mutation_preserves():
    # create the replicated zero-copy 'flat' table on both replicas
    for n, replica in ((node, "1"), (node2, "2")):
        n.query("DROP TABLE IF EXISTS t_zc SYNC")
        n.query(
            f"""CREATE TABLE t_zc (key UInt64, id UInt64, value String,
                PROJECTION p (SELECT key, id ORDER BY id))
                ENGINE = ReplicatedMergeTree('/clickhouse/tables/t_zc', '{replica}')
                ORDER BY key
                SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    storage_policy = 's3', allow_remote_fs_zero_copy_replication = 1,
                    old_parts_lifetime = 20, cleanup_delay_period = 1, max_cleanup_delay_period = 3"""
        )

    # insert on replica 1, sync replica 2, capture baseline
    node.query(
        "INSERT INTO t_zc SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node2.query("SYSTEM SYNC REPLICA t_zc")
    baseline = proj_query("t_zc")
    assert proj_query("t_zc", node2) == baseline
    p2 = part_dir("t_zc", node2)
    assert path_exists(f"{p2}.p.proj", node2)

    # make replica 1 execute the mutation and replica 2 zero-copy-fetch its result
    node2.query("SYSTEM STOP REPLICATION QUEUES t_zc")
    node.query(
        "ALTER TABLE t_zc UPDATE value = concat(value, 'x') WHERE 1 SETTINGS mutations_sync = 1"
    )
    node2.query("SYSTEM START REPLICATION QUEUES t_zc")
    node2.query("SYSTEM SYNC REPLICA t_zc")
    assert active_parts("t_zc", node2) == "1"

    # release replica 1's zero-copy locks before replica 2 removes the old part, so replica 2's
    # removal is the one that decides the fate of the shared blobs
    node.query("DROP TABLE t_zc SYNC")
    wait_for(lambda: outdated_parts("t_zc", node2) == "0")

    # assert the shared blobs survived: projection healthy, SELECT matches, CHECK passes
    assert outdated_parts("t_zc", node2) == "0"
    assert broken_projection_parts("t_zc", node2) == "0"
    assert (
        proj_query("t_zc", node2, extra_settings="force_optimize_projection = 1")
        == baseline
    )
    assert check_table("t_zc", node2) == "1"


# This test checks that the default hardlink mutation shares inodes with the source projection and
# keeps the projection usable after the source part is dropped (keep-list correctness).
# Scenario:
# - create a 'flat' table whose projection excludes the mutated column, insert data
# - mutate the column (carried by hardlink)
# - assert the new sibling shares inodes with the source projection
# - drop the source part from disk, assert CHECK TABLE and SELECT still work
def test_blob_mutation_hardlinks():
    # create a 'flat' table whose projection excludes the mutated column
    node.query("DROP TABLE IF EXISTS t_hl SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_hl (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               old_parts_lifetime = 1, cleanup_delay_period = 1, max_cleanup_delay_period = 3,
               cleanup_delay_period_random_add = 1"""
    )
    node.query("SYSTEM STOP CLEANUP t_hl")
    node.query(
        "INSERT INTO t_hl SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # mutate the non-projection column; the projection is carried by hardlink
    node.query(
        "ALTER TABLE t_hl UPDATE value = 'x' WHERE key = 1 SETTINGS mutations_sync = 2"
    )
    new_part = part_dir("t_hl")
    links = node.exec_in_container(
        ["bash", "-c", f"stat -c %h {new_part}.p.proj/checksums.txt"],
        privileged=True,
        user="root",
    ).strip()
    assert int(links) >= 2  # hardlinked from the source projection

    # drop the source part from disk and verify the new part still works. Removal is a background,
    # time-gated task: old_parts_lifetime = 1 makes the outdated source eligible ~1s after the mutation,
    # and the cleanup_delay_period settings cap the sweep cadence at a few seconds, so a short wait
    # suffices - a timeout here means removal genuinely stalled, not runner lag.
    node.query("SYSTEM START CLEANUP t_hl")
    node.query("SYSTEM START MERGES t_hl")
    wait_for(lambda: outdated_parts("t_hl") == "0", timeout=30)
    assert outdated_parts("t_hl") == "0"
    assert check_table("t_hl") == "1"
    assert proj_query("t_hl") != ""


# This test checks that always_use_copy_instead_of_hardlinks carries the projection by copying: no
# inode is shared with the source part and the zero-copy keep-list stays empty.
# Scenario:
# - create a 'flat' table with always_use_copy_instead_of_hardlinks = 1 whose projection excludes the mutated column
# - mutate the column (carried by copy)
# - assert the new sibling shares no inode (link count 1) and CHECK TABLE / SELECT work
def test_blob_mutation_always_copy():
    # create a 'flat' table that copies instead of hardlinking, projection excludes the mutated column
    node.query("DROP TABLE IF EXISTS t_copy SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_copy (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                    always_use_copy_instead_of_hardlinks = 1"""
    )
    node.query(
        "INSERT INTO t_copy SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )

    # mutate the non-projection column; the projection is carried by copy
    old_part = part_dir("t_copy")
    node.query(
        "ALTER TABLE t_copy UPDATE value = 'x' WHERE key = 1 SETTINGS mutations_sync = 2"
    )
    new_part = part_dir("t_copy")
    assert new_part != old_part
    assert path_exists(f"{new_part}.p.proj")

    # assert no inode is shared (link count 1) and the part is healthy
    links = node.exec_in_container(
        ["bash", "-c", f"stat -c %h {new_part}.p.proj/checksums.txt"],
        privileged=True,
        user="root",
    ).strip()
    assert links == "1"
    assert check_table("t_copy") == "1"
    assert proj_query("t_copy") != ""


# This test checks that the orphan reaper removes remote blobs of a reaped orphan sibling when there
# is no zero-copy: keeping them (keep_in_remote_fs=true) would leak them in object storage forever.
# Scenario:
# - create a 'flat' table on the s3 disk, insert, record the sibling's blob keys
# - stop the server, hide the owner part dir so the sibling becomes a genuine orphan, start it
# - assert startup GC reaps the orphan sibling metadata
# - assert the sibling's blobs are gone from object storage
def test_blob_orphan_gc_removes_without_zero_copy():
    # create a 'flat' table on the s3 disk, record the sibling's blob keys
    node.query("DROP TABLE IF EXISTS t_leak SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_leak (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               storage_policy = 's3'"""
    )
    node.query(
        "INSERT INTO t_leak SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    p = part_dir("t_leak")
    name = part_name("t_leak")
    uuid = node.query("SELECT uuid FROM system.tables WHERE name = 't_leak'").strip()
    sibling_keys = sibling_blob_keys(uuid, name)
    assert sibling_keys  # the projection really lives on the object storage disk
    assert sibling_keys <= minio_keys()  # sanity: key format matches the listing

    # hide the owner dir so the sibling becomes a genuine orphan, then restart
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"mv {p} /tmp/hidden_{name}"], privileged=True, user="root"
    )
    node.start_clickhouse()

    # assert startup GC reaps the orphan sibling metadata and its blobs are gone
    wait_for(lambda: not path_exists(f"{p}.p.proj"))
    assert not path_exists(f"{p}.p.proj")
    leaked = sibling_keys & minio_keys()
    assert leaked == set()

    # cleanup
    node.exec_in_container(
        ["bash", "-c", f"rm -rf /tmp/hidden_{name}"], privileged=True, user="root"
    )
    node.query("DROP TABLE t_leak SYNC")


# This test checks that the publish-time destination sweep removes remote blobs of the residue it
# clears when there is no zero-copy: keeping them would be a permanent S3 leak.
# Scenario:
# - create a 'flat' s3 donor (real blob-backed sibling) and a projection-less-on-insert 'flat' s3 table
# - move the donor sibling as residue to the table's future part name, record its blob keys
# - INSERT to publish the part (the destination sweep clears the residue)
# - assert the residue is gone, its blobs are gone, and the data reads
def test_blob_publish_sweep_removes():
    # create an s3 donor (real blob-backed sibling) and a projection-less-on-insert s3 table
    node.query("DROP TABLE IF EXISTS t_sweep SYNC")
    node.query("DROP TABLE IF EXISTS t_sweep_donor SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_sweep_donor (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               storage_policy = 's3'"""
    )
    node.query(
        """CREATE TABLE t_sweep (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               storage_policy = 's3', materialize_projections_on_insert = 0"""
    )
    node.query(
        "INSERT INTO t_sweep_donor SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    donor_sib = f"{part_dir('t_sweep_donor')}.p.proj"
    donor_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE name = 't_sweep_donor'"
    ).strip()
    donor_part = part_name("t_sweep_donor")
    sibling_keys = sibling_blob_keys(donor_uuid, donor_part)
    assert sibling_keys
    assert sibling_keys <= minio_keys()  # sanity: key format matches the listing

    # plant the donor's sibling as residue at t_sweep's future part name (mv, not cp: a metadata
    # copy would lie about blob refcounts); the donor table is sacrificed
    residue = f"{table_path('t_sweep')}/all_1_1_0.p.proj"
    node.exec_in_container(
        ["bash", "-c", f"mv {donor_sib} {residue}"], privileged=True, user="root"
    )

    # the first insert publishes all_1_1_0; the destination sweep must remove the residue with its blobs
    node.query("INSERT INTO t_sweep SELECT number, number, '' FROM numbers(100)")
    p = part_dir("t_sweep")
    assert p.endswith("all_1_1_0")  # the name collision really happened
    assert not path_exists(residue)
    leaked = sibling_keys & minio_keys()
    assert leaked == set()
    assert node.query("SELECT count() FROM t_sweep").strip() == "100"

    # cleanup
    node.query("DROP TABLE t_sweep SYNC")
    node.query("DROP TABLE t_sweep_donor SYNC")


# This test checks the ATTACH-path destination sweep removes remote blobs of the residue it clears
# when there is no zero-copy: PartsTemporaryRename built its storages without the zero-copy flag, so
# it always kept the blobs - the same S3 leak.
# Scenario:
# - create two 'flat' s3 tables, insert into both, DETACH a part from the first
# - move the donor sibling as residue to the attaching_ destination name, record its blob keys
# - ATTACH the detached part (the destination sweep clears the residue)
# - assert the sweep fired, the residue and its blobs are gone, and the attached part serves its projection
def test_blob_attach_sweep_removes():
    # create two 'flat' s3 tables, insert into both, DETACH a part from the first
    node.query("DROP TABLE IF EXISTS t_att SYNC")
    node.query("DROP TABLE IF EXISTS t_att_donor SYNC")
    node.query("SYSTEM STOP MERGES")
    for tname in ("t_att", "t_att_donor"):
        node.query(
            f"""CREATE TABLE {tname} (key UInt64, id UInt64, value String,
               PROJECTION p (SELECT key, id ORDER BY id))
               ENGINE = MergeTree ORDER BY key
               SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
                   storage_policy = 's3'"""
        )
        node.query(
            f"INSERT INTO {tname} SELECT number, number * 2, toString(number) FROM numbers(1000)"
        )
    name = part_name("t_att")
    node.query(f"ALTER TABLE t_att DETACH PART '{name}'")

    # record the donor's blob keys
    donor_sib = f"{part_dir('t_att_donor')}.p.proj"
    donor_uuid = node.query(
        "SELECT uuid FROM system.tables WHERE name = 't_att_donor'"
    ).strip()
    donor_part = part_name("t_att_donor")
    sibling_keys = sibling_blob_keys(donor_uuid, donor_part)
    assert sibling_keys
    assert sibling_keys <= minio_keys()  # sanity: key format matches the listing

    # plant the donor's sibling as residue at the attaching_ destination name (mv, not cp)
    residue = f"{table_path('t_att')}/detached/attaching_{name}.p.proj"
    node.exec_in_container(
        ["bash", "-c", f"mv {donor_sib} {residue}"], privileged=True, user="root"
    )

    # ATTACH renames detached/<name> to detached/attaching_<name>; the sweep must remove the residue + blobs
    node.query(f"ALTER TABLE t_att ATTACH PART '{name}'")
    assert node.wait_for_log_line(f"detached/attaching_{name}.p.proj")
    assert not path_exists(residue)
    leaked = sibling_keys & minio_keys()
    assert leaked == set()

    # assert the attached part serves its own projection
    assert broken_projection_parts("t_att") == "0"
    assert proj_query("t_att", extra_settings="force_optimize_projection = 1") != ""

    # cleanup
    node.query("DROP TABLE t_att SYNC")
    node.query("DROP TABLE t_att_donor SYNC")


# ==============================================================================
# I. Orphan-sibling garbage collection
# ==============================================================================

# This test checks that the periodic cleaner reaps aged orphan siblings from the live root but never
# a young one (it may belong to an in-flight rename); moving/ falls to the stale-moving-parts sweep,
# and startup (age 0) reaps everything.
# Scenario:
# - create a 'flat' table with a short cleanup interval, plant an aged orphan, a young orphan, and an aged moving/ orphan
# - trigger the background cleanup with a merge
# - assert the aged orphans are reaped, the young one is spared
# - restart; assert the startup pass reaps even the young orphan
def test_orphan_gc_periodic_and_startup():
    # create a 'flat' table with a short cleanup interval, plant aged/young/moving orphans
    setup_table(
        "t_gc",
        "projection_storage_format = 'flat', temporary_directories_lifetime = 3600, "
        "merge_tree_clear_old_temporary_directories_interval_seconds = 1",
    )
    data_root = "/".join(part_dir("t_gc").split("/")[:-1])
    aged = f"{data_root}/gone_0_0_0.p.proj"
    young = f"{data_root}/fresh_0_0_0.p.proj"
    moving_aged = f"{data_root}/moving/gone_1_1_1.p.proj"
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {aged} {young} {moving_aged} && touch -d '2 hours ago' {aged} {moving_aged}",
        ],
        privileged=True,
        user="root",
    )

    # background cleanup runs only when there is work for the cleanup task
    node.query("SYSTEM START MERGES t_gc")
    node.query("INSERT INTO t_gc SELECT number, number, '' FROM numbers(10)")
    node.query("OPTIMIZE TABLE t_gc FINAL")

    # assert the aged orphans are reaped and the young one is spared (age guard)
    wait_for(lambda: not path_exists(aged))
    assert not path_exists(aged)
    wait_for(lambda: not path_exists(moving_aged))
    assert not path_exists(moving_aged)
    assert path_exists(young)  # age guard: far younger than the 3600s lifetime, cannot age out mid-test

    # restart: startup() runs the age-0 sweep synchronously, before its background tasks start. A table
    # read blocks on waitTableStarted until startup() has finished, so the young orphan is already reaped
    # when the read returns - no need to poll the filesystem for it.
    node.restart_clickhouse()
    block_until_tables_loaded("t_gc")
    assert not path_exists(young)


# This test checks that the orphan-GC age guard does not reap a sibling whose owner is mid-rename:
# renames never refresh mtime, so during DROP DETACHED PART an aged sibling of a valid in-flight
# operation must not look like a reapable orphan. A failpoint pauses the rename to make the race deterministic.
# Scenario:
# - create a 'flat' table with a short cleanup interval, DETACH PART, age the detached sibling and an aged decoy orphan
# - enable the sibling-rename failpoint and start DROP DETACHED PART (parent moves, sibling waits)
# - drive the cleaner until it provably cycled inside the window (the decoy is reaped)
# - assert the in-flight sibling was spared and no reap log line mentions it
def test_orphan_gc_spares_inflight_rename():
    # create a 'flat' table with a short cleanup interval, DETACH the part
    setup_table(
        "t_race",
        "projection_storage_format = 'flat', "
        "merge_tree_clear_old_temporary_directories_interval_seconds = 1",
    )
    name = part_name("t_race")
    root = table_path("t_race")
    node.query(f"ALTER TABLE t_race DETACH PART '{name}'")
    sib = f"{root}/detached/{name}.p.proj"
    assert path_exists(sib)

    # trigger inserts must not create flat siblings of their own: their publish rename would park on
    # the same failpoint that holds the DROP DETACHED window open
    node.query("ALTER TABLE t_race MODIFY SETTING materialize_projections_on_insert = 0")

    # age the detached sibling and an aged decoy orphan (a positive control for the cleaner cycling)
    decoy = f"{root}/detached/gone_0_0_0.p.proj"
    node.exec_in_container(
        [
            "bash",
            "-c",
            f"mkdir -p {decoy} && touch -d '2 days ago' {decoy} {sib} {root}/detached/{name}",
        ],
        privileged=True,
        user="root",
    )

    # open the commit window: the parent moves to deleting_<name>, the sibling waits at the old name
    node.query("SYSTEM ENABLE FAILPOINT pause_before_flat_projection_sibling_moves")
    try:
        drop = node.get_query_request(
            f"ALTER TABLE t_race DROP DETACHED PART '{name}' SETTINGS allow_drop_detached = 1"
        )
        wait_for(
            lambda: path_exists(f"{root}/detached/deleting_{name}")
            and path_exists(sib)
        )
        assert path_exists(f"{root}/detached/deleting_{name}")

        # nudge the background assignee until the cleaner provably ran inside the window
        def cleaner_ran():
            node.query("INSERT INTO t_race SELECT number, number, '' FROM numbers(10)")
            return not path_exists(decoy)

        wait_for(cleaner_ran, timeout=30)
        assert not path_exists(decoy)  # positive control: the cleaner cycled
        still_there = path_exists(sib)
    finally:
        node.query(
            "SYSTEM DISABLE FAILPOINT pause_before_flat_projection_sibling_moves"
        )
    drop.get_answer_and_error()  # let the drop finish either way

    # assert the cleaner did not reap a sibling whose owner is mid-rename. The decoy's reap line is the
    # barrier: the decoy and the in-flight sibling are weighed in the same clearOrphanProjectionSiblings
    # sweep, so once the decoy line is in the log that sweep has run and flushed - a sibling reap, had it
    # happened, would be on disk by now too. This lets the negative check read a settled log, no polling.
    assert still_there
    node.wait_for_log_line(f"{decoy} whose part directory")
    assert not node.contains_in_log(f"{sib} whose part directory")


# This test checks that a part that breaks before loadProjections seeds the owned set still carries
# its flat sibling into detached/ for later repair, instead of the startup orphan GC deleting it.
# Scenario:
# - create a 'flat' table
# - stop the server, corrupt checksums.txt (breaks before the owned set is seeded), start it
# - assert the part was detached as broken and its sibling followed it into detached/
# - assert no sibling is stranded at the live name
def test_orphan_gc_broken_part_preserves_sibling():
    # create a 'flat' table
    setup_table("t_broken", "projection_storage_format = 'flat'")
    p = part_dir("t_broken")
    name = part_name("t_broken")
    root = table_path("t_broken")
    assert path_exists(f"{p}.p.proj")

    # corrupt checksums.txt so the part breaks before loadProjections seeds the owned set
    node.stop_clickhouse()
    node.exec_in_container(
        ["bash", "-c", f"echo garbage > {p}/checksums.txt"],
        privileged=True,
        user="root",
    )
    node.start_clickhouse()

    # assert the part was detached as broken and its sibling followed it into detached/.
    # With async_load_databases the part is loaded - and detected broken during loadDataParts - in the
    # background after start_clickhouse() returns. Nothing here queries the table (a pure filesystem
    # check does not force the load), so wait for the broken part to land in detached/ before asserting.
    def find_broken_part():
        return node.exec_in_container(
            ["bash", "-c", f"find {root}/detached -maxdepth 1 -type d -name 'broken*{name}' | head -1"],
            privileged=True,
            user="root",
        ).strip()

    wait_for(lambda: find_broken_part() != "", timeout=120)
    detached_parent = find_broken_part()
    assert detached_parent != ""  # the part really was detached as broken
    assert path_exists(f"{detached_parent}.p.proj")
    assert not path_exists(f"{p}.p.proj")


# ==============================================================================
# J. Freeze / shadow siblings
# ==============================================================================

# This test checks that FREEZE copies the flat projection sibling into shadow/, instead of dropping it. (Issue #2)
# Scenario:
# - create a 'flat' table
# - FREEZE WITH NAME
# - assert a projection sibling exists under shadow/
# @pytest.mark.xfail(reason=REVIEW + "3472535412", strict=False)
def test_freeze_copies_sibling():
    # create a 'flat' table and freeze it
    setup_table("t_freeze", "projection_storage_format = 'flat'")
    node.query("ALTER TABLE t_freeze FREEZE WITH NAME 'flatproj'")

    # assert a projection sibling exists under shadow/
    found = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/shadow/flatproj -name '*p.proj' | head -1"],
        privileged=True,
        user="root",
    ).strip()
    assert found != ""


# This test checks that UNFREEZE removes flat projection siblings from shadow/ together with their
# owner, leaving no part dirs or projection siblings behind.
# Scenario:
# - create a 'flat' table, FREEZE WITH NAME, assert a sibling exists under shadow/
# - UNFREEZE WITH NAME
# - assert no part dirs and no projection siblings remain, and the table is intact
def test_freeze_unfreeze_removes_siblings():
    # create a 'flat' table and freeze it
    setup_table("t_unfreeze", "projection_storage_format = 'flat'")
    node.query("ALTER TABLE t_unfreeze FREEZE WITH NAME 'unfr'")
    found = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/shadow/unfr -name '*p.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert int(found) >= 1

    # unfreeze; the empty dir skeleton may remain but no part dirs / projection siblings may
    node.query("ALTER TABLE t_unfreeze UNFREEZE WITH NAME 'unfr'")
    leftovers = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/shadow/unfr \\( -name '*_*' -o -name '*.proj' -o -name '*.tmp_proj' \\) 2>/dev/null | wc -l",
        ],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"
    assert check_table("t_unfreeze") == "1"


# This test checks that UNFREEZE reaps an owner-less shadow sibling (a crash between freeze's sibling
# and parent copies leaves one, and no other cleanup visits shadow/).
# Scenario:
# - create a 'flat' table, FREEZE WITH NAME, locate an owner and its sibling
# - simulate the crash window: remove the owner dir but keep the sibling
# - UNFREEZE WITH NAME
# - assert the "Removing frozen projection sibling" log line, no *.proj leftovers, table intact
def test_freeze_unfreeze_reaps_ownerless_sibling():
    # create a 'flat' table, freeze it, locate an owner + its sibling
    setup_table("t_shadow", "projection_storage_format = 'flat'")
    node.query("ALTER TABLE t_shadow FREEZE WITH NAME 'orph'")
    owner = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/shadow/orph -type d -name '*_*_*_*' ! -name '*.proj' | head -1",
        ],
        privileged=True,
        user="root",
    ).strip()
    assert owner != ""
    assert path_exists(f"{owner}.p.proj")

    # simulate the crash window: the sibling was copied, the parent dir was not
    node.exec_in_container(["bash", "-c", f"rm -rf {owner}"], privileged=True, user="root")
    node.query("ALTER TABLE t_shadow UNFREEZE WITH NAME 'orph'")

    # assert the owner-less sibling was reaped and the live table is untouched
    assert node.wait_for_log_line("Removing frozen projection sibling")
    leftovers = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/shadow/orph -name '*.proj' 2>/dev/null | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert leftovers == "0"
    assert check_table("t_shadow") == "1"
    assert proj_query("t_shadow", extra_settings="force_optimize_projection = 1") != ""


# This test checks that the owner-less reap respects the partition matcher: UNFREEZE PARTITION must
# only touch orphan siblings of the matched partition.
# Scenario:
# - create a partitioned 'flat' table, FREEZE WITH NAME, make an orphan sibling in partition 0
# - UNFREEZE PARTITION 1; assert partition 0's orphan is untouched
# - UNFREEZE PARTITION 0; assert partition 0's orphan is reaped, table intact
def test_freeze_unfreeze_partition_scopes_reap():
    # create a partitioned 'flat' table and freeze it
    node.query("DROP TABLE IF EXISTS t_shadow2 SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_shadow2 (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree PARTITION BY key % 2 ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat'"""
    )
    node.query(
        "INSERT INTO t_shadow2 SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    node.query("ALTER TABLE t_shadow2 FREEZE WITH NAME 'orph2'")

    # make an orphan sibling in partition 0 (remove its owner dir)
    owner0 = node.exec_in_container(
        [
            "bash",
            "-c",
            "find /var/lib/clickhouse/shadow/orph2 -type d -name '0_*' ! -name '*.proj' | head -1",
        ],
        privileged=True,
        user="root",
    ).strip()
    assert owner0 != ""
    node.exec_in_container(["bash", "-c", f"rm -rf {owner0}"], privileged=True, user="root")

    def orphan_sibling_count():
        return node.exec_in_container(
            ["bash", "-c", "find /var/lib/clickhouse/shadow/orph2 -name '0_*.proj' 2>/dev/null | wc -l"],
            privileged=True,
            user="root",
        ).strip()

    # unfreezing the OTHER partition must not touch partition 0's orphan sibling
    node.query("ALTER TABLE t_shadow2 UNFREEZE PARTITION 1 WITH NAME 'orph2'")
    assert orphan_sibling_count() == "1"

    # unfreezing partition 0 reaps its orphan sibling
    node.query("ALTER TABLE t_shadow2 UNFREEZE PARTITION 0 WITH NAME 'orph2'")
    assert orphan_sibling_count() == "0"
    assert check_table("t_shadow2") == "1"


# ==============================================================================
# K. Backup and restore
# ==============================================================================

# This test checks that BACKUP/RESTORE stores and finds flat projection data under the LOGICAL name
# (<part>/p.proj/...), so backups are layout-independent; the physical sibling name would restore a
# bogus nested dir.
# Scenario:
# - create a 'flat' table, capture SELECT baseline
# - BACKUP TABLE to a file
# - assert the backup uses the logical p.proj name, never a physical *.*.proj dir
# - RESTORE to a new table
# - assert the restore reads, the projection is healthy, SELECT matches, CHECK TABLE passes
def test_backup_restore_flat():
    # create a 'flat' table, capture baseline, clear any prior backup
    setup_table("t_bk", "projection_storage_format = 'flat'")
    baseline = proj_query("t_bk")
    node.query("DROP TABLE IF EXISTS t_bk2 SYNC")
    node.exec_in_container(
        ["bash", "-c", "rm -rf /var/lib/clickhouse/backups/t_bk"],
        privileged=True,
        user="root",
    )

    # back up the table and assert the backup uses the logical name, not the physical sibling name
    node.query("BACKUP TABLE t_bk TO File('/var/lib/clickhouse/backups/t_bk')")
    physical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name '*.*.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert physical_dirs == "0"
    logical_dirs = node.exec_in_container(
        ["bash", "-c", "find /var/lib/clickhouse/backups/t_bk -type d -name 'p.proj' | wc -l"],
        privileged=True,
        user="root",
    ).strip()
    assert logical_dirs == "1"

    # restore into a new table and assert it is healthy and matches baseline
    node.query("RESTORE TABLE t_bk AS t_bk2 FROM File('/var/lib/clickhouse/backups/t_bk')")
    assert node.query("SELECT count() FROM t_bk2").strip() == "1000"
    assert broken_projection_parts("t_bk2") == "0"
    assert proj_query("t_bk2", extra_settings="force_optimize_projection = 1") == baseline
    assert check_table("t_bk2") == "1"


# ==============================================================================
# L. fsync durability smoke
# ==============================================================================

# This test checks that fsync_part_directory=1 exercises the directory-sync points added for flat
# siblings across the lifecycle (power-loss durability itself is untestable in CI; guard placement is
# pinned by the gtest).
# Scenario:
# - create a 'flat' table with fsync_part_directory=1, insert, add and materialize a second projection
# - MERGE (OPTIMIZE TABLE FINAL), drop outdated source parts, DETACH/ATTACH the covering part
# - restart the server
# - assert one active part, no broken projection, both projection parts active, CHECK TABLE passes
def test_fsync_flat_lifecycle():
    # create a 'flat' table with fsync enabled, insert data
    node.query("DROP TABLE IF EXISTS t_fsync SYNC")
    node.query("SYSTEM STOP MERGES")
    node.query(
        """CREATE TABLE t_fsync (key UInt64, id UInt64, value String,
           PROJECTION p (SELECT key, id ORDER BY id))
           ENGINE = MergeTree ORDER BY key
           SETTINGS min_bytes_for_wide_part = 0, projection_storage_format = 'flat',
               fsync_part_directory = 1, old_parts_lifetime = 1"""
    )
    node.query(
        "INSERT INTO t_fsync SELECT number, number * 2, toString(number) FROM numbers(1000)"
    )
    assert path_exists(f"{part_dir('t_fsync')}.p.proj")

    # add and materialize a second projection, then merge
    node.query("ALTER TABLE t_fsync ADD PROJECTION q (SELECT id, key ORDER BY key)")
    node.query("ALTER TABLE t_fsync MATERIALIZE PROJECTION q SETTINGS mutations_sync = 2")
    node.query("SYSTEM START MERGES t_fsync")
    node.query("OPTIMIZE TABLE t_fsync FINAL")

    # drop the outdated source parts first: once the covering part is detached and re-attached under
    # a new block number, a surviving outdated part would resurrect as active on restart
    wait_for(lambda: outdated_parts("t_fsync") == "0", timeout=120)

    # round-trip the covering part through DETACH/ATTACH, then restart
    name = part_name("t_fsync")
    node.query(f"ALTER TABLE t_fsync DETACH PART '{name}'")
    node.query(f"ALTER TABLE t_fsync ATTACH PART '{name}'")
    node.restart_clickhouse()
    block_until_tables_loaded("t_fsync")

    # assert the lifecycle left one healthy part with both projections
    assert active_parts("t_fsync") == "1"
    assert broken_projection_parts("t_fsync") == "0"
    assert int(active_projection_parts("t_fsync")) >= 2
    assert check_table("t_fsync") == "1"
