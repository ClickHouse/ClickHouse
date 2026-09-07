# Tests that directly manipulate the server's on-disk metadata files and flag
# files. These scenarios cannot be stateless tests because stateless tests must
# not modify the server's files on disk (table metadata .sql, flags directory).

import pytest

from helpers.cluster import ClickHouseCluster
from helpers.database_disk import (
    get_database_disk_name,
    read_metadata,
    replace_text_in_metadata,
    write_metadata,
)

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance("node")


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


def test_matview_columns_after_modify_query(started_cluster):
    # Converted from stateless test 03001_matview_columns_after_modify_query.sh.
    node.query("DROP TABLE IF EXISTS src_modify_query")
    node.query("DROP TABLE IF EXISTS mv_modify_query")
    node.query("CREATE TABLE src_modify_query(Timestamp DateTime64(9), c1 String, c2 String) ENGINE=MergeTree ORDER BY Timestamp")
    node.query(
        "CREATE MATERIALIZED VIEW mv_modify_query(timestamp DateTime, c12 Nullable(String)) ENGINE=MergeTree ORDER BY timestamp AS SELECT Timestamp as timestamp, c1 || c2 as c12 FROM src_modify_query"
    )

    mv_uuid = node.query("SELECT uuid FROM system.tables WHERE table='mv_modify_query' AND database='default'").strip()
    if mv_uuid != "00000000-0000-0000-0000-000000000000":
        inner_table_name = f".inner_id.{mv_uuid}"
    else:
        inner_table_name = ".inner.mv_modify_query"

    node.query("INSERT INTO src_modify_query SELECT '2024-02-22'::DateTime + number, number, number FROM numbers(3)")

    src_expected = "Timestamp\tc1\tc2\nDateTime64(9)\tString\tString\n2024-02-22 00:00:00.000000000\t0\t0\n2024-02-22 00:00:01.000000000\t1\t1\n2024-02-22 00:00:02.000000000\t2\t2\n"
    mv_original = "timestamp\tc12\nDateTime\tNullable(String)\n2024-02-22 00:00:00\t00\n2024-02-22 00:00:01\t11\n2024-02-22 00:00:02\t22\n"
    # Columns as they look after the on-disk metadata of the materialized view
    # was rewritten to the wrong types.
    mv_hacked = "timestamp\tc12\nDateTime64(9)\tString\n2024-02-22 00:00:00.000000000\t00\n2024-02-22 00:00:01.000000000\t11\n2024-02-22 00:00:02.000000000\t22\n"

    def mv_and_inner():
        mv = node.query("SELECT * FROM mv_modify_query ORDER BY timestamp FORMAT TSVWithNamesAndTypes")
        inner = node.query(f"SELECT * FROM `{inner_table_name}` ORDER BY timestamp FORMAT TSVWithNamesAndTypes")
        return mv, inner

    assert node.query("SELECT * FROM src_modify_query ORDER BY Timestamp FORMAT TSVWithNamesAndTypes") == src_expected
    assert mv_and_inner() == (mv_original, mv_original)

    # Test 1. MODIFY QUERY doesn't change columns.
    node.query("ALTER TABLE mv_modify_query MODIFY QUERY SELECT Timestamp as timestamp, c1 || c2 as c12 FROM src_modify_query")
    assert mv_and_inner() == (mv_original, mv_original)

    # Test 2. MODIFY QUERY with explicit data types doesn't change columns.
    node.query("ALTER TABLE mv_modify_query MODIFY QUERY SELECT Timestamp::DateTime64(9) as timestamp, (c1 || c2)::String as c12 FROM src_modify_query")
    assert mv_and_inner() == (mv_original, mv_original)

    # Test 3. MODIFY QUERY can even fix wrong columns.
    # We need that because of https://github.com/ClickHouse/ClickHouse/issues/60369
    mv_metadata_path = node.query("SELECT metadata_path FROM system.tables WHERE table='mv_modify_query' AND database='default'").strip()
    node.query("DETACH TABLE mv_modify_query")

    data_path = node.query("SELECT path FROM system.disks WHERE name = 'default'").strip()
    full_metadata_path = data_path + mv_metadata_path

    exists = node.exec_in_container(
        ["bash", "-c", f'test -e "{full_metadata_path}" && echo 1 || echo 0'],
        privileged=True,
        user="root",
    ).strip()
    if exists == "1":
        node.exec_in_container(
            [
                "bash",
                "-c",
                f"sed -i -e 's/`timestamp` DateTime,/`timestamp` DateTime64(9),/g' -e 's/`c12` Nullable(String)/`c12` String/g' \"{full_metadata_path}\"",
            ],
            privileged=True,
            user="root",
        )
    else:
        # Using a remote DB disk: the metadata .sql is not on the local default
        # disk, so edit it through `clickhouse disks` like the original test did.
        replace_text_in_metadata(node, mv_metadata_path, "`timestamp` DateTime,", "`timestamp` DateTime64(9),")
        replace_text_in_metadata(node, mv_metadata_path, "`c12` Nullable(String)", "`c12` String")
        # We need to reload the DB disk if it is a plain-rewritable disk to be able to see the changes
        node.query(f"SYSTEM CLEAR DISK METADATA CACHE {get_database_disk_name(node)}")

    node.query("ATTACH TABLE mv_modify_query")

    # Before MODIFY QUERY: the view has the wrong columns from the hacked
    # metadata, while the inner table keeps the original ones.
    assert mv_and_inner() == (mv_hacked, mv_original)

    node.query("ALTER TABLE mv_modify_query MODIFY QUERY SELECT Timestamp as timestamp, c1 || c2 as c12 FROM src_modify_query")

    # After MODIFY QUERY: the columns are fixed back.
    assert mv_and_inner() == (mv_original, mv_original)

    node.query("DROP TABLE mv_modify_query SYNC")
    node.query("DROP TABLE src_modify_query SYNC")


def test_create_or_replace_with_force_drop_flag(started_cluster):
    # Converted from stateless test 04329_create_or_replace_force_drop_flag.sh.
    server_path = node.query("SELECT value FROM system.server_settings WHERE name = 'path'").strip()
    # `server_path` may or may not end with `/`; normalize before appending `/flags`.
    flags_dir = server_path.rstrip("/") + "/flags"
    flag_file = flags_dir + "/force_drop_table"

    node.exec_in_container(["bash", "-c", f'mkdir -p "{flags_dir}"'], privileged=True, user="root")

    def cleanup():
        node.exec_in_container(["bash", "-c", f'rm -f "{flag_file}"'], privileged=True, user="root")
        node.query("DROP TABLE IF EXISTS t04329 SYNC", settings={"max_table_size_to_drop": 0})

    cleanup()
    try:
        node.query("CREATE TABLE t04329 (a UInt64) ENGINE = MergeTree() ORDER BY a")
        node.query("INSERT INTO t04329 SELECT number FROM numbers(1000)")

        node.exec_in_container(
            ["bash", "-c", f'touch "{flag_file}" && chmod a=rw "{flag_file}"'],
            privileged=True,
            user="root",
        )

        # With `max_table_size_to_drop = 1` replacing the non-empty table would be
        # rejected, but the `force_drop_table` flag file allows it.
        node.query(
            "CREATE OR REPLACE TABLE t04329 (b UInt64) ENGINE = MergeTree() ORDER BY b AS SELECT number FROM numbers(50)",
            settings={"max_table_size_to_drop": 1},
        )

        assert node.query("SELECT name FROM system.columns WHERE database = 'default' AND table = 't04329' ORDER BY name") == "b\n"
        assert node.query("SELECT count() FROM t04329") == "50\n"
        assert node.query("SELECT count() FROM system.tables WHERE database = 'default' AND name LIKE '%tmp_replace%'") == "0\n"

        # The server consumes (removes) the flag file when it is used.
        flag_consumed = node.exec_in_container(
            ["bash", "-c", f'test -f "{flag_file}" && echo 0 || echo 1'],
            privileged=True,
            user="root",
        ).strip()
        assert flag_consumed == "1"
    finally:
        cleanup()


def test_attach_projection_part_offset_setting_disabled(started_cluster):
    # Converted from stateless test 04545_attach_projection_part_offset_setting_disabled.sh.
    #
    # Regression for issue #102445 plus the surrounding checkProperties projection gate contract.
    #
    # checkProperties gates a projection that uses a virtual column behind the feature setting that
    # enables it. Two distinct gate classes:
    #   - allow_part_offset_column_in_projections / allow_commit_order_projection are pure CREATE-time
    #     gates (nothing at merge / MATERIALIZE PROJECTION time reads them). They must fire only when
    #     THIS operation is responsible for the invalid pairing: a projection introduced by the current
    #     operation (CREATE / ADD PROJECTION) with the gate off, or an ALTER that flips the gate from
    #     enabled to disabled while such a projection already exists. They must NOT fire on ATTACH
    #     (else a table becomes permanently unattachable once the default flips across versions, the
    #     #102445 bug) nor for an unrelated later ALTER that leaves an already-disabled gate untouched.
    #   - enable_block_number_column / enable_block_offset_column are NOT CREATE-only: a commit-order
    #     projection can be rebuilt from the base part during a merge, and that rebuild produces
    #     _block_number / _block_offset only when these settings are enabled. So they stay validated
    #     for every projection (even pre-existing, even on ATTACH) against the effective post-operation
    #     settings.
    # Turns a CREATE-only gate off directly in the on-disk metadata (simulating a cross-version default
    # flip) and re-attaches, exercising the #102445 ATTACH path without an ALTER (which is rejected, see
    # the flip cases below).
    def attach_with_gate_disabled(table, setting):
        metadata_path = node.query(f"SELECT metadata_path FROM system.tables WHERE database = 'default' AND name = '{table}'").strip()
        node.query(f"DETACH TABLE {table}")
        # Go through `clickhouse disks` instead of editing the file directly: with a remote database
        # disk the metadata `.sql` is not on the local filesystem at all.
        metadata = read_metadata(node, metadata_path)
        # A rewrite that matches nothing is silent, so assert the enabled spelling exists before the
        # edit and the disabled spelling exists after it. Otherwise a SHOW CREATE formatting drift away
        # from "<setting> = 1" would leave the metadata untouched and re-attach it -- a false positive.
        assert f"{setting} = 1" in metadata, f"'{setting} = 1' not found in {table} metadata before rewrite"
        write_metadata(node, metadata_path, metadata.replace(f"{setting} = 1", f"{setting} = 0"))
        db_disk_name = get_database_disk_name(node)
        if db_disk_name != "default":
            # A plain-rewritable database disk caches metadata; reload it to see the change.
            node.query(f"SYSTEM CLEAR DISK METADATA CACHE {db_disk_name}")
        assert f"{setting} = 0" in read_metadata(node, metadata_path), f"'{setting} = 0' not present in {table} metadata after rewrite"
        node.query(f"ATTACH TABLE {table}")

    # (1) _part_offset projection: allow_part_offset_column_in_projections is CREATE-only, so a table
    # whose on-disk metadata has it disabled must still ATTACH (issue #102445).
    node.query("DROP TABLE IF EXISTS t_04545_po SYNC")
    node.query("CREATE TABLE t_04545_po (a UInt64, b UInt64, PROJECTION p (SELECT a, b, _part_offset ORDER BY b)) ENGINE = MergeTree ORDER BY a SETTINGS allow_part_offset_column_in_projections = 1")
    node.query("INSERT INTO t_04545_po VALUES (1, 1), (2, 2)")
    attach_with_gate_disabled("t_04545_po", "allow_part_offset_column_in_projections")
    assert node.query("SELECT count() FROM t_04545_po") == "2\n"

    # (2) commit-order projection: allow_commit_order_projection is CREATE-only -> ATTACH must succeed
    # even with the gate disabled on disk.
    node.query("DROP TABLE IF EXISTS t_04545_co SYNC")
    node.query(
        "CREATE TABLE t_04545_co (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    node.query("INSERT INTO t_04545_co(a) VALUES (1), (2)")
    attach_with_gate_disabled("t_04545_co", "allow_commit_order_projection")
    assert node.query("SELECT count() FROM t_04545_co") == "2\n"

    # (3) after the #102445 attach, an unrelated later ALTER (ADD COLUMN) must NOT be rejected: no new
    # projection is introduced and the already-off gate is untouched.
    node.query("ALTER TABLE t_04545_po ADD COLUMN c UInt64")

    # (4) an ALTER that flips a CREATE-only gate from enabled to disabled while a matching projection
    # already exists must be rejected (matches PR #104822's regression 04313): a table with such a
    # projection must not be able to turn the feature off.
    node.query("DROP TABLE IF EXISTS t_04545_flip_po SYNC")
    node.query(
        "CREATE TABLE t_04545_flip_po (a UInt64, b UInt64, PROJECTION p (SELECT a, b, _part_offset ORDER BY b)) ENGINE = MergeTree ORDER BY a SETTINGS allow_part_offset_column_in_projections = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_flip_po MODIFY SETTING allow_part_offset_column_in_projections = 0"), "MISSING part_offset flip rejection"

    node.query("DROP TABLE IF EXISTS t_04545_flip_co SYNC")
    node.query(
        "CREATE TABLE t_04545_flip_co (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_flip_co MODIFY SETTING allow_commit_order_projection = 0"), "MISSING commit_order flip rejection"

    # (5) enable_block_number_column / enable_block_offset_column are merge-time dependencies of a
    # commit-order projection, so disabling them via ALTER while such a projection exists must be
    # rejected up front (otherwise a later merge / MATERIALIZE PROJECTION rebuild runs without
    # materializing the required _block_number / _block_offset).
    node.query("DROP TABLE IF EXISTS t_04545_bn SYNC")
    node.query(
        "CREATE TABLE t_04545_bn (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_bn MODIFY SETTING enable_block_number_column = 0"), "MISSING block_number modify rejection"

    node.query("DROP TABLE IF EXISTS t_04545_bo SYNC")
    node.query(
        "CREATE TABLE t_04545_bo (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_bo MODIFY SETTING enable_block_offset_column = 0"), "MISSING block_offset modify rejection"

    # (6) RESET SETTING drops the key from the override list, so the effective value must fall back to
    # the DEFAULT (0 here). checkProperties validates against getDefaultSettings() + settings_changes,
    # so RESET of these merge-time settings while a commit-order projection exists is also rejected.
    node.query("DROP TABLE IF EXISTS t_04545_rbn SYNC")
    node.query(
        "CREATE TABLE t_04545_rbn (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_rbn RESET SETTING enable_block_number_column"), "MISSING block_number reset rejection"

    node.query("DROP TABLE IF EXISTS t_04545_rbo SYNC")
    node.query(
        "CREATE TABLE t_04545_rbo (a UInt64,"
        " PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset))"
        " ENGINE = MergeTree ORDER BY a"
        " SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 1"
    )
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_rbo RESET SETTING enable_block_offset_column"), "MISSING block_offset reset rejection"

    # (7) control: disabling enable_block_number_column on a table WITHOUT a commit-order projection
    # stays allowed (nothing depends on the column).
    node.query("DROP TABLE IF EXISTS t_04545_plain SYNC")
    node.query("CREATE TABLE t_04545_plain (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1")
    node.query("ALTER TABLE t_04545_plain MODIFY SETTING enable_block_number_column = 0")

    # (8) a mixed ALTER that both ADDs a commit-order projection and enables the gate must succeed: the
    # gate is validated against the effective post-ALTER settings, not the stale live value.
    node.query("DROP TABLE IF EXISTS t_04545_mix SYNC")
    node.query("CREATE TABLE t_04545_mix (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1")
    node.query("ALTER TABLE t_04545_mix ADD PROJECTION p (SELECT a, _block_number, _block_offset ORDER BY _block_number, _block_offset), MODIFY SETTING allow_commit_order_projection = 1")

    # (9) control: ADD PROJECTION introducing a commit-order projection while the gate is still off must
    # be rejected (the gate still fires for a projection introduced by the current operation).
    node.query("DROP TABLE IF EXISTS t_04545_addoff SYNC")
    node.query("CREATE TABLE t_04545_addoff (a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, allow_commit_order_projection = 0")
    assert "BAD_ARGUMENTS" in node.query_and_get_error("ALTER TABLE t_04545_addoff ADD PROJECTION p (SELECT a, _block_number ORDER BY _block_number)"), "MISSING add-projection gate-off rejection"

    for table in [
        "t_04545_po",
        "t_04545_co",
        "t_04545_flip_po",
        "t_04545_flip_co",
        "t_04545_bn",
        "t_04545_bo",
        "t_04545_rbn",
        "t_04545_rbo",
        "t_04545_plain",
        "t_04545_mix",
        "t_04545_addoff",
    ]:
        node.query(f"DROP TABLE IF EXISTS {table} SYNC")


