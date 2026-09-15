from helpers.export_partition_helpers import (
    make_iceberg_s3,
    make_source,
    unique_suffix,
    wait_for_export_status,
)
from helpers.iceberg_export_stats import (
    assert_exported_stats,
    fetch_manifest_entries,
)

from .common import setup_tables

CLUSTER_INSTANCES = ["replica1"]

# The happy paths of `EXPORT PARTITION` into an Iceberg destination: one partition, several
# partitions, all of them, and the column statistics carried by the resulting manifest entry.


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_export_partition_to_iceberg(cluster, source_engine):
    """
    Basic happy path: export a single partition and verify row count and content.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "1\t2020\n2\t2020\n3\t2020", (
        f"Unexpected data in Iceberg table:\n{result}"
    )


def test_export_two_partitions_to_iceberg(cluster, source_engine):
    """
    Export two partitions in a single ALTER TABLE statement and verify that both
    land in the Iceberg table with correct row counts.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(
        f"""
        ALTER TABLE {mt_table}
            EXPORT PARTITION ID '2020' TO TABLE {iceberg_table},
            EXPORT PARTITION ID '2021' TO TABLE {iceberg_table}
        """,
        settings={"allow_insert_into_iceberg": 1},
    )

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, iceberg_table, "2021", "COMPLETED")

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2021").strip())

    assert count_2020 == 3, f"Expected 3 rows for year=2020, got {count_2020}"
    assert count_2021 == 1, f"Expected 1 row for year=2021, got {count_2021}"


def test_export_partition_all_to_iceberg(cluster, source_engine):
    """
    `ALTER TABLE ... EXPORT PARTITION ALL TO TABLE ...` schedules every active partition
    in one statement and exercises the Iceberg-specific destination compatibility checks
    (which are repeated per sub-call inside the loop).
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_{uid}"
    iceberg_table = f"iceberg_{uid}"

    setup_tables(cluster, mt_table, iceberg_table, nodes=["replica1"], engine=source_engine)

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ALL TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )

    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")
    wait_for_export_status(node, mt_table, iceberg_table, "2021", "COMPLETED")

    count_2020 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2020").strip())
    count_2021 = int(node.query(f"SELECT count() FROM {iceberg_table} WHERE year = 2021").strip())

    assert count_2020 == 3, f"Expected 3 rows for year=2020, got {count_2020}"
    assert count_2021 == 1, f"Expected 1 row for year=2021, got {count_2021}"


def setup_deleted_rows_tables(node, mt_table: str, iceberg_table: str, engine: str):
    """Source holding partition 2020 in two separate parts, plus the Iceberg destination.

    Merges are disabled because a merge applies the deleted mask: it would rewrite the parts
    below into one without the deleted rows, removing the case these tests are about.
    """
    make_source(
        node, mt_table, "id Int64, year Int32", "year",
        engine=engine, replica_name="replica1",
        extra_settings="max_bytes_to_merge_at_max_space_in_pool = 1",
    )

    node.query(f"INSERT INTO {mt_table} VALUES (1, 2020), (2, 2020)")
    node.query(f"INSERT INTO {mt_table} VALUES (3, 2020)")

    make_iceberg_s3(node, iceberg_table, "id Int64, year Int32", partition_by="year")


def test_export_partition_with_a_fully_deleted_part(cluster, source_engine):
    """
    A part whose rows were all removed by a lightweight delete exports successfully without
    writing a data file. The export must still commit, carrying the files the other parts
    produced.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_deleted_part_{uid}"
    iceberg_table = f"iceberg_deleted_part_{uid}"

    setup_deleted_rows_tables(node, mt_table, iceberg_table, source_engine)

    node.query(f"DELETE FROM {mt_table} WHERE id IN (1, 2)", settings={"mutations_sync": 2})

    active_parts = node.query(
        f"SELECT count() FROM system.parts WHERE table = '{mt_table}' AND active"
    ).strip()
    assert active_parts == "2", (
        f"Expected both inserted parts to survive the delete, got {active_parts}"
    )

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    result = node.query(f"SELECT id, year FROM {iceberg_table} ORDER BY id").strip()
    assert result == "3\t2020", f"Unexpected data in Iceberg table:\n{result}"


def test_export_partition_where_every_row_is_deleted(cluster, source_engine):
    """
    When no part of the partition has a surviving row the export produces no files at all.
    An empty export is not corrupted state: the task must reach COMPLETED and leave the
    destination untouched.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_deleted_all_{uid}"
    iceberg_table = f"iceberg_deleted_all_{uid}"

    setup_deleted_rows_tables(node, mt_table, iceberg_table, source_engine)

    node.query(f"DELETE FROM {mt_table} WHERE year = 2020", settings={"mutations_sync": 2})

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 0, f"Expected the Iceberg table to stay empty, got {count} rows"


def setup_stats_tables(node, mt_table: str, iceberg_table: str, engine: str = "ReplicatedMergeTree"):
    """Local variant of setup_tables using the wider schema with a Nullable column."""
    columns = "id Int32, name String, tag Nullable(String), year Int32"

    make_source(
        node, mt_table, columns, "year",
        order_by="id", replica_name="replica1",
engine=engine)
    node.query(
        f"""
        INSERT INTO {mt_table} (id, name, tag, year) VALUES
            (1, 'aaa', 'x',  2020),
            (2, 'mmm', NULL, 2020),
            (3, 'zzz', 'y',  2020),
            (4, 'kkk', 'z',  2021)
        """
    )

    make_iceberg_s3(node, iceberg_table, columns, partition_by="year")


def test_export_partition_writes_column_statistics(cluster, source_engine):
    """
    Export a whole partition (EXPORT PARTITION ID '2020') that contains one NULL
    and verify that the resulting Iceberg manifest entry carries accurate per-file
    column statistics: record_count, file_size_in_bytes, column_sizes,
    null_value_counts, and lower/upper bounds.
    """
    node = cluster.instances["replica1"]

    uid = unique_suffix()
    mt_table = f"mt_stats_{uid}"
    iceberg_table = f"iceberg_stats_{uid}"

    setup_stats_tables(node, mt_table, iceberg_table, engine=source_engine)

    node.query(
        f"ALTER TABLE {mt_table} EXPORT PARTITION ID '2020' TO TABLE {iceberg_table}",
        settings={"allow_insert_into_iceberg": 1},
    )
    wait_for_export_status(node, mt_table, iceberg_table, "2020", "COMPLETED")

    count = int(node.query(f"SELECT count() FROM {iceberg_table}").strip())
    assert count == 3, f"Expected 3 rows in Iceberg table after export, got {count}"

    query_id = f"stats_partition_{uid}"
    node.query(
        f"SELECT * FROM {iceberg_table} ORDER BY id",
        query_id=query_id,
        settings={"iceberg_metadata_log_level": "manifest_file_entry"},
    )

    entries = fetch_manifest_entries(node, query_id)
    assert_exported_stats(entries)
