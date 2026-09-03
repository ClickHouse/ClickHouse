import glob
import json
import os

from pyiceberg.table import StaticTable

from helpers.iceberg_utils import get_uuid_str, iceberg_local_interop_dir

# Same per-xdist-worker path conftest uses (parallel-safe under --dist=each).
ICEBERG_DIR_NODE1 = iceberg_local_interop_dir("node1")


def newest_metadata_file(table_dir):
    """Newest `vN.metadata.json` of a table, by `N`.

    The documents are numbered in write order, and two of them can carry the same
    `last-updated-ms`, so the number decides rather than the timestamp. `N` is not
    zero padded, so the name itself does not sort.
    """
    candidates = glob.glob(os.path.join(table_dir, "metadata", "v*.metadata.json"))
    assert candidates, f"no metadata file under {table_dir}"

    def version(path):
        return int(os.path.basename(path).split(".", 1)[0][1:])

    return max(candidates, key=version)


def table_location(table_dir):
    with open(newest_metadata_file(table_dir)) as metadata:
        return json.load(metadata)["location"]


def write_local_table(node, table_name, table_dir, full_path):
    """Create an `IcebergLocal` table and insert two rows through ClickHouse.

    The setting is passed on the CREATE alone: it is read when the table is
    created, and the INSERT then carries that choice into the manifests.
    """
    node.query(
        f"""
        CREATE TABLE {table_name} (id UInt64, v Int64)
        ENGINE=IcebergLocal(local, path = '{table_dir}', format=Parquet)
        ORDER BY (id)
        """,
        settings={
            "allow_insert_into_iceberg": 1,
            "write_full_path_in_iceberg_metadata": full_path,
        },
    )
    node.query(
        f"INSERT INTO {table_name} VALUES (1, 10), (2, 20)",
        settings={"allow_insert_into_iceberg": 1},
    )
    assert int(node.query(f"SELECT count() FROM {table_name}")) == 2


def scan_with_pyiceberg(table_dir):
    arrow = StaticTable.from_metadata(newest_metadata_file(table_dir)).scan().to_arrow()
    return sorted(zip(arrow.column("id").to_pylist(), arrow.column("v").to_pylist()))


def test_ch_write_full_path_pyiceberg_read(started_cluster_iceberg):
    """
    ClickHouse writes an `IcebergLocal` table in full-path mode, pyiceberg scans it.

    Regression for issue #102321: `write_full_path_in_iceberg_metadata` stamped the
    table with a `local://` scheme and an empty authority in front of an already
    absolute path, so every path in the metadata read `local:////abs/path` and
    pyiceberg 0.11.1 refused the table with `ValueError: Unrecognized filesystem
    type in URI: local`.
    """
    node1 = started_cluster_iceberg.instances["node1"]
    suffix = get_uuid_str()

    on_name = "test_full_path_on_" + suffix
    on_dir = f"{ICEBERG_DIR_NODE1}/default/{on_name}"
    write_local_table(node1, on_name, on_dir, 1)

    # external_dirs mounts the container path at the same absolute path on the host,
    # so pyiceberg opens the very table ClickHouse just wrote. The scan resolves
    # `manifest-list`, `manifest_path` and `data_file.file_path` in turn, so it fails
    # on any carrier naming an unresolvable URI, not only on the declared location.
    assert scan_with_pyiceberg(on_dir) == [(1, 10), (2, 20)]

    # An empty authority contributes no path segment, so an absolute path keeps
    # exactly one root slash: this spelling excludes the `file:////` form as well
    # as the `local://` one. It is asserted after the scan so that a reader the URI
    # defeats is reported as a read failure rather than as a string mismatch.
    assert table_location(on_dir).startswith(f"file://{on_dir}"), table_location(on_dir)

    # Control: the default mode writes the same absolute path with no scheme at all,
    # and pyiceberg reads that too. It is what makes the arm above a statement about
    # the scheme this setting adds rather than about the reader.
    off_name = "test_full_path_off_" + suffix
    off_dir = f"{ICEBERG_DIR_NODE1}/default/{off_name}"
    write_local_table(node1, off_name, off_dir, 0)

    assert "://" not in table_location(off_dir), table_location(off_dir)
    assert scan_with_pyiceberg(off_dir) == [(1, 10), (2, 20)]
