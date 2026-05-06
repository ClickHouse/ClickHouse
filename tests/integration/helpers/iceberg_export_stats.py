"""Shared helpers for verifying Iceberg per-file column statistics produced by
``EXPORT PART`` / ``EXPORT PARTITION``.

Both the MergeTree and ReplicatedMergeTree export test modules drive the same
schema and expected stats shape (see ``assert_exported_stats``), so the
assertions, the manifest-entry reader, and the small byte/int decoders live
here instead of being duplicated in each test module.

The only ClickHouse-side prerequisite is that ``system.iceberg_metadata_log``
is enabled on the node: point the test cluster at
``configs/config.d/metadata_log.xml`` (shipped next to each test) and run the
probing SELECT with ``SETTINGS iceberg_metadata_log_level = 'manifest_file_entry'``.
"""

import json

from helpers.iceberg_utils import get_bound_for_column


# Iceberg assigns field ids positionally (starting at 1) to the non-partition
# columns in declaration order; partition-source columns share the same ids and
# partition transform outputs live in a separate 1000+ namespace.  For the
# schema used by the two export-stats tests (id Int32, name String,
# tag Nullable(String), year Int32) this yields the mapping below.
STATS_FIELD_IDS = {"id": 1, "name": 2, "tag": 3}


def decode_int_bound(raw):
    """Decode a JSON-serialized Iceberg integer bound (little-endian signed bytes).

    ClickHouse's Iceberg writer currently dumps integer bounds using the
    underlying ``Field`` storage width (8 bytes for Int32/Int64/Date/...).  Some
    writers produce the spec-correct 4-byte Int32 encoding.  Accept both.
    """
    data = raw.encode("latin-1")
    assert len(data) in (4, 8), f"Unexpected bound width {len(data)}: {raw!r}"
    return int.from_bytes(data, "little", signed=True)


def get_int_for_column(m, column_id):
    """Look up a value for ``column_id`` in an Iceberg integer map serialized as
    either a dict ``{str(column_id): value}`` or a list of ``{key, value}`` records.

    Unlike :func:`helpers.iceberg_utils.get_bound_for_column`, this helper does
    not try to unescape the value, so it works for numeric columns like
    ``column_sizes`` and ``null_value_counts`` where the raw value is an int
    (or a quoted int64 string).
    """
    if m is None:
        return None
    value = None
    if isinstance(m, dict):
        value = m.get(str(column_id))
    elif isinstance(m, list):
        for item in m:
            if isinstance(item, dict) and item.get("key") == column_id:
                value = item.get("value")
                break
    if value is None:
        return None
    return int(value) if isinstance(value, str) else value


def fetch_manifest_entries(node, query_id):
    """Read JSON manifest-file entries emitted for ``query_id`` into
    ``system.iceberg_metadata_log``.

    The outer ``FORMAT JSONEachRow`` is required: the default TSV format
    escapes backslashes in the ``content`` string, which would double-encode
    the inner ``\\uXXXX`` sequences coming from Iceberg bytes-bounds.
    """
    node.query("SYSTEM FLUSH LOGS")
    raw = node.query(
        f"""
        SELECT DISTINCT content
        FROM system.iceberg_metadata_log
        WHERE query_id = '{query_id}'
          AND content_type = 'ManifestFileEntry'
          AND content != ''
        FORMAT JSONEachRow
        """
    )
    entries = []
    for line in raw.strip().split("\n"):
        if not line:
            continue
        outer = json.loads(line)
        content = outer.get("content")
        if content:
            entries.append(json.loads(content))
    return entries


def assert_exported_stats(entries):
    """Assert that at least one manifest entry describes the exported 2020 data file.

    Expected shape (three rows, one NULL in ``tag``):

    * ``record_count        = 3``
    * ``file_size_in_bytes  > 0``
    * ``column_sizes[id|name|tag] > 0``
    * ``null_value_counts   = {id: 0, name: 0, tag: 1}``
    * ``lower_bounds        = {id: 1, name: "aaa", tag: "x"}``
    * ``upper_bounds        = {id: 3, name: "zzz", tag: "y"}``
    """
    assert entries, "No ManifestFileEntry rows recorded in system.iceberg_metadata_log"

    id_fid = STATS_FIELD_IDS["id"]
    name_fid = STATS_FIELD_IDS["name"]
    tag_fid = STATS_FIELD_IDS["tag"]

    matched = False
    for entry in entries:
        data_file = entry.get("data_file") or {}
        # Skip manifest entries that explicitly mark themselves as deletes; data
        # entries either omit `content` (v1 manifest) or set it to 0.
        if data_file.get("content", 0) not in (0, None):
            continue

        record_count = data_file.get("record_count")
        if record_count != 3:
            continue

        file_size = data_file.get("file_size_in_bytes")
        assert file_size and file_size > 0, (
            f"Expected positive file_size_in_bytes, got {file_size!r}"
        )

        for field in ("id", "name", "tag"):
            fid = STATS_FIELD_IDS[field]
            size = get_int_for_column(data_file.get("column_sizes"), fid)
            assert size is not None, (
                f"column_sizes missing entry for field_id={fid} ({field})"
            )
            assert size > 0, f"column_sizes[{field}] expected > 0, got {size!r}"

        null_counts = data_file.get("null_value_counts")
        assert get_int_for_column(null_counts, id_fid) == 0, (
            f"Expected 0 nulls in id, got null_value_counts={null_counts!r}"
        )
        assert get_int_for_column(null_counts, name_fid) == 0, (
            f"Expected 0 nulls in name, got null_value_counts={null_counts!r}"
        )
        assert get_int_for_column(null_counts, tag_fid) == 1, (
            f"Expected 1 null in tag (one NULL was inserted), "
            f"got null_value_counts={null_counts!r}"
        )

        lower = data_file.get("lower_bounds")
        upper = data_file.get("upper_bounds")

        assert decode_int_bound(get_bound_for_column(lower, id_fid)) == 1, (
            f"lower_bounds[id] expected 1, got {get_bound_for_column(lower, id_fid)!r}"
        )
        assert decode_int_bound(get_bound_for_column(upper, id_fid)) == 3, (
            f"upper_bounds[id] expected 3, got {get_bound_for_column(upper, id_fid)!r}"
        )

        assert get_bound_for_column(lower, name_fid) == "aaa", (
            f"lower_bounds[name] expected 'aaa', got {get_bound_for_column(lower, name_fid)!r}"
        )
        assert get_bound_for_column(upper, name_fid) == "zzz", (
            f"upper_bounds[name] expected 'zzz', got {get_bound_for_column(upper, name_fid)!r}"
        )

        assert get_bound_for_column(lower, tag_fid) == "x", (
            f"lower_bounds[tag] expected 'x' (nulls are skipped), got {get_bound_for_column(lower, tag_fid)!r}"
        )
        assert get_bound_for_column(upper, tag_fid) == "y", (
            f"upper_bounds[tag] expected 'y' (nulls are skipped), got {get_bound_for_column(upper, tag_fid)!r}"
        )

        matched = True
        break

    assert matched, (
        f"No data-file manifest entry with record_count=3 was found.  "
        f"Parsed {len(entries)} entr(y|ies) but none matched."
    )
