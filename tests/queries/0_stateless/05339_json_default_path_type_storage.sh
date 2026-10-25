#!/usr/bin/env bash
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Fails if `dpt_roundtrip` and `dpt_expected` differ in values or hashes,
# and empties `dpt_roundtrip` for the next format.
function check_roundtrip()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT throwIf(count() != 0, '$1 changed values') FROM (
            (SELECT id, j, sipHash64(j) FROM dpt_roundtrip EXCEPT ALL SELECT id, j, h FROM dpt_expected)
            UNION ALL
            (SELECT id, j, h FROM dpt_expected EXCEPT ALL SELECT id, j, sipHash64(j) FROM dpt_roundtrip)
        );
        TRUNCATE TABLE dpt_roundtrip;" > /dev/null
}

# Copies `dpt_storage` into `dpt_roundtrip` through the given format and checks the result.
function roundtrip()
{
    local format=$1
    shift
    $CLICKHOUSE_CLIENT "$@" --query "SELECT id, j FROM dpt_storage ORDER BY id FORMAT $format" |
        $CLICKHOUSE_CLIENT --query "INSERT INTO dpt_roundtrip FORMAT $format"
    check_roundtrip "$format"
}

# `run <type> <shared data version> <part layout> [formats]`: the format round-trips test the
# serialization of the column read back from storage, so they do not depend on the layout and
# run only where requested.
function run()
{
    local type=$1 version=$2 layout=$3 formats=${4:-}
    local bytes=1000000000 substreams=1
    if [ "$layout" = wide ]; then bytes=0; fi
    if [ "$layout" = compact_without_substreams ]; then substreams=0; fi
    # Different insertion orders select different runtime paths. All paths in rows 1 and 2 are
    # explicit, so moving them between runtime/shared storage must preserve comparison and hashes.
    $CLICKHOUSE_CLIENT --query "
        DROP TABLE IF EXISTS dpt_storage;
        DROP TABLE IF EXISTS dpt_expected;
        DROP TABLE IF EXISTS dpt_roundtrip;
        DROP TABLE IF EXISTS dpt_missing;
        CREATE TABLE dpt_storage (id UInt64, j JSON(max_dynamic_paths=1, DEFAULT PATH TYPE $type))
        ENGINE=MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part=$bytes, min_rows_for_wide_part=0,
            index_granularity=1, write_marks_for_substreams_in_compact_parts=$substreams, object_serialization_version='v3',
            object_shared_data_serialization_version='$version',
            object_shared_data_serialization_version_for_zero_level_parts='$version',
            object_shared_data_buckets_for_compact_part=2, object_shared_data_buckets_for_wide_part=2;
        SYSTEM STOP MERGES dpt_storage;
        INSERT INTO dpt_storage VALUES (1, '{\"a\":0,\"b\":2,\"n\":{\"c\":3}}');
        INSERT INTO dpt_storage VALUES (2, '{\"b\":2,\"n\":{\"c\":3},\"a\":0}');
        SELECT throwIf(uniqExact(sipHash64(j)) != 1, 'hash depends on runtime path selection') FROM dpt_storage;
        SELECT throwIf(uniqExact(j) != 1, 'distinct depends on runtime path selection') FROM dpt_storage;
        SELECT throwIf(count() != 1, 'grouping depends on runtime path selection') FROM (SELECT j FROM dpt_storage GROUP BY j)
            SETTINGS allow_suspicious_types_in_group_by=1;
        SELECT throwIf(countIf(toString(j.a) != '0' OR toString(j.b) != '2' OR toString(j.n.c) != '3') != 0) FROM dpt_storage;
        SELECT throwIf(countIf(toString(j.^n) != '{\"c\":3}' AND toString(j.^n) != '{\"c\":\"3\"}') != 0) FROM dpt_storage;
        -- Missing vs explicit default: path a is missing in id=3 and an explicit default(T) in id=4;
        -- moving paths between runtime and shared storage through merges must not densify the missing
        -- path (no a in output, no path in enumeration, unchanged hash/comparison).
        INSERT INTO dpt_storage VALUES (3, '{\"b\":1}'), (4, '{\"a\":0,\"b\":1}');
        CREATE TABLE dpt_missing ENGINE=Memory AS SELECT id, j, sipHash64(j) AS h FROM dpt_storage WHERE id IN (3, 4);
        SELECT throwIf(countIf(has(JSONAllPaths(j), 'a') != (id = 4)) != 0, 'missing path leaked into output') FROM dpt_storage WHERE id IN (3, 4);
        SELECT throwIf(countIf(s.j != e.j OR sipHash64(s.j) != e.h) != 0, 'missing path changed hash')
            FROM dpt_storage AS s JOIN dpt_missing AS e ON s.id = e.id;
        SYSTEM START MERGES dpt_storage;
        OPTIMIZE TABLE dpt_storage FINAL;
        SELECT throwIf(countIf(has(JSONAllPaths(j), 'a') != (id = 4)) != 0, 'merge densified missing path') FROM dpt_storage WHERE id IN (3, 4);
        SELECT throwIf(countIf(s.j != e.j OR sipHash64(s.j) != e.h) != 0, 'merge changed missing vs default')
            FROM dpt_storage AS s JOIN dpt_missing AS e ON s.id = e.id;
        SELECT throwIf(countIf(s.j.a != e.j.a) != 0, 'extraction of missing path changed')
            FROM dpt_storage AS s JOIN dpt_missing AS e ON s.id = e.id;
        CREATE TABLE dpt_expected ENGINE=Memory AS SELECT id, j, sipHash64(j) AS h FROM dpt_storage;
        CREATE TABLE dpt_roundtrip (id UInt64, j JSON(max_dynamic_paths=1, DEFAULT PATH TYPE $type)) ENGINE=Memory;
    " > /dev/null

    if [ -n "$formats" ]; then
        roundtrip Native --output_format_native_use_flattened_dynamic_and_json_serialization=0
        roundtrip Native --output_format_native_use_flattened_dynamic_and_json_serialization=1
        # Individual-value binary serialization.
        roundtrip RowBinary
        # Binary type encoding (JSON encoding version 1 with DEFAULT PATH TYPE): the receiver
        # reconstructs the column type from the stream.
        roundtrip RowBinaryWithNamesAndTypes
    fi

    $CLICKHOUSE_CLIENT --query "
        ALTER TABLE dpt_storage MODIFY COLUMN j JSON(max_dynamic_paths=1, DEFAULT PATH TYPE String) SETTINGS mutations_sync=2;
        SELECT throwIf(countIf(j.a != '0' OR j.b != '2' OR j.n.c != '3') != 0, 'ALTER changed values') FROM dpt_storage WHERE id < 3;
        DROP TABLE dpt_storage;
        DROP TABLE dpt_expected;
        DROP TABLE dpt_roundtrip;
        DROP TABLE dpt_missing;
    " > /dev/null
}

# Every shared data version and every part layout, without the full cross product.
run Int64 map compact_without_substreams
run Int64 map_with_buckets wide
run Int64 advanced compact
run Int64 advanced wide formats
run String advanced wide formats

# A duplicated dynamic path in the binary format must not append a second `Variant(T)` entry,
# which would make the runtime-path column longer than the enclosing JSON column. Default:
# reject; with type_json_skip_duplicated_paths=1 keep the first value.
function dup_row()
{
    printf '\x02\x01a\x08\x01\x00\x00\x00\x00\x00\x00\x00\x01a\x08\x02\x00\x00\x00\x00\x00\x00\x00'
}
$CLICKHOUSE_CLIENT --query "DROP TABLE IF EXISTS dpt_dup"
$CLICKHOUSE_CLIENT --query "CREATE TABLE dpt_dup (x JSON(DEFAULT PATH TYPE Int64)) ENGINE=Memory"
dup_row | $CLICKHOUSE_CLIENT --query "INSERT INTO dpt_dup FORMAT RowBinary" 2>&1 | grep -o -m1 'Found duplicated path' || :
dup_row | $CLICKHOUSE_CLIENT --type_json_skip_duplicated_paths 1 --query "INSERT INTO dpt_dup FORMAT RowBinary"
$CLICKHOUSE_CLIENT --query "SELECT x FROM dpt_dup"
$CLICKHOUSE_CLIENT --query "DROP TABLE dpt_dup"

# A duplicated typed path is skipped as well: its value has to be consumed, otherwise the next
# path is read from the middle of it. The row is `a` = 1, `a` = 2, `b` = 'x' (as `Dynamic`, then as a `T` blob).
printf '\x03\x01a\x01\x01a\x02\x01b\x15\x01x' > "${CLICKHOUSE_TMP}/dpt_dup_typed.bin"
$CLICKHOUSE_LOCAL --type_json_skip_duplicated_paths 1 --query "SELECT x FROM file('${CLICKHOUSE_TMP}/dpt_dup_typed.bin', RowBinary, 'x JSON(a UInt8)')"
printf '\x03\x01a\x01\x01a\x02\x01b\x02\x01x' > "${CLICKHOUSE_TMP}/dpt_dup_typed.bin"
$CLICKHOUSE_LOCAL --type_json_skip_duplicated_paths 1 --query "SELECT x FROM file('${CLICKHOUSE_TMP}/dpt_dup_typed.bin', RowBinary, 'x JSON(DEFAULT PATH TYPE String, a UInt8)')"
