#!/usr/bin/env bash
# Tags: no-parallel, long
set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

for type in Int64 String; do
    for version in map map_with_buckets advanced; do
        for layout in compact wide compact_without_substreams; do
            bytes=1000000000
            substreams=1
            if [ "$layout" = wide ]; then bytes=0; fi
            if [ "$layout" = compact_without_substreams ]; then substreams=0; fi
            # Different insertion orders select different runtime paths. All paths in these
            # rows are explicit, so moving them between runtime/shared storage must preserve
            # comparison and hashes (unlike densifying a genuinely missing non-nullable path).
            $CLICKHOUSE_CLIENT --multiquery --query "
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
                CREATE TABLE dpt_expected ENGINE=Memory AS SELECT id, j, sipHash64(j) AS h FROM dpt_storage;
                SELECT throwIf(uniqExact(h) != 1, 'hash depends on runtime path selection') FROM dpt_expected;
                SELECT throwIf(uniqExact(j) != 1, 'distinct depends on runtime path selection') FROM dpt_storage;
                SELECT throwIf(count() != 1, 'grouping depends on runtime path selection') FROM (SELECT j FROM dpt_storage GROUP BY j)
                    SETTINGS allow_suspicious_types_in_group_by=1;
                SELECT throwIf(countIf(toString(j.a) != '0' OR toString(j.b) != '2' OR toString(j.n.c) != '3') != 0) FROM dpt_storage;
                SELECT throwIf(countIf(toString(j.^n) != '{\"c\":3}' AND toString(j.^n) != '{\"c\":\"3\"}') != 0) FROM dpt_storage;
                -- Missing vs explicit default: path a is missing in id=3 and an explicit
                -- default(T) in id=4; moving paths between runtime and shared storage through
                -- merges must not densify the missing path (no a in output, no path in
                -- enumeration, unchanged hash/comparison).
                INSERT INTO dpt_storage VALUES (3, '{\"b\":1}'), (4, '{\"a\":0,\"b\":1}');
                CREATE TABLE dpt_missing ENGINE=Memory AS SELECT id, j, sipHash64(j) AS h FROM dpt_storage WHERE id IN (3, 4);
                SELECT throwIf(countIf(has(JSONAllPaths(j), 'a') != (id = 4)) != 0, 'missing path leaked into output') FROM dpt_storage WHERE id IN (3, 4);
                SELECT throwIf(countIf(s.j != e.j OR sipHash64(s.j) != e.h) != 0, 'missing path changed hash')
                    FROM dpt_storage AS s JOIN dpt_missing AS e ON s.id = e.id WHERE s.id IN (3, 4);
                SYSTEM START MERGES dpt_storage;
                OPTIMIZE TABLE dpt_storage FINAL;
                SELECT throwIf(countIf(has(JSONAllPaths(s.j), 'a') != (s.id = 4)) != 0, 'merge densified missing path') FROM dpt_storage AS s WHERE s.id IN (3, 4);
                SELECT throwIf(countIf(s.j != e.j OR sipHash64(s.j) != e.h) != 0, 'merge changed missing vs default')
                    FROM dpt_storage AS s JOIN dpt_missing AS e ON s.id = e.id WHERE s.id IN (3, 4);
                SELECT throwIf(countIf(s.j.a != e.a_v) != 0, 'extraction of missing path changed')
                    FROM dpt_storage AS s JOIN (SELECT id, j.a AS a_v FROM dpt_missing) AS e ON s.id = e.id WHERE s.id IN (3, 4);
                CREATE TABLE dpt_roundtrip (id UInt64, j JSON(max_dynamic_paths=1, DEFAULT PATH TYPE $type)) ENGINE=Memory;
            " > /dev/null

            # dpt_expected currently holds only the initial (id=1,2) rows for non-nullable types.
            # Rebuild it from the full storage so the round-trip comparison below covers all rows.
            $CLICKHOUSE_CLIENT --multiquery --query "
                TRUNCATE TABLE dpt_expected;
                INSERT INTO dpt_expected SELECT id, j, sipHash64(j) FROM dpt_storage;
            " > /dev/null

            for flattened in 0 1; do
                $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE dpt_roundtrip"
                $CLICKHOUSE_CLIENT --query "SELECT id, j FROM dpt_storage ORDER BY id SETTINGS output_format_native_use_flattened_dynamic_and_json_serialization=$flattened FORMAT Native" |
                    $CLICKHOUSE_CLIENT --query "INSERT INTO dpt_roundtrip FORMAT Native"
                $CLICKHOUSE_CLIENT --multiquery --query "
                    SELECT throwIf(count() != 0, 'Native changed values') FROM (
                        SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                        EXCEPT ALL
                        SELECT id, j, h FROM dpt_expected ORDER BY id, j
                    );
                    SELECT throwIf(count() != 0, 'Native changed values') FROM (
                        SELECT id, j, h FROM dpt_expected ORDER BY id, j
                        EXCEPT ALL
                        SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                    );
                " > /dev/null
            done
            $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE dpt_roundtrip"
            $CLICKHOUSE_CLIENT --query "SELECT id, j FROM dpt_storage ORDER BY id FORMAT RowBinary" |
                $CLICKHOUSE_CLIENT --query "INSERT INTO dpt_roundtrip FORMAT RowBinary"
            $CLICKHOUSE_CLIENT --multiquery --query "
                SELECT throwIf(count() != 0, 'RowBinary changed values') FROM (
                    SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                    EXCEPT ALL
                    SELECT id, j, h FROM dpt_expected ORDER BY id, j
                );
                SELECT throwIf(count() != 0, 'RowBinary changed values') FROM (
                    SELECT id, j, h FROM dpt_expected ORDER BY id, j
                    EXCEPT ALL
                    SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                );
            " > /dev/null
            # RowBinaryWithNamesAndTypes exercises the binary type encoding (JSON version 1
            # with DEFAULT PATH TYPE): the receiver reconstructs the column type from the
            # stream via input_format_binary_decode_types_in_binary_format.
            $CLICKHOUSE_CLIENT --query "TRUNCATE TABLE dpt_roundtrip"
            $CLICKHOUSE_CLIENT --query "SELECT id, j FROM dpt_storage ORDER BY id FORMAT RowBinaryWithNamesAndTypes" |
                $CLICKHOUSE_CLIENT --query "INSERT INTO dpt_roundtrip FORMAT RowBinaryWithNamesAndTypes"
            $CLICKHOUSE_CLIENT --multiquery --query "
                SELECT throwIf(count() != 0, 'RowBinaryWithNamesAndTypes changed values') FROM (
                    SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                    EXCEPT ALL
                    SELECT id, j, h FROM dpt_expected ORDER BY id, j
                );
                SELECT throwIf(count() != 0, 'RowBinaryWithNamesAndTypes changed values') FROM (
                    SELECT id, j, h FROM dpt_expected ORDER BY id, j
                    EXCEPT ALL
                    SELECT id, j, sipHash64(j) FROM dpt_roundtrip ORDER BY id, j
                );
                ALTER TABLE dpt_storage MODIFY COLUMN j JSON(max_dynamic_paths=1, DEFAULT PATH TYPE String) SETTINGS mutations_sync=2;
                SELECT throwIf(countIf(j.a != '0' OR j.b != '2' OR j.n.c != '3') != 0, 'ALTER changed values') FROM dpt_storage WHERE id < 3;
                DROP TABLE dpt_storage;
                DROP TABLE dpt_expected;
                DROP TABLE dpt_roundtrip;
            " > /dev/null
        done
    done
done
printf 'OK\n'
