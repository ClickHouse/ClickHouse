#!/usr/bin/env bash
# Tags: no-fasttest, no-msan, no-tsan

set -euo pipefail
CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

query() { ${CLICKHOUSE_CLIENT} --query "$1"; }

# The first blocks have sparse typed paths; later blocks/granules are dense.
# `Nullable` zero and `Dynamic` zero are present values, while `UInt64` zero is a default.
# Dynamic paths retain compact `Variant` encoding even alongside sparse typed paths.
query "DROP TABLE IF EXISTS json_sparse_source; CREATE TABLE json_sparse_source
(id UInt64, j JSON(t UInt64, n Nullable(Int64), z UInt8, half UInt8, nest.v UInt64, s String)) ENGINE=Memory;
INSERT INTO json_sparse_source SELECT number,
concat('{\"always\":0,\"half\":', toString(number % 2),
       if(number % 128 = 0 OR number >= 3072,
          ',\"t\":7,\"n\":0,\"d\":0,\"nest\":{\"v\":9,\"d\":0},\"s\":\"abc\"', ''), '}')
FROM numbers(4096);"

check_reads() {
    # Check every read stage, including mixed versions, merges and mutations.
    query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0
                       AND (startsWith(x, 'j.d.') OR startsWith(x, 'j.always.') OR startsWith(x, 'j.nest.d.')),
                       substreams)) != 0)
           FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                AND active AND column='j'" >/dev/null
    for block_size in 1000 1; do
        # Whole JSON, direct paths, .null, and a subobject share substream caches.
        query "SELECT throwIf(countIf(toString(a.j) != toString(b.j)
                   OR a.j.t != b.j.t OR a.j.n.null != b.j.n.null
                   OR toString(a.j.d) != toString(b.j.d)
                   OR toString(a.j.^nest) != toString(b.j.^nest)) != 0)
               FROM json_sparse_test a INNER JOIN json_sparse_source b USING id
               SETTINGS max_threads=1, max_block_size=$block_size, join_algorithm='hash',
                        optimize_functions_to_subcolumns=1" >/dev/null
        # Only subcolumns, including Dynamic's type-specific null map. Exercise
        # nonzero offsets and partial granules through PREWHERE and LIMIT/OFFSET.
        query "SELECT throwIf(countIf(t != if(id % 128 = 0 OR id >= 3072, 7, 0)
                   OR nnull != NOT (id % 128 = 0 OR id >= 3072)
                   OR dnull != nnull OR z != 0 OR half != id % 2 OR always != 0) != 0)
               FROM (SELECT id, j.t AS t, j.n.null AS nnull, j.d.:Int64.null AS dnull,
                            j.z AS z, j.half AS half, j.always.:Int64 AS always
                     FROM json_sparse_test PREWHERE id >= 3 AND id < 4000
                     ORDER BY id LIMIT 2701 OFFSET 5)
               SETTINGS max_threads=1, max_block_size=$block_size" >/dev/null
        query "SELECT throwIf(sum(j.n.null) != 3048), throwIf(countIf(isNotNull(j.d)) != 1048),
                      throwIf(sum(j.t) != 7336), throwIf(sum(j.n) != 0)
               FROM json_sparse_test SETTINGS max_threads=1, max_block_size=$block_size" >/dev/null
    done
}

for part in Wide Compact; do
    if [[ $part == Wide ]]; then wide_limit=0; else wide_limit=1000000000; fi
    for ratio in 0.9375 1 0; do
        query "DROP TABLE IF EXISTS json_sparse_test;
               CREATE TABLE json_sparse_test AS json_sparse_source ENGINE=MergeTree ORDER BY id
               SETTINGS min_bytes_for_wide_part=$wide_limit, min_rows_for_wide_part=$wide_limit,
                        index_granularity=64, index_granularity_bytes=1048576,
                        write_marks_for_substreams_in_compact_parts=1,
                        ratio_of_defaults_for_sparse_serialization=$ratio,
                        object_serialization_version='v4',
                        object_shared_data_serialization_version='map',
                        object_shared_data_serialization_version_for_zero_level_parts='map',
                        merge_max_block_size=128;
               SYSTEM STOP MERGES json_sparse_test;"
        for start in 0 512 1024 1536 2048 2560 3072 3584; do
            query "INSERT INTO json_sparse_test SELECT * FROM json_sparse_source
                   WHERE id >= $start AND id < $start + 512 ORDER BY id
                   SETTINGS max_threads=1"
        done
        query "SELECT throwIf(countIf(part_type != '$part') != 0)
               FROM system.parts WHERE database=currentDatabase() AND table='json_sparse_test' AND active" >/dev/null
        # Inspect real per-part stream metadata, not the type's default serialization.
        if [[ $ratio == 1 ]]; then sparse_condition="= 0"; else sparse_condition="> 0"; fi
        if [[ $ratio == 0 ]]; then half_sparse_parts=8; else half_sparse_parts=0; fi
        query "SELECT throwIf(NOT (countIf(arrayExists(x -> position(x, 'sparse.idx') > 0, substreams)) $sparse_condition)),
                      throwIf(countIf(has(substreams, 'j.half.sparse.idx')) != $half_sparse_parts),
                      throwIf(countIf(has(substreams, 'j.always.sparse.idx')) != 0),
                      throwIf(countIf(has(substreams, 'j.d.sparse.idx')) != 0)
               FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                    AND active AND column='j'" >/dev/null
        if [[ $ratio != 1 ]]; then
            query "SELECT throwIf(countIf(has(substreams, 'j.t.sparse.idx')) != 6),
                          throwIf(countIf(has(substreams, 'j.n.sparse.idx')) != 6),
                          throwIf(countIf(has(substreams, 'j.d.sparse.idx')) != 0),
                          throwIf(countIf(has(substreams, 'j.z.sparse.idx')) != 8)
                   FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                        AND active AND column='j'" >/dev/null
        fi
        # Reader behavior must not depend on the current writer version or threshold.
        query "ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='v3',
                   ratio_of_defaults_for_sparse_serialization=1"
        check_reads
        query "ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='v4',
                   ratio_of_defaults_for_sparse_serialization=$ratio;
               SYSTEM START MERGES json_sparse_test; OPTIMIZE TABLE json_sparse_test FINAL"
        check_reads
        query "CHECK TABLE json_sparse_test SETTINGS check_query_single_value_result=1" | grep -qx 1
        echo "$part ratio=$ratio OK"
    done

    # Mix legacy V3 and V4 parts, then merge using V4 with advanced shared data.
    query "TRUNCATE TABLE json_sparse_test;
           SYSTEM STOP MERGES json_sparse_test;
           ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='v3',
               ratio_of_defaults_for_sparse_serialization=0.9375;
           INSERT INTO json_sparse_test SELECT * FROM json_sparse_source WHERE id < 2048;
           ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='v4',
               object_shared_data_serialization_version='advanced';
           INSERT INTO json_sparse_test SELECT * FROM json_sparse_source WHERE id >= 2048;"
    check_reads
    query "SYSTEM START MERGES json_sparse_test; OPTIMIZE TABLE json_sparse_test FINAL"
    check_reads
    echo "$part mixed V3/V4 OK"

    # One INSERT with multiple blocks; later blocks change the default ratio.
    query "TRUNCATE TABLE json_sparse_test;
           INSERT INTO json_sparse_test SELECT * FROM json_sparse_source ORDER BY id
           SETTINGS max_threads=1, max_block_size=128, max_insert_block_size=128,
                    min_insert_block_size_rows=0, min_insert_block_size_bytes=0;"
    check_reads
    # Rewriting with ratio=1 disables sparse, even though the source part was sparse.
    query "ALTER TABLE json_sparse_test MODIFY SETTING ratio_of_defaults_for_sparse_serialization=1;
           ALTER TABLE json_sparse_test UPDATE j=j WHERE 1 SETTINGS mutations_sync=2;"
    check_reads
    query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0, substreams)) != 0)
           FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                AND active AND column='j'" >/dev/null
    # Re-enable sparse encoding for eligible typed paths with defaults, then rewrite to the old format.
    query "ALTER TABLE json_sparse_test MODIFY SETTING ratio_of_defaults_for_sparse_serialization=0;
           ALTER TABLE json_sparse_test UPDATE j=j WHERE 1 SETTINGS mutations_sync=2;"
    check_reads
    query "SELECT throwIf(countIf(has(substreams, 'j.z.sparse.idx')) = 0)
           FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                AND active AND column='j'" >/dev/null
    query "ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='v3';
           ALTER TABLE json_sparse_test UPDATE j=j WHERE 1 SETTINGS mutations_sync=2;"
    check_reads
    query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0, substreams)) != 0)
           FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                AND active AND column='j'" >/dev/null
    echo "$part rewrite encoding OK"
done
for version in v1 v2 v3; do
    query "TRUNCATE TABLE json_sparse_test;
           ALTER TABLE json_sparse_test MODIFY SETTING object_serialization_version='$version',
               ratio_of_defaults_for_sparse_serialization=0;
           INSERT INTO json_sparse_test SELECT * FROM json_sparse_source;"
    check_reads
    query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0, substreams)) != 0)
           FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_test'
                AND active AND column='j'" >/dev/null
    echo "$version unchanged OK"
done
query "DROP TABLE json_sparse_test; DROP TABLE json_sparse_source;"

for part in Wide Compact; do
    if [[ $part == Wide ]]; then wide_limit=0; else wide_limit=1000000000; fi
    query "CREATE TABLE json_sparse_nested
           (id UInt64, j JSON(inner JSON(t UInt64, n Nullable(Int64)), max_dynamic_paths=1))
           ENGINE=MergeTree ORDER BY id
           SETTINGS min_bytes_for_wide_part=$wide_limit, min_rows_for_wide_part=$wide_limit,
                    index_granularity=64, index_granularity_bytes=1048576,
                    write_marks_for_substreams_in_compact_parts=1,
                    object_serialization_version='v4', ratio_of_defaults_for_sparse_serialization=0.9375,
                    merge_max_block_size=128;
           SYSTEM STOP MERGES json_sparse_nested;
           INSERT INTO json_sparse_nested SELECT number,
                  if(number % 128 = 0, '{\"inner\":{\"t\":7,\"n\":0},\"x\":0}', '{}')
                  FROM numbers(1024);
           INSERT INTO json_sparse_nested SELECT number+1024,
                  if(number % 128 = 0, '{\"inner\":{\"t\":7,\"n\":0},\"y\":0}', '{}')
                  FROM numbers(1024);"
    for stage in before after; do
        if [[ $stage == after ]]; then
            query "SYSTEM START MERGES json_sparse_nested; OPTIMIZE TABLE json_sparse_nested FINAL"
        fi
        query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0
                           AND (startsWith(x, 'j.x.') OR startsWith(x, 'j.y.')), substreams)) != 0)
               FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_nested'
                    AND active AND column='j'" >/dev/null
        query "SELECT throwIf(sum(j.inner.t) != 112), throwIf(sum(j.inner.n.null) != 2032),
                      throwIf(countIf(isNotNull(j.x)) != 8), throwIf(countIf(isNotNull(j.y)) != 8)
               FROM json_sparse_nested SETTINGS max_threads=1, max_block_size=7" >/dev/null
        query "SELECT throwIf(countIf(toString(j) != if(id % 128 = 0,
                        if(id < 1024, '{\"inner\":{\"n\":0,\"t\":7},\"x\":0}', '{\"inner\":{\"n\":0,\"t\":7},\"y\":0}'),
                        '{\"inner\":{\"n\":null,\"t\":0}}')) != 0)
               FROM json_sparse_nested SETTINGS max_threads=1, max_block_size=7" >/dev/null
    done
    query "CHECK TABLE json_sparse_nested SETTINGS check_query_single_value_result=1" | grep -qx 1
    query "DROP TABLE json_sparse_nested"
    echo "$part nested and shared paths OK"

    # Purely dynamic V4 has an empty typed sparse list, including after a merge.
    query "CREATE TABLE json_sparse_dynamic (id UInt64, j JSON(max_dynamic_types=1))
           ENGINE=MergeTree ORDER BY id SETTINGS object_serialization_version='v4',
               ratio_of_defaults_for_sparse_serialization=0.9375,
               min_bytes_for_wide_part=$wide_limit, min_rows_for_wide_part=$wide_limit,
               index_granularity=64, index_granularity_bytes=1048576;
           SYSTEM STOP MERGES json_sparse_dynamic;
           INSERT INTO json_sparse_dynamic SELECT number,
               multiIf(number % 256 = 0, '{\"d\":0}', number % 256 = 128, '{\"d\":\"s\"}', '{}')
               FROM numbers(2048);
           INSERT INTO json_sparse_dynamic SELECT number + 2048,
               multiIf(number % 256 = 0, '{\"d\":\"s\"}', number % 256 = 128, '{\"d\":0}', '{}')
               FROM numbers(2048);"
    for stage in before after; do
        if [[ $stage == after ]]; then
            query "SYSTEM START MERGES json_sparse_dynamic; OPTIMIZE TABLE json_sparse_dynamic FINAL"
        fi
        query "SELECT throwIf(countIf(arrayExists(x -> position(x, 'sparse.idx') > 0, substreams)) != 0)
               FROM system.parts_columns WHERE database=currentDatabase() AND table='json_sparse_dynamic'
                    AND active AND column='j'" >/dev/null
        query "SELECT throwIf(countIf(isNotNull(j.d)) != 32),
                      throwIf(countIf(isNotNull(j.d.:Int64)) != 16),
                      throwIf(countIf(isNotNull(j.d.:String)) != 16),
                      throwIf(sum(j.d.:Int64.null) != 4080),
                      throwIf(countIf(toString(j) != if(id % 128 != 0, '{}',
                          if((id < 2048) = (id % 256 = 0), '{\"d\":0}', '{\"d\":\"s\"}'))) != 0)
               FROM json_sparse_dynamic SETTINGS max_threads=1, max_block_size=7" >/dev/null
    done
    query "CHECK TABLE json_sparse_dynamic SETTINGS check_query_single_value_result=1" | grep -qx 1
    query "DROP TABLE json_sparse_dynamic"
    echo "$part Dynamic mixed types without sparse streams OK"
done
