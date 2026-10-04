#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

LOCAL_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/external-set.XXXXXX")
trap 'rm -rf "${LOCAL_DIR}"' EXIT

# Runs the remaining arguments in a fresh process, so that `system.events` counts only its queries.
run()
{
    local name="$1"
    shift
    ${CLICKHOUSE_LOCAL} --path "${LOCAL_DIR}/${name}" "$@"
}

cat > "${LOCAL_DIR}/query-log.yaml" <<'YAML'
query_log:
    database: system
    table: query_log
    engine: "ENGINE = Memory"
YAML

# Works like `run` and also logs the queries, so that a report can tell what each query did with its sets.
run_logged()
{
    run "$@" --config-file "${LOCAL_DIR}/query-log.yaml" --log_queries 1
}

# These queries cover every key representation of the in-memory set, with hits, misses and `NULL`s
# on both sides, casts of the left side, and `NOT IN`. Each line counts and checksums the rows
# found. With a threshold of 1 byte, every set spills to disk before its first chunk; small blocks
# make the sorter write a run for each chunk, so keys repeat across runs, and lookups cross the
# blocks of the file. The report below checks this for each query.
PARITY_QUERIES=$(cat <<'SQL'
WITH rhs AS (SELECT toUInt8(number * 7 % 101) FROM numbers(3000))
SELECT 'key8', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toUInt8(number % 251) AS x FROM numbers(10000));
WITH rhs AS (SELECT toUInt16(number * 3 % 40000) FROM numbers(3000))
SELECT 'key16', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toUInt16(number % 50000) AS x FROM numbers(10000));
WITH rhs AS (SELECT toInt32(number * 3) - 10000 FROM numbers(3000))
SELECT 'key32 Int32', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toInt32(number) - 10000 AS x FROM numbers(10000));
WITH rhs AS (SELECT number * 3 FROM numbers(3000))
SELECT 'key64 UInt64', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, number AS x FROM numbers(10000));
WITH rhs AS (SELECT toUInt128(number * 3) * 1000000007 FROM numbers(3000))
SELECT 'keys128 UInt128', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toUInt128(number) * 1000000007 AS x FROM numbers(10000));
WITH rhs AS (SELECT toUInt256(number * 3) * 12345678901234567890 FROM numbers(3000))
SELECT 'keys256 UInt256', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toUInt256(number) * 12345678901234567890 AS x FROM numbers(10000));
WITH rhs AS (SELECT toString(number * 3) FROM numbers(3000))
SELECT 'key_string', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toString(number) AS x FROM numbers(10000));
WITH rhs AS (SELECT toFixedString(toString(number * 3), 40) FROM numbers(3000))
SELECT 'key_fixed_string', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toFixedString(toString(number % 100000), 40) AS x FROM numbers(10000));
WITH rhs AS (SELECT (toUInt16(number * 3 % 300), toUInt16(number % 7)) FROM numbers(3000))
SELECT 'keys32', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (toUInt16(number % 300), toUInt16(number % 7)) AS x FROM numbers(10000));
WITH rhs AS (SELECT (toUInt32(number * 3), toUInt32(number % 7)) FROM numbers(3000))
SELECT 'keys64', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (toUInt32(number), toUInt32(number % 7)) AS x FROM numbers(10000));
WITH rhs AS (SELECT (toString(number * 3), number % 7) FROM numbers(3000))
SELECT 'hashed tuple', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (toString(number), number % 7) AS x FROM numbers(10000));

-- These keys hold bytes that are not text: zero bytes, non-ASCII bytes, and keys of several KiB.
WITH rhs AS (SELECT concat(toString(number * 3), repeat(char(0, 120, 255), 1 + number * 3 % 5 * 1024)) FROM numbers(1000))
SELECT 'key_string bytes', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, concat(toString(number), repeat(char(0, 120, 255), 1 + number % 5 * 1024)) AS x FROM numbers(3000));

-- The subquery returns constant key columns.
WITH rhs AS (SELECT 7 FROM numbers(1000))
SELECT 'constant key column', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, number AS x FROM numbers(10000));
WITH rhs AS (SELECT number * 3, 1 FROM numbers(3000))
SELECT 'constant key element', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (number, 1) AS x FROM numbers(10000));

-- `NULL`s are skipped in the set and never found, or kept in the set and found with `transform_null_in`.
WITH rhs AS (SELECT if(number % 5 = 0, NULL, number * 3) FROM numbers(3000))
SELECT 'Nullable key64', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, if(number % 7 = 0, NULL, number) AS x FROM numbers(10000));
WITH rhs AS (SELECT if(number % 5 = 0, NULL, number * 3) FROM numbers(3000))
SELECT 'Nullable nullable_keys128', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, if(number % 7 = 0, NULL, number) AS x FROM numbers(10000))
SETTINGS transform_null_in = 1;
WITH rhs AS (SELECT if(number % 5 = 0, NULL, toString(number * 3)) FROM numbers(3000))
SELECT 'Nullable hashed', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, if(number % 7 = 0, NULL, toString(number)) AS x FROM numbers(10000))
SETTINGS transform_null_in = 1;
WITH rhs AS (SELECT (if(number % 5 = 0, NULL, toUInt32(number * 3)), toUInt8(number % 3)) FROM numbers(3000))
SELECT 'nullable_keys128 tuple', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (if(number % 7 = 0, NULL, toUInt32(number)), toUInt8(number % 3)) AS x FROM numbers(10000))
SETTINGS transform_null_in = 1;
WITH rhs AS (SELECT (if(number % 5 = 0, NULL, number * 3), number % 3, number % 5) FROM numbers(3000))
SELECT 'nullable_keys256 tuple', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, (if(number % 7 = 0, NULL, number), number % 3, number % 5) AS x FROM numbers(10000))
SETTINGS transform_null_in = 1;

-- These sets hold `NULL` alone, `LowCardinality(Nullable(String))` keys, a subquery with a single
-- tuple column, and a tuple with `LowCardinality` and `Nullable` elements.
SELECT 'null sets', 1 IN (SELECT NULL), NULL IN (SELECT NULL), 1 IN (SELECT NULL WHERE 0);
SELECT 'null sets transform_null_in', NULL IN (SELECT NULL) SETTINGS transform_null_in = 1;
SELECT 'LowCardinality Nullable', groupArray(x IN (SELECT toLowCardinality(if(number % 3 = 0, NULL, toString(number))) FROM numbers(10)))
FROM (SELECT toLowCardinality(if(number % 4 = 0, NULL, toString(number))) AS x FROM numbers(12));
SELECT 'LowCardinality Nullable transform_null_in', groupArray(x IN (SELECT toLowCardinality(if(number % 3 = 0, NULL, toString(number))) FROM numbers(10)))
FROM (SELECT toLowCardinality(if(number % 4 = 0, NULL, toString(number))) AS x FROM numbers(12)) SETTINGS transform_null_in = 1;
SELECT 'tuple column', groupArray((number, number % 3) IN (SELECT tuple(number, number % 3) FROM numbers(5))) FROM numbers(8);
SELECT 'tuple with LowCardinality and Nullable', groupArray(t IN (SELECT (toLowCardinality(toString(number % 3)), if(number = 1, NULL, number)) FROM numbers(5)))
FROM (SELECT (toLowCardinality(toString(number % 3)), if(number = 2, NULL, number)) AS t FROM numbers(8));
SELECT 'tuple with LowCardinality and Nullable transform_null_in', groupArray(t IN (SELECT (toLowCardinality(toString(number % 3)), if(number = 1, NULL, number)) FROM numbers(5)))
FROM (SELECT (toLowCardinality(toString(number % 3)), if(number = 2, NULL, number)) AS t FROM numbers(8)) SETTINGS transform_null_in = 1;

-- The left side is cast to the key types of the set; values that do not fit are not found.
WITH rhs AS (SELECT number * 3 FROM numbers(3000))
SELECT 'cast Int64 to UInt64', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toInt64(number) - 100 AS x FROM numbers(10000));
WITH rhs AS (SELECT toInt32(number * 3) FROM numbers(3000))
SELECT 'cast Float64 to Int32', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, number / 2 AS x FROM numbers(10000));
WITH rhs AS (SELECT toFixedString(toString(number % 500), 3) FROM numbers(3000))
SELECT 'cast String to FixedString', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toString(number % 5000) AS x FROM numbers(10000));
WITH rhs AS (SELECT toString(number * 3) FROM numbers(3000))
SELECT 'cast LowCardinality to String', countIf(x IN rhs), countIf(x NOT IN rhs), sum(cityHash64(number) * (x IN rhs))
FROM (SELECT number, toLowCardinality(toString(number)) AS x FROM numbers(10000));

-- Special floating-point values are compared by their binary representation, in memory and on disk alike.
SELECT 'floats', groupArray(x IN (SELECT arrayJoin([0., nan, inf, 1.5]))), groupArray(x NOT IN (SELECT arrayJoin([0., nan, inf, 1.5])))
FROM (SELECT arrayJoin([0., -0., nan, -nan, inf, -inf, 1.5, 2.5]) AS x);

-- These queries cover constant left sides, an empty set, a set used twice, and `IN` outside of a filter
-- conjunct.
SELECT 'constant', 5 IN (SELECT number FROM numbers(10)), 50 IN (SELECT number FROM numbers(10)), 5 NOT IN (SELECT number FROM numbers(10));
SELECT 'empty', countIf(number IN (SELECT number FROM numbers(0))), countIf(number NOT IN (SELECT number FROM numbers(0))) FROM numbers(10);
WITH rhs AS (SELECT number * 3 FROM numbers_mt(3335))
SELECT 'shared', countIf((number IN rhs) != (number % 3 = 0)), countIf((number NOT IN rhs) != (number % 3 != 0)) FROM numbers_mt(10005);
SELECT 'disjunction', groupArray(number) FROM numbers(20) WHERE number = 19 OR number IN (SELECT number * 5 FROM numbers(3));

-- Index analysis builds the set before the query reads the table, and the filter still applies to every row.
DROP TABLE IF EXISTS lhs;
CREATE TABLE lhs (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
INSERT INTO lhs SELECT number FROM numbers(100000);
SELECT 'index', count(), sum(k) FROM lhs WHERE k IN (SELECT number * 10 FROM numbers(1000)) SETTINGS use_index_for_in_with_subqueries = 1;
DROP TABLE lhs;
SQL
)

# The report shows, for each query of a case by the label of its first column, the sets that it
# filled, how many of them spilled to disk, how many times the runs of its sets were merged, and
# whether its lookups read the sets from disk. The lines of the report start with `report`.
PARITY_REPORT=$(cat <<'SQL'
SYSTEM FLUSH LOGS query_log;
SELECT 'report', extract(query, 'SELECT \'([^\']+)\'') AS label, ProfileEvents['SetsBuiltFromSubquery'],
    ProfileEvents['SetsSpilledToDisk'], ProfileEvents['ExternalSetMerge'], ProfileEvents['ExternalSetReadBlocks'] > 0
FROM system.query_log
WHERE type = 'QueryFinish' AND query_kind = 'Select' AND label != ''
ORDER BY event_time_microseconds;
SQL
)

for threshold in 0 1; do
    run_logged "parity-${threshold}" --max_bytes_before_external_set "${threshold}" --max_block_size 1000 \
        --max_untracked_memory 0 --multiquery \
        > "${LOCAL_DIR}/parity-${threshold}.out" <<< "${PARITY_QUERIES}
${PARITY_REPORT}"
done
diff -u <(grep -v '^report' "${LOCAL_DIR}/parity-0.out") <(grep -v '^report' "${LOCAL_DIR}/parity-1.out")
grep -v '^report' "${LOCAL_DIR}/parity-1.out"

# With the threshold of 1 byte, every set that receives a chunk spills to disk before inserting it, the sorter
# writes each chunk as a run and merges the runs into the finished set, and lookups read the set from disk;
# exact memory tracking makes the runs deterministic. A set of an empty subquery receives no chunk, and a set
# of `NULL`s alone keeps no key to merge or to read. Without a threshold, no set spills to disk.
grep '^report' "${LOCAL_DIR}/parity-1.out"
grep '^report' "${LOCAL_DIR}/parity-0.out" | awk -F'\t' '{ built += $3; spilled += $4 } END { print "in memory", built, spilled }'

# Prints the value of an event, 0 if it did not occur.
event()
{
    echo "SELECT sum(value) FROM system.events WHERE event = '$1';"
}

# The runs of the external sort and the finished set use the codec of `temporary_files_codec`: the keys
# compress with `LZ4` and take more space than their own bytes with `NONE`, which adds a header to each block.
# The sets on disk find the same keys with either codec, for the narrowest and the widest keys on disk.
for codec in LZ4 NONE; do
    run "codec-${codec}" --max_bytes_before_external_set 1 --temporary_files_codec "${codec}" --multiquery <<SQL
SELECT countIf(number IN (SELECT number * 3 FROM numbers(30000))) FROM numbers(90000);
SELECT countIf(toUInt256(number) IN (SELECT toUInt256(number * 3) FROM numbers(30000))) FROM numbers(90000);
SELECT '${codec}', (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetCompressedBytes')
    < (SELECT sum(value) FROM system.events WHERE event = 'ExternalSetUncompressedBytes');
SQL
done

# The rows of the subquery count once in the query progress, whether the set is in memory or on disk.
for threshold in 0 1; do
    run "progress-${threshold}" --max_bytes_before_external_set "${threshold}" --multiquery <<SQL
SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000)) FORMAT Null;
$(event SelectedRows)
$(event SetsSpilledToDisk)
SQL
done

# The files are removed when the queries finish.
run cleanup --max_bytes_before_external_set 1 --multiquery <<SQL
SELECT count() FROM numbers(2048) WHERE number IN (SELECT number FROM numbers(2048));
SELECT sum(value) > 0 FROM system.events WHERE event = 'ExternalSetWritePart';
SELECT value FROM system.metrics WHERE metric = 'TemporaryFilesForSet';
SQL

expect_error()
{
    local expected="$1"
    local query="$2"
    if run errors --max_bytes_before_external_set 1 --multiquery --query "$query" > "${LOCAL_DIR}/error.out" 2> "${LOCAL_DIR}/error.err"; then
        echo "Expected ${expected}"
        return 1
    fi
    grep -q "(${expected})" "${LOCAL_DIR}/error.err"
    echo "$expected"
}

expect_error BAD_ARGUMENTS "SELECT 1 IN (SELECT number FROM numbers(10)) SETTINGS max_bytes_ratio_before_external_set = 1"
expect_error TOO_MANY_ROWS_OR_BYTES "SELECT 1 IN (SELECT number FROM numbers(10000)) SETTINGS max_temporary_data_on_disk_size_for_query = 1"
expect_error NOT_ENOUGH_SPACE "SELECT 1 IN (SELECT number FROM numbers(100)) SETTINGS min_free_disk_space_for_temporary_data = 1000000000000000"

# When writing the set stops before its end, as at the time limit in the `break` overflow mode, the incomplete
# set is not used: the query stops with an exception.
expect_error QUERY_WAS_CANCELLED "SYSTEM ENABLE FAILPOINT disk_set_builder_stop_before_finish;
    SELECT count() FROM numbers(10) WHERE number IN (SELECT number FROM numbers(1000))"
