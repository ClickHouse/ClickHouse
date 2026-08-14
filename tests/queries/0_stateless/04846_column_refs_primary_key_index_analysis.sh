#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Primary key index analysis must prune exactly the same granules whether the key values are
# materialized as `Field` or referenced in the index columns, i.e. whether
# `use_column_refs_for_primary_key_index_analysis` is enabled or not.
#
# For every query both the result and the `EXPLAIN indexes = 1` output (which reports the surviving
# parts and granules) are compared between the two representations. Only a divergence is printed.

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE IF EXISTS t_pk_refs_str;
DROP TABLE IF EXISTS t_pk_refs_nullable;
DROP TABLE IF EXISTS t_pk_refs_func;
DROP TABLE IF EXISTS t_pk_refs_desc;
DROP TABLE IF EXISTS t_pk_refs_partitioned;

-- String key: the representation that \`Field\` makes expensive.
CREATE TABLE t_pk_refs_str (s String, lc LowCardinality(String), n UInt64)
ENGINE = MergeTree ORDER BY (s, lc, n) SETTINGS index_granularity = 8;

INSERT INTO t_pk_refs_str SELECT toString(number % 1000), toString(number % 7), number FROM numbers(10000);

-- Nullable key.
CREATE TABLE t_pk_refs_nullable (a Nullable(UInt64), b Nullable(String))
ENGINE = MergeTree ORDER BY (a, b) SETTINGS index_granularity = 8, allow_nullable_key = 1;

INSERT INTO t_pk_refs_nullable
SELECT if(number % 13 = 0, NULL, number % 500), if(number % 11 = 0, NULL, toString(number % 300)) FROM numbers(5000);

-- Monotonic function chains over the key.
CREATE TABLE t_pk_refs_func (d DateTime, x Int64)
ENGINE = MergeTree ORDER BY (d, x) SETTINGS index_granularity = 8;

INSERT INTO t_pk_refs_func SELECT toDateTime('2020-01-01') + number * 60, number - 2500 FROM numbers(5000);

-- Reverse-sorted key column.
CREATE TABLE t_pk_refs_desc (a UInt64, s String)
ENGINE = MergeTree ORDER BY (a DESC, s) SETTINGS index_granularity = 8;

INSERT INTO t_pk_refs_desc SELECT number % 400, toString(number % 90) FROM numbers(5000);

-- Partitioned, so that the partition minmax turns key columns into constant coordinates.
CREATE TABLE t_pk_refs_partitioned (d Date, k UInt64, s String)
ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY (d, k, s) SETTINGS index_granularity = 8;

INSERT INTO t_pk_refs_partitioned
SELECT toDate('2020-01-01') + number % 200, number % 300, toString(number % 50) FROM numbers(6000);
"

queries=(
    "SELECT count() FROM t_pk_refs_str WHERE s = '42'"
    "SELECT count() FROM t_pk_refs_str WHERE s > '500' AND s < '600'"
    "SELECT count() FROM t_pk_refs_str WHERE s IN ('1', '42', '999') AND lc = '3'"
    "SELECT count() FROM t_pk_refs_str WHERE (s, lc) IN (('1', '1'), ('42', '0'))"
    "SELECT count() FROM t_pk_refs_str WHERE s = '7' AND n BETWEEN 100 AND 8000"
    "SELECT count() FROM t_pk_refs_str WHERE s LIKE '12%'"
    "SELECT count() FROM t_pk_refs_str WHERE lc IN ('2', '5')"
    "SELECT count() FROM t_pk_refs_str WHERE s = '3' AND lc NOT IN ('0', '1')"
    "SELECT count() FROM t_pk_refs_str WHERE toUInt64(s) < 50"
    "SELECT sum(n) FROM t_pk_refs_str WHERE s NOT IN ('1', '2') AND lc > '4'"
    "SELECT count() FROM t_pk_refs_nullable WHERE a IS NULL"
    "SELECT count() FROM t_pk_refs_nullable WHERE a IS NOT NULL AND a < 100"
    "SELECT count() FROM t_pk_refs_nullable WHERE b IS NULL AND a = 7"
    "SELECT count() FROM t_pk_refs_nullable WHERE a IN (1, 2, NULL, 400)"
    "SELECT count() FROM t_pk_refs_func WHERE toDate(d) = '2020-01-02'"
    "SELECT count() FROM t_pk_refs_func WHERE toStartOfHour(d) >= '2020-01-02 03:00:00' AND d < '2020-01-02 05:00:00'"
    "SELECT count() FROM t_pk_refs_func WHERE abs(x) < 100"
    "SELECT count() FROM t_pk_refs_func WHERE -x > 2000"
    "SELECT count() FROM t_pk_refs_func WHERE toUInt32(d) % 3600 = 0 AND d < '2020-01-02'"
    "SELECT count() FROM t_pk_refs_desc WHERE a > 300"
    "SELECT count() FROM t_pk_refs_desc WHERE a BETWEEN 100 AND 120 AND s = '7'"
    "SELECT count() FROM t_pk_refs_desc WHERE a IN (5, 50, 395)"
    "SELECT count() FROM t_pk_refs_partitioned WHERE d = '2020-03-01'"
    "SELECT count() FROM t_pk_refs_partitioned WHERE d > '2020-02-15' AND k = 42"
    "SELECT count() FROM t_pk_refs_partitioned WHERE toYYYYMM(d) = 202004 AND s = '10'"
    "SELECT count() FROM t_pk_refs_partitioned WHERE k < 10 AND s IN ('1', '2')"
)

for query in "${queries[@]}"
do
    for prefix in "" "EXPLAIN indexes = 1 "
    do
        with_fields=$($CLICKHOUSE_CLIENT --use_lightweight_primary_key_index_analysis 1 \
            --use_column_refs_for_primary_key_index_analysis 0 --query "$prefix$query")
        with_refs=$($CLICKHOUSE_CLIENT --use_lightweight_primary_key_index_analysis 1 \
            --use_column_refs_for_primary_key_index_analysis 1 --query "$prefix$query")

        if [[ "$with_fields" != "$with_refs" ]]
        then
            echo "MISMATCH for: $prefix$query"
            diff <(echo "$with_fields") <(echo "$with_refs")
        fi
    done
done

echo "All representations agree"

$CLICKHOUSE_CLIENT --multiquery "
DROP TABLE t_pk_refs_str;
DROP TABLE t_pk_refs_nullable;
DROP TABLE t_pk_refs_func;
DROP TABLE t_pk_refs_desc;
DROP TABLE t_pk_refs_partitioned;
"
