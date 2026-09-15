#!/usr/bin/env bash
# Tags: no-fasttest, shard

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# No ORDER BY or LIMIT in any `--stage fetch_columns` query below: either one makes the stage
# optimizer return a stage above FetchColumns, so the read under test never happens. Rows are
# sorted by `sort` instead.

${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE src (A Int64, B Int64) ENGINE = MergeTree ORDER BY A;
INSERT INTO src SELECT -number, number * 10 FROM numbers(100);
CREATE TABLE dist (A Int64, B Int64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), src);
CREATE TABLE dist_over_dist (A Int64, B Int64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), dist);

-- Buffer whose declared type differs from the destination's un-order-preservingly.
CREATE TABLE buf_mismatch (A UInt64, B Int64)
    ENGINE = Buffer(currentDatabase(), dist, 1, 3600, 36000, 10000, 1000000, 10000000, 100000000);
-- Buffer with the destination's exact structure: reads through a different conversion site.
CREATE TABLE buf_matched (A Int64, B Int64)
    ENGINE = Buffer(currentDatabase(), dist, 1, 3600, 36000, 10000, 1000000, 10000000, 100000000);
-- Order-preserving widening, which must keep working.
CREATE TABLE src32 (A Int32) ENGINE = MergeTree ORDER BY A;
INSERT INTO src32 SELECT -number FROM numbers(10);
CREATE TABLE dist32 (A Int32)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), src32);
CREATE TABLE buf_widen (A Int64)
    ENGINE = Buffer(currentDatabase(), dist32, 1, 3600, 36000, 10000, 1000000, 10000000, 100000000);

-- The view's declared type differs from its target's, so reading it needs a conversion too.
CREATE TABLE mv_source (A Int64) ENGINE = MergeTree ORDER BY A;
CREATE TABLE mv_target (A Int64) ENGINE = MergeTree ORDER BY A;
CREATE TABLE dist_target (A Int64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), mv_target, A);
CREATE MATERIALIZED VIEW mv1 TO dist_target AS SELECT toUInt64(A) AS A FROM mv_source;
-- Foreground insert, so the local shard is written before the reads below whatever
-- prefer_localhost_replica the run happens to randomize.
INSERT INTO mv_source SELECT -number FROM numbers(10) SETTINGS distributed_foreground_insert = 1;

-- Merge forwards its child's stage, so a Distributed child is read at FetchColumns here too, and
-- Merge adapts that header itself: by name when the names match, by position otherwise.
CREATE TABLE mrg (A Int64, B Int64) ENGINE = Merge(currentDatabase(), '^dist$');
-- Merge whose declared type differs from the child's un-order-preservingly, as buf_mismatch does.
CREATE TABLE mrg_mismatch (A UInt64, B Int64) ENGINE = Merge(currentDatabase(), '^dist$');

CREATE TABLE rt (k Int64, v Int64) ENGINE = MergeTree ORDER BY k;
INSERT INTO rt SELECT -number, number FROM numbers(5);
-- Keys matching a NONZERO B, so a value replaced by a default is visible in the result.
CREATE TABLE keys (v Int64) ENGINE = MergeTree ORDER BY v;
INSERT INTO keys SELECT (number + 1) * 10 FROM numbers(3);
CREATE TABLE dist_keys (v Int64)
    ENGINE = Distributed(test_cluster_two_shards_localhost, currentDatabase(), keys);
"

# $1 label, $2 query. Asserts the result's column names, row count and rows: all three, because the
# count alone would miss a wrong value and the deduped prefix alone would miss a lost row. Logs are
# silenced because Buffer warns on every type-mismatched read.
fetch() {
    echo "-- $1"
    ${CLICKHOUSE_CLIENT} --stage fetch_columns --send_logs_level none --format TSVWithNames \
        -q "$2" 2>&1 | {
            read -r header
            echo "header: $header"
            rows=$(cat)
            echo "rows: $(printf '%s\n' "$rows" | grep -c .)"
            # LC_ALL=C: the values are negative, and a UTF-8 collation ignores the leading `-`.
            printf '%s\n' "$rows" | LC_ALL=C sort -u | head -4
        }
}

echo '=== Buffer over Distributed, declared type differs ==='
fetch 'buf_mismatch A' 'SELECT A FROM buf_mismatch'
fetch 'buf_mismatch B' 'SELECT B FROM buf_mismatch'
fetch 'buf_mismatch A, B' 'SELECT A, B FROM buf_mismatch'
# Column order differs from the Buffer's declaration order, so a positional match would pair
# B with A here instead of failing.
fetch 'buf_mismatch B, A' 'SELECT B, A FROM buf_mismatch'
fetch 'buf_mismatch filtered' 'SELECT A FROM buf_mismatch WHERE B > 900'
fetch 'buf_mismatch expression' 'SELECT A + 1 FROM buf_mismatch'

echo '=== Buffer over Distributed, identical structure ==='
fetch 'buf_matched A' 'SELECT A FROM buf_matched'
fetch 'buf_matched B' 'SELECT B FROM buf_matched'

echo '=== Buffer over Distributed, order-preserving widening ==='
fetch 'buf_widen A' 'SELECT A FROM buf_widen'

echo '=== MaterializedView over a Distributed target ==='
fetch 'mv1 A' 'SELECT A FROM mv1'

echo '=== Merge over Distributed ==='
fetch 'mrg A' 'SELECT A FROM mrg'
fetch 'mrg A, B' 'SELECT A, B FROM mrg'
# Reversed projection, so a positional match against a differently ordered header would swap A and B.
# Both of Merge's headers are built from the requested columns, so the two ways it can match agree
# here; this case is what would show it if that ever stopped holding.
fetch 'mrg B, A' 'SELECT B, A FROM mrg'
fetch 'mrg filtered' 'SELECT A FROM mrg WHERE B > 900'
fetch 'mrg, serialize_query_plan' 'SELECT A FROM mrg SETTINGS serialize_query_plan = 1'
fetch 'mrg_mismatch A' 'SELECT A FROM mrg_mismatch'
fetch 'mrg_mismatch B, A' 'SELECT B, A FROM mrg_mismatch'

echo '=== Distributed read without a wrapper ==='
fetch 'dist A' 'SELECT A FROM dist'
fetch 'dist A, B' 'SELECT A, B FROM dist'
fetch 'dist in a subquery' 'SELECT A FROM (SELECT A FROM dist)'
fetch 'dist over dist' 'SELECT A FROM dist_over_dist'

# Shipping a subquery to the shards as a temporary table clones the shard query tree again, so the
# table expression carrying the identifiers cannot be remembered from before that step.
echo '=== Subquery shipped to the shards ==='
fetch 'global in' \
    'SELECT A, B FROM buf_mismatch WHERE B GLOBAL IN (SELECT v FROM keys)'
fetch 'global in, Distributed subquery' \
    'SELECT A, B FROM buf_mismatch WHERE B GLOBAL IN (SELECT v FROM dist_keys)'
fetch 'in, prefer_global_in_and_join' \
    'SELECT A, B FROM buf_mismatch WHERE B IN (SELECT v FROM keys) SETTINGS prefer_global_in_and_join = 1'
fetch 'in, distributed_product_mode=local' \
    "SELECT A, B FROM buf_mismatch WHERE B IN (SELECT v FROM dist_keys) SETTINGS distributed_product_mode = 'local'"
fetch 'in, distributed_product_mode=global' \
    "SELECT A, B FROM buf_mismatch WHERE B IN (SELECT v FROM dist_keys) SETTINGS distributed_product_mode = 'global'"

# Plan serialization reads a remote storage through its own FetchColumns interpreter, which renames
# in the opposite direction. CI does not randomize that setting, so the test has to set it.
echo '=== Plan serialization ==='
fetch 'buf_mismatch, serialize_query_plan' \
    'SELECT A FROM buf_mismatch SETTINGS serialize_query_plan = 1'
fetch 'dist over dist, serialize_query_plan' \
    'SELECT A FROM dist_over_dist SETTINGS serialize_query_plan = 1'
fetch 'mv1, serialize_query_plan' \
    'SELECT A FROM mv1 SETTINGS serialize_query_plan = 1'

echo '=== Columns from outside the storage schema are left alone ==='
fetch 'join, Distributed on the left' \
    "SELECT d.A, r.v FROM dist AS d JOIN ${CLICKHOUSE_DATABASE}.rt AS r ON d.A = r.k"
# A table function resolves to a nested Distributed just like a table does, so its joined columns
# need the same treatment.
fetch 'join, cluster() on the left' \
    "SELECT d.A, r.v FROM cluster(test_cluster_two_shards_localhost, ${CLICKHOUSE_DATABASE}, src) AS d
        JOIN ${CLICKHOUSE_DATABASE}.rt AS r ON d.A = r.k"

echo '=== Without the analyzer ==='
fetch 'buf_mismatch A' 'SELECT A FROM buf_mismatch SETTINGS enable_analyzer = 0'
fetch 'dist A' 'SELECT A FROM dist SETTINGS enable_analyzer = 0'
