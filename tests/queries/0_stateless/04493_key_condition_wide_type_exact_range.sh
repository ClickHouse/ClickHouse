#!/usr/bin/env bash
# Regression for the debug-only "Inconsistent KeyCondition behavior" logical error (#90461 canary).
#
# Comparing the first key column against a wider-typed constant (e.g. UInt64 key vs toUInt256(...))
# inserts a widening CAST into the monotonic function chain. matchesExactContinuousRange used to
# classify such a column as an exact POINT because the CAST is strictly monotonic, but checkInRange
# applies the chain to granule bounds with forced-closed bounds, so a granule straddling a key
# boundary is honestly reported as can_be_false. The exact continuous-range (implicit count
# projection) path then contradicted that and aborted in debug builds.
#
# The trigger needs the parts read fresh from disk in a separate process, so use clickhouse-local
# with a persisted table: build the data in one invocation, then run the count queries in a second.
# In release builds the broken exact range is dropped and read normally, so the count stays correct;
# here we check the queries do not abort and return the same value as the bare (native-typed) comparison.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB_DIR="${CLICKHOUSE_TMP}/04493_${CLICKHOUSE_DATABASE}"
rm -rf "${DB_DIR}"
mkdir -p "${DB_DIR}"

# Build a multi-column-key table with a tiny granularity and several distinct first-key values, so
# granules straddle the team_id boundary. Persist it to disk.
${CLICKHOUSE_LOCAL} --path="${DB_DIR}" --multiquery --query "
CREATE TABLE t (team_id UInt64, k UInt8, s String)
ENGINE = MergeTree ORDER BY (team_id, k, s) SETTINGS index_granularity = 4;
SYSTEM STOP MERGES t;
INSERT INTO t SELECT 1, number % 5, toString(number) FROM numbers(50);
INSERT INTO t SELECT 2, number % 5, toString(number) FROM numbers(50);
INSERT INTO t SELECT number % 3, number % 5, toString(number) FROM numbers(50);
"

# Query the persisted table in a separate process. Each of these used to abort with
# "Inconsistent KeyCondition behavior" in debug builds; all must equal the native-typed count (13).
# Pin the projection settings so the exact-count path is exercised regardless of their defaults.
${CLICKHOUSE_LOCAL} --path="${DB_DIR}" --multiquery --query "
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
-- Assert the wide-typed comparison is routed through the exact-count projection (the abort path); count alone passes even if it is silently declined.
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = 0) WHERE explain ILIKE '%_exact_count_projection%';
-- Assert the wide-typed comparison forces generic-exclusion index search: the fix demotes the leading key from POINT to RANGE. This distinguishes fixed from reverted code even in release, where the debug abort is compiled out.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = 0) WHERE explain ILIKE '%generic exclusion search%';
SELECT count() FROM t WHERE team_id = 1 AND k = 0;
SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toInt256(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toUInt128(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toInt128(1) AND k = 0;
SELECT count() FROM t WHERE team_id IN (toUInt256(1)) AND k = 0;
-- A wider-typed cast applied to the key inside IN (not a bare key) populates the set-index function chain,
-- so this exercises the changed non-empty mapping.functions branch; it must also route through generic exclusion.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t WHERE toUInt256(team_id) IN (toUInt256(1)) AND k = 0) WHERE explain ILIKE '%generic exclusion search%';
SELECT count() FROM t WHERE toUInt256(team_id) IN (toUInt256(1)) AND k = 0;
SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = toUInt256(0);
SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = 0 SETTINGS force_primary_key = 1;
"

rm -rf "${DB_DIR}"

# Issue #111421 (qoega): composite (x, y, z) key at index_granularity = 1, a count() with the wider-typed
# equality on the leading key AND range conditions on the rest, over a multi-x dataset. This is the
# aggregate-projection exact-count path with the mark layout fine enough to disagree with the analyzer's
# continuous-range claim; it aborted in debug and read normally (correct count) in release. Correct count
# is 3: rows (3,55,1235),(3,55,2791),(3,55,5786), all with z <= 5786.
DB_DIR2="${CLICKHOUSE_TMP}/04493b_${CLICKHOUSE_DATABASE}"
rm -rf "${DB_DIR2}"
mkdir -p "${DB_DIR2}"

${CLICKHOUSE_LOCAL} --path="${DB_DIR2}" --multiquery --query "
CREATE TABLE pk (x UInt64, y UInt64, z UInt64)
ENGINE = MergeTree ORDER BY (x, y, z) SETTINGS index_granularity = 1;
INSERT INTO pk VALUES
  (1, 11, 1235), (1, 11, 4395), (1, 22, 3545), (1, 22, 6984), (1, 33, 4596),
  (2, 11, 3572), (2, 11, 4563), (2, 11, 4578), (2, 22, 2791), (2, 22, 2791),
  (2, 22, 5786), (2, 22, 5786), (3, 33, 1235), (3, 33, 2791), (3, 33, 2791),
  (3, 44, 4578), (3, 44, 4935), (3, 55, 1235), (3, 55, 2791), (3, 55, 5786);
"

# Query in a separate process. Used to abort in debug; must return 3 (matching the native-typed comparison).
# Pin the projection settings so the exact-count path is exercised regardless of their defaults.
${CLICKHOUSE_LOCAL} --path="${DB_DIR2}" --multiquery --query "
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
-- Assert the wide-typed comparison is routed through the exact-count projection (the abort path); count alone passes even if it is silently declined.
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z)) WHERE explain ILIKE '%_exact_count_projection%';
-- Assert generic-exclusion index search (leading key demoted POINT -> RANGE by the fix); distinguishes fixed from reverted code in release too.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z)) WHERE explain ILIKE '%generic exclusion search%';
SELECT count() FROM pk WHERE (x = 3) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt128(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z) SETTINGS optimize_use_implicit_projections = 0;
"

# A native exact-point atom and a widened-cast atom on the same key column (x = 3 AND x = toUInt256(3)).
# enable_analyzer = 0 keeps both atoms in the key condition (the analyzer folds them into one); the
# column is then pinned POINT by the native equality while the redundant cast atom stays in the rpn. This
# must still yield an exact, consistent range: expect the native count (3), the exact-count path live, no abort.
${CLICKHOUSE_LOCAL} --path="${DB_DIR2}" --multiquery --query "
SET enable_analyzer = 0;
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM pk WHERE (x = 3) AND (x = toUInt256(3)) AND (y = 55) AND (5786 >= z)) WHERE explain ILIKE '%_exact_count_projection%';
-- The native x = 3 atom must keep the leading key at POINT (binary search); the redundant widened-cast
-- atom must not overwrite it into a RANGE. Generic exclusion also yields exact ranges, so assert the
-- search algorithm directly, otherwise the count/exact-count assertions pass even if the point were lost.
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM pk WHERE (x = 3) AND (x = toUInt256(3)) AND (y = 55) AND (5786 >= z)) WHERE explain ILIKE '%binary search%';
SELECT count() FROM pk WHERE (x = 3) AND (x = toUInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = 3) AND (x = toInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = 3) AND (x = toUInt128(3)) AND (y = 55) AND (5786 >= z);
"

rm -rf "${DB_DIR2}"

# Same wider-typed cast on the leading key, now with a DESCending key column (ORDER BY (g, r DESC)):
# the fix is order-independent, so this must also return the native count (10), not abort.
DB_DIR3="${CLICKHOUSE_TMP}/04493c_${CLICKHOUSE_DATABASE}"
rm -rf "${DB_DIR3}"
mkdir -p "${DB_DIR3}"

${CLICKHOUSE_LOCAL} --path="${DB_DIR3}" --multiquery --query "
CREATE TABLE t_rev (g UInt32, r UInt32) ENGINE = MergeTree ORDER BY (g, r DESC) SETTINGS index_granularity = 4;
INSERT INTO t_rev SELECT number % 10, 1000 - number FROM numbers(1000);
"

${CLICKHOUSE_LOCAL} --path="${DB_DIR3}" --multiquery --query "
SET optimize_use_projections = 1;
SET optimize_use_implicit_projections = 1;
-- Exact-count projection path live (the abort path) and generic-exclusion index search (leading key
-- demoted POINT -> RANGE by the fix); the second assertion distinguishes fixed from reverted code in release too.
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_rev WHERE (g = toUInt256(5)) AND (toInt64(r) >= 900)) WHERE explain ILIKE '%_exact_count_projection%';
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_rev WHERE (g = toUInt256(5)) AND (toInt64(r) >= 900)) WHERE explain ILIKE '%generic exclusion search%';
SELECT count() FROM t_rev WHERE (g = 5) AND (toInt64(r) >= 900);
SELECT count() FROM t_rev WHERE (g = toUInt256(5)) AND (toInt64(r) >= 900);
SELECT count() FROM t_rev WHERE (g = toInt256(5)) AND (toInt64(r) >= 900);
SELECT count() FROM t_rev WHERE (g = toUInt128(5)) AND (toInt64(r) >= 900);
SELECT count() FROM t_rev WHERE (g = toUInt256(5)) AND (toInt64(r) >= 900) SETTINGS optimize_use_implicit_projections = 0;
"

rm -rf "${DB_DIR3}"
