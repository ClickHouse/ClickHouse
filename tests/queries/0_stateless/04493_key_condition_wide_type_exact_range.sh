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
${CLICKHOUSE_LOCAL} --path="${DB_DIR}" --multiquery --query "
SELECT count() FROM t WHERE team_id = 1 AND k = 0;
SELECT count() FROM t WHERE team_id = toUInt256(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toInt256(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toUInt128(1) AND k = 0;
SELECT count() FROM t WHERE team_id = toInt128(1) AND k = 0;
SELECT count() FROM t WHERE team_id IN (toUInt256(1)) AND k = 0;
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
${CLICKHOUSE_LOCAL} --path="${DB_DIR2}" --multiquery --query "
SELECT count() FROM pk WHERE (x = 3) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toInt256(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt128(3)) AND (y = 55) AND (5786 >= z);
SELECT count() FROM pk WHERE (x = toUInt256(3)) AND (y = 55) AND (5786 >= z) SETTINGS optimize_use_implicit_projections = 0;
"

rm -rf "${DB_DIR2}"
