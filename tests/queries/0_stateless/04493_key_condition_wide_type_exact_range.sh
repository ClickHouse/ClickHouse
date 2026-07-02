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
# The count is always correct in release builds; here we only check the queries do not abort and
# return the same value as the bare (native-typed) comparison.

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
