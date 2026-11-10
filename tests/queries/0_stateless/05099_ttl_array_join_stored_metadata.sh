#!/usr/bin/env bash
# A TTL expression containing `arrayJoin` is rejected when it is stated (see
# `05073_ttl_array_join_rejected`), but a table whose TTL was stored before that check existed still has
# to load: rejecting it while the metadata is read fails the whole load rather than the one table, so
# `TTLValidationMode::Attach` skips the check. Such a TTL must not silently execute either - every
# consumer indexes the expression result positionally against the block's rows, so a row whose own TTL is
# far in the future is deleted along with an earlier expired one. It has to fail instead, at the moment
# the stored expression is turned back into a runnable one.
#
# `clickhouse-local` over a prepared data directory is how the stored metadata is obtained here: the
# table is created with an ordinary TTL, its stored definition is then edited into the form an older
# server would have written, and the next start loads it.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP:?}/${CLICKHOUSE_TEST_UNIQUE_NAME:?}"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

# `materialize_ttl_after_modify = 0` leaves the parts without TTL information, which is what makes the
# merge below recalculate the TTL - the same thing a background TTL merge does after an upgrade.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "
CREATE DATABASE db;
CREATE TABLE db.t (k UInt32, arr Array(DateTime)) ENGINE = MergeTree ORDER BY k;
INSERT INTO db.t VALUES (1, [now() - 3600, now() - 3600]), (2, [now() + 100000]);
ALTER TABLE db.t MODIFY TTL arr[1] SETTINGS materialize_ttl_after_modify = 0;
"

sed -i 's/^TTL arr\[1\]$/TTL arrayJoin(arr)/' "${WORKING_DIR}/metadata/db/t.sql"

# The table loads, otherwise the whole database would not.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "SELECT count() FROM db.t"

# Rebuilding the stored expression fails, both for an INSERT and for a TTL merge. Without the check the
# merge deleted both rows, including the one whose own TTL is a day away.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "INSERT INTO db.t VALUES (3, [now() + 100000])" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_TTL_EXPRESSION'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "OPTIMIZE TABLE db.t FINAL" 2>&1 >/dev/null | grep -o -m 1 -F 'BAD_TTL_EXPRESSION'
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "SELECT k FROM db.t ORDER BY k"

# Dropping the TTL is the way out of it, and it works.
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "ALTER TABLE db.t REMOVE TTL"
$CLICKHOUSE_LOCAL --path "${WORKING_DIR}" -q "OPTIMIZE TABLE db.t FINAL; SELECT k FROM db.t ORDER BY k"

rm -rf "${WORKING_DIR}"
