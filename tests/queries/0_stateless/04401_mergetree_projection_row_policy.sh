#!/usr/bin/env bash
# Tags: no-fasttest

# A SELECT row policy on a MergeTree table must not be bypassable by reading the
# table's projection or index data directly via mergeTreeProjection / mergeTreeIndex.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# User is a global object, so make its name unique per parallel run.
user="user_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user} NOT IDENTIFIED"

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE users (id UInt64, name String, department String, salary UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO users VALUES (1,'Alice','engineering',100000),(2,'Bob','finance',120000),(3,'Carol','engineering',110000),(4,'Dave','hr',90000);
-- Projection that drops the policy column 'department' on purpose: the column-overlap
-- heuristic would let it through, but whole-row data still leaks, so the read must be denied.
ALTER TABLE users ADD PROJECTION proj (SELECT id, name, salary ORDER BY id);
-- mutations_sync=2: MATERIALIZE PROJECTION is async by default, so without it the
-- direct projection read below can race the mutation and see no projection part.
ALTER TABLE users MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2;
OPTIMIZE TABLE users FINAL;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.users TO ${user};
CREATE ROW POLICY rp ON users FOR SELECT USING department = 'engineering' TO ${user};
"

echo "=== baseline: row policy is enforced on a normal SELECT ==="
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT name FROM ${CLICKHOUSE_DATABASE}.users ORDER BY id"

echo "=== mergeTreeProjection with a row policy: denied ==="
${CLICKHOUSE_CLIENT} --user "${user}" -q "
SELECT name, salary FROM mergeTreeProjection(currentDatabase(), users, proj)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== mergeTreeProjection without a row policy: allowed ==="
# A separate table with no row policy at all: the guard must let the projection read through.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE users_open (id UInt64, name String, department String, salary UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO users_open VALUES (1,'Alice','engineering',100000),(2,'Bob','finance',120000),(3,'Carol','engineering',110000),(4,'Dave','hr',90000);
ALTER TABLE users_open ADD PROJECTION proj (SELECT id, name, salary ORDER BY id);
ALTER TABLE users_open MATERIALIZE PROJECTION proj SETTINGS mutations_sync = 2;
OPTIMIZE TABLE users_open FINAL;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.users_open TO ${user};
"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() FROM mergeTreeProjection(currentDatabase(), users_open, proj)"

# Partition by id so that with_minmax exposes the minmax_id column derived from it.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE idx (id UInt64, secret UInt64) ENGINE = MergeTree() PARTITION BY intDiv(id, 50) ORDER BY id;
INSERT INTO idx SELECT number, number * 1000 FROM numbers(100);
OPTIMIZE TABLE idx FINAL;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.idx TO ${user};
CREATE ROW POLICY rp_idx ON idx FOR SELECT USING id < 10 TO ${user};
"

echo "=== mergeTreeIndex with a row policy: denied (primary key column read) ==="
${CLICKHOUSE_CLIENT} --user "${user}" -q "
SELECT id FROM mergeTreeIndex(currentDatabase(), idx)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== mergeTreeIndex with a row policy: denied (with_minmax exposes minmax_id) ==="
# minmax_<col> derives from a source column the policy filters on, so reading it leaks
# min/max over hidden rows even though minmax_id is not itself a source column.
${CLICKHOUSE_CLIENT} --user "${user}" -q "
SELECT minmax_id FROM mergeTreeIndex(currentDatabase(), idx, with_minmax = 1)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== mergeTreeIndex with a row policy: denied (part metadata, no policy column requested) ==="
# part_name/granule metadata still reveals that hidden rows exist, so it must be denied too.
${CLICKHOUSE_CLIENT} --user "${user}" -q "
SELECT part_name FROM mergeTreeIndex(currentDatabase(), idx)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

${CLICKHOUSE_CLIENT} -q "
CREATE TABLE idx_all_hidden (id UInt64, secret UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO idx_all_hidden SELECT number, number * 1000 FROM numbers(100);
OPTIMIZE TABLE idx_all_hidden FINAL;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.idx_all_hidden TO ${user};
CREATE ROW POLICY rp_all_hidden ON idx_all_hidden FOR SELECT USING 0 TO ${user};
"

echo "=== mergeTreeIndex with a column-independent policy (USING 0): denied ==="
# USING 0 references no column, so a column-overlap check would wrongly allow it
# even though the policy hides every row.
${CLICKHOUSE_CLIENT} --user "${user}" -q "
SELECT part_name FROM mergeTreeIndex(currentDatabase(), idx_all_hidden)
" 2>&1 | grep -o 'ACCESS_DENIED' | head -1

echo "=== mergeTreeIndex without a row policy: allowed ==="
# A separate table with no row policy at all: the guard must let the index read through.
${CLICKHOUSE_CLIENT} -q "
CREATE TABLE idx_open (id UInt64, secret UInt64) ENGINE = MergeTree() ORDER BY id;
INSERT INTO idx_open SELECT number, number * 1000 FROM numbers(100);
OPTIMIZE TABLE idx_open FINAL;
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.idx_open TO ${user};
"
${CLICKHOUSE_CLIENT} --user "${user}" -q "SELECT count() > 0 FROM mergeTreeIndex(currentDatabase(), idx_open, with_minmax = 1)"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
