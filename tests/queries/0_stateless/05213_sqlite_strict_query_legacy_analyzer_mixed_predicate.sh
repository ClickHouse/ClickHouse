#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: requires the SQLite library, which is not built in the fast test.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DB="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_sqlite_strict_legacy_mixed.sqlite3"
trap 'rm -f "$DB"' EXIT
rm -f "$DB"

sqlite3 "$DB" "CREATE TABLE t1(id INTEGER PRIMARY KEY, name TEXT);"
sqlite3 "$DB" "INSERT INTO t1 VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd');"

# A query-backed source is passed to SQLite as is, so under `external_table_strict_query = 1` every outer
# filter over the source's columns has to be rejected. On the legacy analyzer path the guard prunes the
# predicates of other joined tables before looking for a filter on the source. A predicate on the source
# that shares its subtree with a foreign child - `id IN (SELECT ... FROM other)`, `id = 1 OR other.flag`,
# `NOT (id = 1 AND other.flag)` - is dropped by that pruning as a whole; it must still be rejected, because
# it is evaluated locally over the source's rows exactly like a plain `id = 1`.
${CLICKHOUSE_LOCAL} --multiquery --query="
SET enable_analyzer = 0;

CREATE TABLE local_r (id Int64, flag UInt8) ENGINE = Memory;
INSERT INTO local_r VALUES (1, 1), (2, 0);

SELECT '-- a source predicate mixed with a subquery is rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) WHERE id IN (SELECT id FROM local_r WHERE flag = 1) SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) WHERE id IN (SELECT id FROM local_r WHERE flag = 1) SETTINGS external_table_strict_query = 0;

SELECT '-- a disjunction mixing the source and the joined local side is rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 OR r.flag SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 OR r.flag SETTINGS external_table_strict_query = 0;

SELECT '-- a negated conjunction mixing the source and the joined local side is rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE NOT (l.id = 1 AND r.flag) SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE NOT (l.id = 1 AND r.flag) SETTINGS external_table_strict_query = 0;

SELECT '-- a plain source predicate is still rejected';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE l.id = 1 SETTINGS external_table_strict_query = 1; -- { serverError INCORRECT_QUERY }

SELECT '-- a predicate on the joined local side alone is not a filter on the source';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.flag SETTINGS external_table_strict_query = 1;
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) AS l LEFT JOIN local_r AS r USING (id) WHERE r.flag = 1 OR r.id = 2 SETTINGS external_table_strict_query = 1;

SELECT '-- no outer filter is allowed';
SELECT count() FROM sqlite('${DB}', query('SELECT id, name FROM t1')) SETTINGS external_table_strict_query = 1;

DROP TABLE local_r;
"
