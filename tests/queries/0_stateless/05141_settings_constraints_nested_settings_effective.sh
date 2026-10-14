#!/usr/bin/env bash
# Tags: no-old-analyzer
# (the clamp is analyzer-path behaviour; the legacy interpreter throws on such a nested clause)
# A nested `SETTINGS` clause is clamped to the session's settings constraints, and the clamped clause is
# what the query node keeps. So a nested `use_query_cache = 1` the constraints dropped must not enable
# the query result cache in the `Planner` (issue #117226), a shard receives the clamped clause, and an
# unknown name or an uncastable value in a nested clause throws, as it does in a top-level clause - also
# when the clause sits in a view's stored body and the view is read on a remote shard.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Users and settings profiles are server-global, so their names carry the test database to keep this
# test safe against a concurrent copy of itself - which is how the flaky check runs it.
SUFFIX="05141_${CLICKHOUSE_DATABASE}"
CONST_USER="user_const_${SUFFIX}"
CONST_PROFILE="profile_const_${SUFFIX}"
RO_USER="user_ro_${SUFFIX}"
RO_PROFILE="profile_ro_${SUFFIX}"
FREE_USER="user_free_${SUFFIX}"
DIST_USER="user_dist_${SUFFIX}"
DIST_PROFILE="profile_dist_${SUFFIX}"
TABLE="t_05141"
DIST_TABLE="dist_05141"
VIEW="v_05141"
DIST_VIEW="dist_v_05141"
PARAM_VIEW="pv_05141"
# `system.query_cache` lists the entries of every user and every test, and the cache is never dropped
# here, so each cached subquery carries a per-run marker in its text and is looked up by it.
MARKER="marker_${SUFFIX}"

${CLICKHOUSE_CLIENT} --query "
DROP USER IF EXISTS ${CONST_USER}, ${RO_USER}, ${FREE_USER}, ${DIST_USER};
DROP SETTINGS PROFILE IF EXISTS ${CONST_PROFILE}, ${RO_PROFILE}, ${DIST_PROFILE};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${DIST_TABLE};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${TABLE};
DROP TABLE IF EXISTS ${CLICKHOUSE_DATABASE}.${DIST_VIEW};
DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.${VIEW};
DROP VIEW IF EXISTS ${CLICKHOUSE_DATABASE}.${PARAM_VIEW};

CREATE TABLE ${CLICKHOUSE_DATABASE}.${TABLE} (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.${TABLE} VALUES (1, 'a'), (2, 'b'), (3, 'c');
CREATE TABLE ${CLICKHOUSE_DATABASE}.${DIST_TABLE} AS ${CLICKHOUSE_DATABASE}.${TABLE}
    ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), ${TABLE});

CREATE SETTINGS PROFILE ${CONST_PROFILE} SETTINGS use_query_cache = 0 CONST;
CREATE SETTINGS PROFILE ${RO_PROFILE} SETTINGS readonly = 1 CONST;
CREATE SETTINGS PROFILE ${DIST_PROFILE} SETTINGS max_rows_to_read MAX 2;
CREATE USER ${CONST_USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${CONST_PROFILE};
CREATE USER ${RO_USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${RO_PROFILE};
CREATE USER ${FREE_USER} IDENTIFIED WITH no_password;
CREATE USER ${DIST_USER} IDENTIFIED WITH no_password SETTINGS PROFILE ${DIST_PROFILE};
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${CONST_USER}, ${RO_USER}, ${FREE_USER}, ${DIST_USER};
"

# `CREATE` does not analyze the body of a view with an explicit column list nor of a parameterized view, so
# such definitions can carry an invalid nested setting.
${CLICKHOUSE_CLIENT} --query "
CREATE VIEW ${CLICKHOUSE_DATABASE}.${VIEW} (x UInt8) AS SELECT * FROM (SELECT 1 AS x SETTINGS max_threds = 1);
CREATE TABLE ${CLICKHOUSE_DATABASE}.${DIST_VIEW} (x UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), ${VIEW});
CREATE VIEW ${CLICKHOUSE_DATABASE}.${PARAM_VIEW} AS SELECT * FROM (SELECT 1 AS x SETTINGS max_threds = 1) WHERE x = {p:UInt8};
"

# The bare binary, not ${CLICKHOUSE_CLIENT}: the harness's randomized settings would be rejected for the readonly user.
RESTRICTED_CLIENT="${CLICKHOUSE_CLIENT_BINARY} --host=${CLICKHOUSE_HOST} --port=${CLICKHOUSE_PORT_TCP} --database=${CLICKHOUSE_DATABASE}"
CONST="${RESTRICTED_CLIENT} --user=${CONST_USER}"
RO="${RESTRICTED_CLIENT} --user=${RO_USER}"
FREE="${RESTRICTED_CLIENT} --user=${FREE_USER}"
DIST="${RESTRICTED_CLIENT} --user=${DIST_USER}"

# Cache entries whose text carries the marker: subquery entries, then top-level entries.
function cache_entries()
{
    ${CLICKHOUSE_CLIENT} --query "SELECT countIf(is_subquery), countIf(NOT is_subquery) FROM system.query_cache WHERE query LIKE '%$1%'"
}

# The marker is compared against a real column so it survives constant folding and stays in the
# subquery text the cache entry is keyed and listed by.
echo "-- use_query_cache = 0 CONST: a nested use_query_cache = 1 runs but caches nothing"
${CONST} --query "SELECT count() FROM (SELECT id FROM ${TABLE} WHERE s != '${MARKER}_const' SETTINGS use_query_cache = 1)"
cache_entries "${MARKER}_const"

echo "-- readonly = 1: a nested use_query_cache = 1 runs but caches nothing"
${RO} --query "SELECT count() FROM (SELECT id FROM ${TABLE} WHERE s != '${MARKER}_ro' SETTINGS use_query_cache = 1)"
cache_entries "${MARKER}_ro"

echo "-- unconstrained: a nested use_query_cache = 1 caches the subquery"
${FREE} --query "SELECT count() FROM (SELECT id FROM ${TABLE} WHERE s != '${MARKER}_free' SETTINGS use_query_cache = 1)"
cache_entries "${MARKER}_free"

echo "-- unconstrained: a nested use_query_cache = 1 equal to the outer value still caches the subquery, next to the outer query"
${FREE} --query "SELECT count() FROM (SELECT id FROM ${TABLE} WHERE s != '${MARKER}_both' SETTINGS use_query_cache = 1) SETTINGS use_query_cache = 1"
cache_entries "${MARKER}_both"

echo "-- unconstrained: a nested use_query_cache = 'true' given as a string is applied"
${FREE} --query "SELECT count() FROM (SELECT id FROM ${TABLE} WHERE s != '${MARKER}_string' SETTINGS use_query_cache = 'true')"
cache_entries "${MARKER}_string"

echo "-- a nested unknown setting still throws"
${CONST} --query "SELECT count() FROM (SELECT id FROM ${TABLE} SETTINGS max_threds = 1)" 2>&1 | grep -c -F "UNKNOWN_SETTING"

echo "-- a nested value the setting cannot take still throws"
${CONST} --query "SELECT count() FROM (SELECT id FROM ${TABLE} SETTINGS max_threads = 'abc')" 2>&1 | grep -c -F "CANNOT_PARSE_INPUT_ASSERTION_FAILED"

# The IN subquery runs on the shards, whose user has no constraints. It must arrive with the value the
# initiator clamped (2, so reading 3 rows fails there), not with the value as written.
echo "-- a nested clause forwarded to the shards carries the clamped value"
${DIST} --query "SELECT count() FROM ${DIST_TABLE} WHERE id IN (SELECT id FROM ${TABLE} SETTINGS max_rows_to_read = 100)" 2>&1 | grep -c -F "TOO_MANY_ROWS"

echo "-- the same query for an unconstrained user reads both shards"
${FREE} --query "SELECT count() FROM ${DIST_TABLE} WHERE id IN (SELECT id FROM ${TABLE} SETTINGS max_rows_to_read = 100)"

# The bare binary again: `${CLICKHOUSE_CLIENT}` adds `--send_logs_level`, so the server's error log line would
# be printed too and `grep -c` would count two lines.
echo "-- a view with an invalid nested setting in its stored body fails on a direct read"
${RESTRICTED_CLIENT} --query "SELECT * FROM ${VIEW}" 2>&1 | grep -c -F "UNKNOWN_SETTING"

# `prefer_localhost_replica = 0` is explicit: the local shard is analyzed under the initiator's query kind
# and already throws, so only the remote path exercises the fix, and the harness randomizes this setting.
echo "-- and on a remote shard through Distributed"
${RESTRICTED_CLIENT} --query "SELECT * FROM ${DIST_VIEW} SETTINGS prefer_localhost_replica = 0" 2>&1 | grep -c -F "UNKNOWN_SETTING"

echo "-- also when the shard inlines the view"
${RESTRICTED_CLIENT} --query "SELECT * FROM ${DIST_VIEW} SETTINGS prefer_localhost_replica = 0, analyzer_inline_views = 1" 2>&1 | grep -c -F "UNKNOWN_SETTING"

echo "-- a parameterized view with an invalid nested setting fails on a direct read"
${RESTRICTED_CLIENT} --query "SELECT * FROM ${PARAM_VIEW}(p = 1)" 2>&1 | grep -c -F "UNKNOWN_SETTING"

# 127.0.0.2 is the non-local shard of `test_cluster_two_shards`; the port is explicit so the test runs on
# a non-default port; the database is qualified because the remote session does not inherit the test database.
echo "-- and on a remote shard"
${RESTRICTED_CLIENT} --query "SELECT * FROM remote('127.0.0.2:${CLICKHOUSE_PORT_TCP}', view(SELECT * FROM ${CLICKHOUSE_DATABASE}.${PARAM_VIEW}(p = 1)))" 2>&1 | grep -c -F "UNKNOWN_SETTING"

echo "-- the session settings are untouched"
${CONST} --query "SELECT getSetting('use_query_cache')"

${CLICKHOUSE_CLIENT} --query "
DROP TABLE ${CLICKHOUSE_DATABASE}.${DIST_TABLE};
DROP TABLE ${CLICKHOUSE_DATABASE}.${TABLE};
DROP TABLE ${CLICKHOUSE_DATABASE}.${DIST_VIEW};
DROP VIEW ${CLICKHOUSE_DATABASE}.${VIEW};
DROP VIEW ${CLICKHOUSE_DATABASE}.${PARAM_VIEW};
DROP USER ${CONST_USER}, ${RO_USER}, ${FREE_USER}, ${DIST_USER};
DROP SETTINGS PROFILE ${CONST_PROFILE}, ${RO_PROFILE}, ${DIST_PROFILE};
"
