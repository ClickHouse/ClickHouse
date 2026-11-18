#!/usr/bin/env bash
# Tags: shard

# A parameterized view referenced unqualified in a query that is shipped to a shard used to arrive
# without its database, so the shard re-resolved the name against its own default database. The
# session database must be the view's own database and the call must be unqualified, or nothing is
# measured. `enable_analyzer` is pinned because the parameterized view is only represented as a
# resolved table function node in the analyzer.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --enable_analyzer 1"

# A same-named view in `default` is what the shard falls back to, so it turns the failure from a
# hard error into a silently wrong answer. Its name carries the test database to stay unique.
shadow="pv_${CLICKHOUSE_DATABASE}"

${CLIENT} -q "
CREATE TABLE src (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO src SELECT 't1', number FROM numbers(3);

CREATE TABLE local_data (tenant_id String, v UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO local_data SELECT 't1', number FROM numbers(3);

CREATE TABLE dist AS local_data
    ENGINE = Distributed('test_cluster_two_shards', '${CLICKHOUSE_DATABASE}', 'local_data', rand());

CREATE VIEW ${shadow} AS SELECT tenant_id, host_id FROM src WHERE tenant_id IN {tenants:Array(String)};
CREATE VIEW plain AS SELECT tenant_id, host_id FROM src;
"

echo '-- non-GLOBAL IN through a Distributed table'
${CLIENT} -q "SELECT count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM ${shadow}(tenants = ['t1']))"

echo '-- JOIN ... ON'
${CLIENT} -q "SELECT count() FROM dist AS d JOIN ${shadow}(tenants = ['t1']) AS p ON d.tenant_id = p.tenant_id"

echo '-- JOIN ... USING'
${CLIENT} -q "SELECT count() FROM dist JOIN ${shadow}(tenants = ['t1']) USING (tenant_id) SETTINGS joined_subquery_requires_alias = 0"

echo '-- remote()'
${CLIENT} -q "SELECT count() FROM remote('127.0.0.2', '${CLICKHOUSE_DATABASE}', 'local_data') WHERE tenant_id IN (SELECT tenant_id FROM ${shadow}(tenants = ['t1']))"

echo '-- a same-named view in the database the shard falls back to must not shadow it'
${CLIENT} -q "
CREATE TABLE default.src_${CLICKHOUSE_DATABASE} (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO default.src_${CLICKHOUSE_DATABASE} VALUES ('zzz_never', 999);
CREATE VIEW default.${shadow} AS
    SELECT tenant_id, host_id FROM default.src_${CLICKHOUSE_DATABASE} WHERE tenant_id IN {tenants:Array(String)};
"

echo '-- per-shard rows, one local replica'
${CLIENT} -q "SELECT _shard_num, count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM ${shadow}(tenants = ['t1'])) GROUP BY _shard_num ORDER BY _shard_num"

echo '-- per-shard rows, every replica remote'
${CLIENT} -q "SELECT _shard_num, count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM ${shadow}(tenants = ['t1'])) GROUP BY _shard_num ORDER BY _shard_num SETTINGS prefer_localhost_replica = 0"

echo '-- per-shard rows without any view, the denominator'
${CLIENT} -q "SELECT _shard_num, count() FROM dist GROUP BY _shard_num ORDER BY _shard_num"

echo '-- CONTROL a regular table function keeps its unqualified name'
${CLIENT} -q "SELECT count() FROM dist WHERE 1 IN (SELECT number FROM numbers(3))"

echo '-- CONTROL GLOBAL IN'
${CLIENT} -q "SELECT count() FROM dist WHERE tenant_id GLOBAL IN (SELECT tenant_id FROM ${shadow}(tenants = ['t1']))"

echo '-- CONTROL an ordinary view'
${CLIENT} -q "SELECT count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM plain)"

echo '-- CONTROL an already qualified call'
${CLIENT} -q "SELECT count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM ${CLICKHOUSE_DATABASE}.${shadow}(tenants = ['t1']))"

echo '-- CONTROL an unresolved parse renders unchanged'
${CLIENT} -q "SELECT formatQuery('SELECT * FROM pv(x = 1)')"

${CLIENT} -q "
DROP VIEW default.${shadow};
DROP TABLE default.src_${CLICKHOUSE_DATABASE};
"

# A database whose name contains a dot has no resolvable qualified spelling, because
# extractDatabaseAndTableNameForParameterizedView reads only one or two identifier parts. Such a
# name must be left exactly as the user wrote it, so the failure keeps naming the view alone.
dotted="${CLICKHOUSE_DATABASE}.sub"
${CLIENT} -q "
CREATE DATABASE \`${dotted}\`;
CREATE TABLE \`${dotted}\`.src (tenant_id String, host_id UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO \`${dotted}\`.src SELECT 't1', number FROM numbers(3);
CREATE TABLE \`${dotted}\`.local_data (tenant_id String, v UInt64) ENGINE = MergeTree ORDER BY tenant_id;
INSERT INTO \`${dotted}\`.local_data SELECT 't1', number FROM numbers(3);
CREATE TABLE \`${dotted}\`.dist AS \`${dotted}\`.local_data
    ENGINE = Distributed('test_cluster_two_shards', '${dotted}', 'local_data', rand());
CREATE VIEW \`${dotted}\`.pvd AS
    SELECT tenant_id, host_id FROM \`${dotted}\`.src WHERE tenant_id IN {tenants:Array(String)};
"

DOTTED_CLIENT="${CLIENT//--database=${CLICKHOUSE_DATABASE}/--database=${dotted}}"

echo '-- a dotted database name still resolves locally'
${DOTTED_CLIENT} -q "SELECT count() FROM pvd(tenants = ['t1'])"

echo '-- and is not qualified for the shard, so it keeps failing under its own name'
${DOTTED_CLIENT} -q "SELECT count() FROM dist WHERE tenant_id IN (SELECT tenant_id FROM pvd(tenants = ['t1']))" 2>&1 \
    | grep -oE 'Unknown table function [^ ]*' | head -n 1

${CLIENT} -q "DROP DATABASE \`${dotted}\`"
