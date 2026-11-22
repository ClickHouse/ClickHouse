#!/usr/bin/env bash
# The explicit legacy spelling must be persisted for every definition a user supplies, including one
# whose key expression only becomes visible after SQL UDF substitution and one supplied by a
# full-definition ATTACH. Definitions replayed from stored metadata must be left alone.
# A generated UUID and database-scoped names keep this runnable in parallel with itself.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UDF="totime_04909_${CLICKHOUSE_DATABASE}"
UUID=$(${CLICKHOUSE_CLIENT} -q "SELECT generateUUIDv4()")

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
CREATE FUNCTION ${UDF} AS x -> toTime(x);

-- The key expression arrives through a SQL UDF body, so it exists only after substitution.
CREATE TABLE t_udf_key (c0 DateTime) ENGINE = MergeTree() ORDER BY ${UDF}(c0);
SELECT 'udf', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_udf_key';

-- A database-qualified target is the same fresh definition as an unqualified one.
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_qualified_key (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
SELECT 'qualified', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_qualified_key';

-- AS materializes the source definition during CREATE normalization. Rewrite that copied key too.
CREATE TABLE t_as_source (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
CREATE TABLE t_as_key AS t_as_source;
SELECT 'as', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_as_key';
"

# A full-definition ATTACH is CREATE-like user input and persists what it is given. The server warns
# that the form is not recommended, which is expected here.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 -q "
ATTACH TABLE t_attach_key UUID '${UUID}' (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
" 2>/dev/null
${CLICKHOUSE_CLIENT} -q "
SELECT 'attach', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_attach_key';
"

# The explicit legacy spelling is what makes the physical key type independent of the reloading
# session: it must still read as DateTime after a replay under the default setting.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --describe_compact_output 1 --multiquery -q "
CREATE TABLE t_replayed_key (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
INSERT INTO t_replayed_key SELECT toDateTime('2024-01-01 12:34:56');
SELECT 'stored_key_type';
DESCRIBE mergeTreeIndex(currentDatabase(), t_replayed_key);
DETACH TABLE t_replayed_key;
"

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 0 --describe_compact_output 1 --multiquery -q "
ATTACH TABLE t_replayed_key;
SELECT 'replayed', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_replayed_key';
SELECT 'replayed_key_type';
DESCRIBE mergeTreeIndex(currentDatabase(), t_replayed_key);
"

# A TTL GROUP BY key must stay a prefix of the primary key, so both have to be rewritten together.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
CREATE TABLE t_ttl_key (c0 DateTime, v UInt32) ENGINE = MergeTree() ORDER BY toTime(c0)
TTL c0 + INTERVAL 1 DAY GROUP BY toTime(c0) SET v = max(v);
SELECT 'ttl_group_by', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_ttl_key';
"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP TABLE t_udf_key;
DROP TABLE t_attach_key;
DROP TABLE t_qualified_key;
DROP TABLE t_as_key;
DROP TABLE t_as_source;
DROP TABLE t_replayed_key;
DROP TABLE t_ttl_key;
DROP FUNCTION ${UDF};
"
