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
"

# A full-definition ATTACH is CREATE-like user input and persists what it is given. The server warns
# that the form is not recommended, which is expected here.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 -q "
ATTACH TABLE t_attach_key UUID '${UUID}' (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
" 2>/dev/null
${CLICKHOUSE_CLIENT} -q "
SELECT 'attach', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_attach_key';
"

# A definition stored without the legacy setting must survive a replay under it unchanged.
${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 0 --multiquery -q "
CREATE TABLE t_replayed_key (c0 DateTime) ENGINE = MergeTree() ORDER BY toTime(c0);
INSERT INTO t_replayed_key SELECT toDateTime('2024-01-01 12:34:56');
DETACH TABLE t_replayed_key;
"

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
ATTACH TABLE t_replayed_key;
SELECT 'replayed', sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_replayed_key';
SELECT 'replayed_rows', count() FROM t_replayed_key;
"

${CLICKHOUSE_CLIENT} --multiquery -q "
DROP TABLE t_udf_key;
DROP TABLE t_attach_key;
DROP TABLE t_qualified_key;
DROP TABLE t_replayed_key;
DROP FUNCTION ${UDF};
"
