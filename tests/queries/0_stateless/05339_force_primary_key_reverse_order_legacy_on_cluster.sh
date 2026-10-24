#!/usr/bin/env bash
# Tags: distributed, no-replicated-database
# Tag no-replicated-database: ON CLUSTER is not allowed

# With `distributed_ddl_entry_format_version` below 3 the initiator ships the query text as written,
# so `force_primary_key_reverse_order` must be applied before the query is enqueued.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -e

# SQL UDF names are server-global, so the name is derived from the test database to keep parallel runs isolated.
udf="${CLICKHOUSE_DATABASE}_reverse_key"

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP FUNCTION IF EXISTS ${udf}"
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --multiquery --query "
SET distributed_ddl_output_mode = 'none';
SET force_primary_key_reverse_order = 1;

SET distributed_ddl_entry_format_version = 1;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_reverse_legacy_1 ON CLUSTER test_shard_localhost (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);

SET distributed_ddl_entry_format_version = 2;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_reverse_legacy_2 ON CLUSTER test_shard_localhost (a UInt32) ENGINE = MergeTree ORDER BY a;

-- A SQL UDF returning a tuple must be expanded before the rewrite, so that every element of the tuple becomes DESC,
-- as on the normal path. Otherwise the sorting key has one element, shorter than the explicit primary key.
CREATE FUNCTION ${udf} AS (x, y) -> tuple(x, y);
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_reverse_legacy_udf ON CLUSTER test_shard_localhost (a UInt32, b UInt32) ENGINE = MergeTree PRIMARY KEY (a, b) ORDER BY ${udf}(a, b);

SET distributed_ddl_entry_format_version = 6;
CREATE TABLE ${CLICKHOUSE_DATABASE}.t_reverse_current ON CLUSTER test_shard_localhost (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);

-- sorting_key in system.tables does not show the direction, so it is taken from the definition.
SELECT name, extract(create_table_query, 'ORDER BY (.+?) SETTINGS') FROM system.tables WHERE database = currentDatabase() AND name LIKE 't_reverse_%' ORDER BY name;
"
