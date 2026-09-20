#!/usr/bin/env bash
# Tags: no-fasttest

# Addresses https://github.com/ClickHouse/ClickHouse/issues/101905
# format_schema_source='query' should reject multi-row query results

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

PROTO_SCHEMA='syntax = "proto3"; message KVP { uint64 key = 1; string value = 2; }'

# Should fail because the schema query returns multiple rows
${CLICKHOUSE_LOCAL} --logger.console=0 --query "
CREATE TABLE test_data (key UInt64, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO test_data VALUES (1, 'abc');
SELECT * FROM test_data
SETTINGS
    format_schema_source = 'query',
    format_schema = 'SELECT ''${PROTO_SCHEMA}'' FROM numbers(3)',
    format_schema_message_name = 'KVP'
FORMAT Protobuf;
" 2>&1 | grep -o 'Expected the schema query result to have one row'

# Should succeed with a query that returns exactly one row
${CLICKHOUSE_LOCAL} --logger.console=0 --query "
CREATE TABLE test_data (key UInt64, value String) ENGINE = MergeTree() ORDER BY key;
INSERT INTO test_data VALUES (1, 'abc');
SELECT * FROM test_data
SETTINGS
    format_schema_source = 'query',
    format_schema = 'SELECT ''${PROTO_SCHEMA}''',
    format_schema_message_name = 'KVP'
FORMAT Protobuf;
" | ${CLICKHOUSE_LOCAL} --logger.console=0 --input-format Protobuf --structure 'key UInt64, value String' --query "
SELECT * FROM table
SETTINGS
    format_schema_source = 'query',
    format_schema = 'SELECT ''${PROTO_SCHEMA}''',
    format_schema_message_name = 'KVP';
"
