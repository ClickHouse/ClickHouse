#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.distributed_ddl_queue` replays the entry that every host of the cluster executes, so the
# entry in Keeper keeps the real values of the statement and of the settings it was issued with. Both
# columns must still hide the credentials when the table is read.
#
# The statement carries its secret in a settings profile element rather than in a table engine, so
# that the test does not depend on an engine being compiled in.
QUERY_CANARY="c05057ddlqueuequery"
SETTINGS_CANARY="c05057ddlqueuesettings"
PROFILE="p05057_$CLICKHOUSE_DATABASE"

${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&distributed_ddl_output_mode=none&url_base=https%3A%2F%2Fu%3A$SETTINGS_CANARY%40example.com%2Fd%2F" \
    --data-binary "CREATE SETTINGS PROFILE $PROFILE ON CLUSTER test_shard_localhost
        SETTINGS format_avro_schema_registry_url = 'http://u:$QUERY_CANARY@reg:8080/'"

$CLICKHOUSE_CLIENT -q "SELECT
        countIf(position(query, '[HIDDEN]') > 0) > 0,
        countIf(position(query, '$QUERY_CANARY') > 0),
        countIf(position(settings['url_base'], '[HIDDEN]') > 0) > 0,
        countIf(position(settings['url_base'], '$SETTINGS_CANARY') > 0)
    FROM system.distributed_ddl_queue
    WHERE position(query, '$PROFILE') > 0 AND position(query, 'CREATE') > 0"

$CLICKHOUSE_CLIENT -q "DROP SETTINGS PROFILE IF EXISTS $PROFILE ON CLUSTER test_shard_localhost" > /dev/null
