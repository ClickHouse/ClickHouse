#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no `Kafka`, `NATS` or `RabbitMQ`, whose secret settings the test expects to find

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `system.engine_settings` and `system.merge_tree_settings` print `value` unmasked. That is safe only while
# every secret engine setting has an empty default, and `MergeTree` and `Distributed`, which also read the
# server configuration, have no secret settings. Nothing else enforces it, so check it here.
#
# The list of secret settings, shared by `SHOW CREATE TABLE` and `system.table_settings`, is not readable
# from SQL, but `query_log` stores a query with those secrets masked. So one query assigns every engine
# setting a probe that each masking rule changes - the whole value, a URI password or a connection string
# key - and a setting whose assignment is not logged verbatim is secret.

probe="u://probe_user:probe_secret@h/?AccountKey=probe_secret"
query_id="${CLICKHOUSE_DATABASE}_engine_settings_secrets_${RANDOM}${RANDOM}"

assignments=$($CLICKHOUSE_CLIENT -q "
    SELECT arrayStringConcat(arrayMap(name -> concat(name, ' = ''$probe'''), groupUniqArray(name)), ', ')
    FROM system.engine_settings
    FORMAT TSVRaw")

# The table does not exist, so the query fails - it only has to reach `query_log`.
$CLICKHOUSE_CLIENT --query_id "$query_id" --max_query_size 100000000 --log_queries_cut_to_length 100000000 \
    -q "ALTER TABLE ${CLICKHOUSE_DATABASE}.missing_table MODIFY SETTING $assignments" >/dev/null 2>&1

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

$CLICKHOUSE_CLIENT -q "
    CREATE VIEW probed_settings AS
    WITH (
        SELECT any(query) FROM system.query_log
        WHERE event_date >= yesterday() AND current_database = currentDatabase()
            AND query_id = '$query_id' AND type != 'QueryStart'
    ) AS logged
    SELECT
        engine_name, name, value, default,
        position(logged, concat(' ', name, ' = ')) > 0 AS logged_at_all,
        position(logged, concat(' ', name, ' = ''$probe''')) = 0 AS secret
    FROM system.engine_settings"

$CLICKHOUSE_CLIENT -q "
    SELECT 'every assignment is logged', countIf(NOT logged_at_all) = 0 FROM probed_settings;
    SELECT 'known secrets are found', hasAll(groupUniqArrayIf(name, secret),
        ['kafka_sasl_password', 'nats_url', 'rabbitmq_address', 'after_processing_move_connection_string']) FROM probed_settings;
    SELECT 'an ordinary setting is not taken for a secret', NOT has(groupUniqArrayIf(name, secret), 'index_granularity') FROM probed_settings;
    SELECT 'secrets with a non-empty default or value:';
    SELECT engine_name, name, value, default FROM probed_settings WHERE secret AND (value != '' OR default != '') ORDER BY ALL;"
