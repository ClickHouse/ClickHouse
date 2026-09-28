#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no `NATS`.
#
# A `NATS` table that states no authentication of its own authenticates with the server's `<nats>` config
# section, and `system.table_settings` reports those values with source `config`. They are the operator's, the
# username as much as the password, not what a query stated: `SHOW CREATE TABLE` never printed them, so
# `displaySecretsInShowAndSelect` - which reveals the secrets a query states - must not reveal them either. The table
# stating its own credentials is the control: the same reader sees those, so the config values are hidden by their
# source and not because nothing can be shown.
# A macro from the server configuration expanded into a secret the table states is the server's in the same way.
#
# `clickhouse-local` with a config file, because the stateless test server does not enable
# `display_secrets_in_show_and_select`. The tables are attached rather than created: an attached `NATS` table
# does not fail when its broker is unreachable, and `127.0.0.1:1` refuses at once. An `Atomic` database attaches
# a table with a definition only under a `UUID`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONFIG="${CLICKHOUSE_TMP}/config_secret_is_never_shown.xml"
cat > "$CONFIG" <<'EOF'
<clickhouse>
    <display_secrets_in_show_and_select>1</display_secrets_in_show_and_select>
    <nats>
        <user>config_user</user>
        <password>leak05260configpw</password>
    </nats>
    <macros>
        <nats_pw>leak05260macropw</nats_pw>
        <amqp_pw>leak05260amqppw</amqp_pw>
    </macros>
</clickhouse>
EOF

NATS_SETTINGS="nats_url = '127.0.0.1:1', nats_subjects = 's', nats_format = 'CSV',
    nats_startup_connect_tries = 1, nats_reconnect_wait = 1"

$CLICKHOUSE_LOCAL --config-file "$CONFIG" --format_display_secrets_in_show_and_select 1 -q "
ATTACH TABLE from_config UUID '05260000-0000-4000-8000-000000000001' (a UInt64) ENGINE = NATS SETTINGS ${NATS_SETTINGS};
ATTACH TABLE own UUID '05260000-0000-4000-8000-000000000002' (a UInt64) ENGINE = NATS SETTINGS ${NATS_SETTINGS},
    nats_username = 'table_user', nats_password = 'table_password';
SELECT table, name, value, is_masked, source FROM system.table_settings
WHERE name IN ('nats_username', 'nats_password') ORDER BY table, name;"

echo "-- a macro from the server configuration, expanded into a secret the table states, is the server's too"
# `SHOW CREATE TABLE` prints `{nats_pw}`; the engine works with, and would report, what the macro holds. The
# `RabbitMQ` address with a credential of its own is the control: the same reader sees that one.
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --format_display_secrets_in_show_and_select 1 -q "
ATTACH TABLE from_macro UUID '05260000-0000-4000-8000-000000000003' (a UInt64) ENGINE = NATS SETTINGS ${NATS_SETTINGS},
    nats_username = 'table_user', nats_password = '{nats_pw}';
CREATE TABLE address_from_macro (a UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_address = 'amqp://u:{amqp_pw}@127.0.0.1:1/v', rabbitmq_exchange_name = 'x', rabbitmq_format = 'CSV';
CREATE TABLE address_stated (a UInt64) ENGINE = RabbitMQ
    SETTINGS rabbitmq_address = 'amqp://u:stated_pw@127.0.0.1:1/v', rabbitmq_exchange_name = 'x', rabbitmq_format = 'CSV';
SELECT table, name, value, is_masked, source FROM system.table_settings
WHERE name IN ('nats_password', 'rabbitmq_address') ORDER BY table, name;" -- --message_queue_disable_insertion=1

echo "-- a secret a table states without a macro stays visible, even where the engine reformats it"
# The engine reports the server list it parsed, `a:1,b:2`, not the stated text; what decides is whether a macro
# supplied any of it, which none did here.
$CLICKHOUSE_LOCAL --config-file "$CONFIG" --format_display_secrets_in_show_and_select 1 -q "
ATTACH TABLE list_stated UUID '05260000-0000-4000-8000-000000000004' (a UInt64) ENGINE = NATS
    SETTINGS nats_server_list = 'a:1, b:2', nats_subjects = 's', nats_format = 'CSV';
SELECT table, name, value, is_masked, source FROM system.table_settings
WHERE table = 'list_stated' AND name = 'nats_server_list';" -- --message_queue_disable_insertion=1

rm -f "$CONFIG"
