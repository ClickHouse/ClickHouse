#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: the fast test build has no `NATS`.
#
# A `NATS` table that states no authentication of its own authenticates with the server's `<nats>` config
# section, and `system.table_settings` reports that value with source `config`. It is the operator's secret, not
# one a query stated: `SHOW CREATE TABLE` never printed it, so `displaySecretsInShowAndSelect` - which reveals the
# secrets a query states - must not reveal it either. The table stating its own credentials is the control: the
# same reader sees those, so the config secret is hidden by its source and not because no secret can be shown.
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

rm -f "$CONFIG"
