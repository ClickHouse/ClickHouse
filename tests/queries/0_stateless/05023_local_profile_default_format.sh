#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A `default_format` configured in the default profile must keep driving the output format of
# `clickhouse-local`, while the synthetic `TabSeparated` seed for the sessions of the embedded
# protocol listeners must not: the seed is reset on the client session, or the interactive
# display default would be `TSV` instead of `PrettyCompact`.

CONFIG_DIR=$CLICKHOUSE_TMP/05023_config
mkdir -p "$CONFIG_DIR"

cat > "$CONFIG_DIR/config.xml" <<EOF
<clickhouse>
    <users_config>users.xml</users_config>
</clickhouse>
EOF

cat > "$CONFIG_DIR/users.xml" <<EOF
<clickhouse>
    <profiles>
        <default>
            <default_format>JSONEachRow</default_format>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
EOF

# The profile value formats the batch output.
$CLICKHOUSE_LOCAL --config-file "$CONFIG_DIR/config.xml" -q "SELECT 1 AS x"

# And it stays visible in the session as an ordinary changed setting.
$CLICKHOUSE_LOCAL --config-file "$CONFIG_DIR/config.xml" -q "SELECT name, value, changed FROM system.settings WHERE name = 'default_format' FORMAT TSV"

# An explicit command-line setting still overrides the profile.
$CLICKHOUSE_LOCAL --config-file "$CONFIG_DIR/config.xml" --default_format CSV -q "SELECT 1 AS x"

# Without a profile value, the listener seed is reset: the setting is not marked as changed.
$CLICKHOUSE_LOCAL -q "SELECT count() FROM system.settings WHERE name = 'default_format' AND changed"

# A profile value that happens to be the same format as the listener seed (`TSV`) is still a
# deliberate user default: it is told apart from the seed by provenance, not by its value, so it
# survives on the client session as a changed setting.
cat > "$CONFIG_DIR/users.xml" <<EOF
<clickhouse>
    <profiles>
        <default>
            <default_format>TSV</default_format>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file "$CONFIG_DIR/config.xml" -q "SELECT name, value, changed FROM system.settings WHERE name = 'default_format' FORMAT TSV"

rm -r "$CONFIG_DIR"
