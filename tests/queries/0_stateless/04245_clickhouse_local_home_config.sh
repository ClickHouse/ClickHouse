#!/usr/bin/env bash
# Regression test for https://github.com/ClickHouse/ClickHouse/issues/104932
# `clickhouse-local` silently ignored user configuration files auto-discovered
# via `getLocalConfigPath` (`~/.clickhouse-local/config.xml`, `./clickhouse-local.xml`,
# `/etc/clickhouse-local/config.xml`). The `setupUsers` gate only recognized configs
# from `--config-file` and `./config.xml`, falling through to the minimal default user
# XML for every other discovery path.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TESTDIR="$CURDIR/04245_test_dir_$$"

cleanup() {
    cd "$CURDIR" || exit 1
    rm -rf "$TESTDIR"
}
trap cleanup EXIT

rm -rf "$TESTDIR"
mkdir -p "$TESTDIR/home/.clickhouse-local"
mkdir -p "$TESTDIR/cwd"

# A config that points to a users.xml living next to it and sets `max_threads=42`
# in the default profile. If user configuration is loaded, `SELECT getSetting('max_threads')`
# must return 42 instead of the built-in default.
cat > "$TESTDIR/home/.clickhouse-local/config.xml" <<EOF
<clickhouse>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
EOF
cat > "$TESTDIR/home/.clickhouse-local/users.xml" <<EOF
<clickhouse>
    <profiles>
        <default>
            <max_threads>42</max_threads>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <quotas>
        <default></default>
    </quotas>
</clickhouse>
EOF

echo "-- HOME/.clickhouse-local/config.xml — must apply user config (regression for #104932)"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')"
)

echo "-- ./clickhouse-local.xml — must apply user config"
(
    cd "$TESTDIR/cwd" || exit 1
    cp "$TESTDIR/home/.clickhouse-local/config.xml" "./clickhouse-local.xml"
    cp "$TESTDIR/home/.clickhouse-local/users.xml" "./users.xml"
    HOME="$TESTDIR" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')"
    rm -f "./clickhouse-local.xml" "./users.xml"
)

echo "-- --config-file pointing at HOME config — regression check, must still apply user config"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR" "$CLICKHOUSE_LOCAL" \
        --config-file="$TESTDIR/home/.clickhouse-local/config.xml" \
        --query "SELECT getSetting('max_threads')"
)

echo "-- no config anywhere — must fall back to default"
EMPTY_HOME=$(mktemp -d)
trap 'cleanup; rm -rf "$EMPTY_HOME"' EXIT
(
    cd "$TESTDIR/cwd" || exit 1
    # Default `max_threads` depends on the host's CPU count, so just assert that
    # the query succeeds with a positive integer (i.e. did not crash on missing config).
    OUT=$(HOME="$EMPTY_HOME" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads') > 0")
    echo "$OUT"
)
