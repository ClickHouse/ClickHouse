#!/usr/bin/env bash
# Regression for https://github.com/ClickHouse/ClickHouse/issues/104932

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TESTDIR="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_104932"
trap 'rm -rf "$TESTDIR"' EXIT

rm -rf "$TESTDIR"
mkdir -p "$TESTDIR/home/.clickhouse-local"
mkdir -p "$TESTDIR/cwd"

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

echo "-- HOME/.clickhouse-local/config.xml"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')"
)

echo "-- ./clickhouse-local.xml"
(
    cd "$TESTDIR/cwd" || exit 1
    cp "$TESTDIR/home/.clickhouse-local/config.xml" "./clickhouse-local.xml"
    cp "$TESTDIR/home/.clickhouse-local/users.xml" "./users.xml"
    HOME="$TESTDIR" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')"
    rm -f "./clickhouse-local.xml" "./users.xml"
)

echo "-- --config-file"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR" "$CLICKHOUSE_LOCAL" \
        --config-file="$TESTDIR/home/.clickhouse-local/config.xml" \
        --query "SELECT getSetting('max_threads')"
)

echo "-- no config"
mkdir -p "$TESTDIR/empty_home"
(
    cd "$TESTDIR/cwd" || exit 1
    # Default `max_threads` depends on the host's CPU count, so just assert that
    # the query succeeds with a positive integer (i.e. did not crash on missing config).
    OUT=$(HOME="$TESTDIR/empty_home" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads') > 0")
    echo "$OUT"
)
