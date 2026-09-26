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

# A relative users.xml path next to the loaded config is anchored to the
# config's directory; a missing file fails fast instead of silently picking
# up a `./users.xml` from cwd. Verified for both forms below.
cat > "$TESTDIR/cwd/users.xml" <<EOF
<clickhouse>
    <profiles>
        <default>
            <max_threads>99</max_threads>
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

echo "-- missing user_directories.users_xml.path does not silently load cwd users.xml"
mkdir -p "$TESTDIR/orphan_home/.clickhouse-local"
cat > "$TESTDIR/orphan_home/.clickhouse-local/config.xml" <<EOF
<clickhouse>
    <user_directories>
        <users_xml>
            <path>users.xml</path>
        </users_xml>
    </user_directories>
</clickhouse>
EOF
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/orphan_home" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')" 2>&1 \
        | grep -oE 'FILE_DOESNT_EXIST|max_threads' \
        | head -n 1
)

echo "-- missing users_config users.xml does not silently load cwd users.xml"
mkdir -p "$TESTDIR/orphan_home_uc/.clickhouse-local"
cat > "$TESTDIR/orphan_home_uc/.clickhouse-local/config.xml" <<EOF
<clickhouse>
    <users_config>users.xml</users_config>
</clickhouse>
EOF
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/orphan_home_uc" "$CLICKHOUSE_LOCAL" --query "SELECT getSetting('max_threads')" 2>&1 \
        | grep -oE 'FILE_DOESNT_EXIST|max_threads' \
        | head -n 1
)
rm -f "$TESTDIR/cwd/users.xml"
