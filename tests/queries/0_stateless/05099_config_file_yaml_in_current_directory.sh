#!/usr/bin/env bash
# A configuration file can be written in XML or in YAML, so `config.yaml` and `config.yml` in the
# current directory must be found just like `config.xml`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_config_yaml"
trap 'rm -rf "$TESTDIR"' EXIT

rm -rf "$TESTDIR"
mkdir -p "$TESTDIR/cwd" "$TESTDIR/home"

write_config()
{
    cat > "$1" <<EOF
max_thread_pool_size: $2
EOF
}

echo "-- ./config.yaml"
write_config "$TESTDIR/cwd/config.yaml" 1001
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
)

echo "-- ./config.yml"
mv "$TESTDIR/cwd/config.yaml" "$TESTDIR/cwd/config.yml"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
)

echo "-- ./config.xml wins over ./config.yaml"
write_config "$TESTDIR/cwd/config.yaml" 1002
rm -f "$TESTDIR/cwd/config.yml"
cat > "$TESTDIR/cwd/config.xml" <<EOF
<clickhouse>
    <max_thread_pool_size>1003</max_thread_pool_size>
</clickhouse>
EOF
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
)

echo "-- --config-file wins over the current directory"
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --config-file "$TESTDIR/cwd/config.yaml" \
        --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
)
rm -f "$TESTDIR/cwd/config.xml" "$TESTDIR/cwd/config.yaml"

echo "-- HOME/.clickhouse-local/config.yaml"
mkdir -p "$TESTDIR/home/.clickhouse-local"
write_config "$TESTDIR/home/.clickhouse-local/config.yaml" 1004
(
    cd "$TESTDIR/cwd" || exit 1
    HOME="$TESTDIR/home" "$CLICKHOUSE_LOCAL" --query "SELECT value FROM system.server_settings WHERE name = 'max_thread_pool_size'"
)
