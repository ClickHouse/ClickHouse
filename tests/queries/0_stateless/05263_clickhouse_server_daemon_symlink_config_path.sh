#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts a separate server process.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The configuration path must never have its `..` collapsed lexically, neither while a relative command line
# path is resolved against the start directory in daemon mode, nor while the file is read: with a symlinked
# component, `conf/../config.xml` names a file in the symlink target and not one next to the symlink.
# Collapsing it loads a different configuration, with a different data directory, without reporting anything.
# https://github.com/ClickHouse/ClickHouse/issues/43170

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05263"
rm -rf "$work"
mkdir -p "$work/releases/r1/conf" "$work/run"

write_config()
{
    # The ports are picked by the kernel, so parallel runs do not collide. The test never connects to them.
    cat > "$1" <<EOF
<clickhouse>
    <logger><level>information</level><console>1</console></logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>0</tcp_port>
    <path>$2/</path>
    <tmp_path>$2/tmp/</tmp_path>
    <user_files_path>$2/user_files/</user_files_path>
    <users><default><no_password/><profile>default</profile><quota>default</quota>
        <networks><ip>::/0</ip></networks></default></users>
    <profiles><default></default></profiles>
    <quotas><default></default></quotas>
</clickhouse>
EOF
}

# The configuration `conf/../config.xml` really names, and the decoy at the lexically collapsed path.
write_config "$work/releases/r1/config.xml" "$work/release_data"
write_config "$work/run/config.xml" "$work/decoy_data"
ln -s "$work/releases/r1/conf" "$work/run/conf"

# The pid file is read here rather than from a variable: the daemon writes it early in
# `BaseDaemon::initialize`, so it can already exist when the test gives up before the server is ready.
# `StatusFile` unlinks it on a clean shutdown, so this is also correct after the server has stopped.
cleanup()
{
    local pid
    pid=$(cat "$work/run/server.pid" 2>/dev/null)
    [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

cd "$work/run" || exit 1
$CLICKHOUSE_BINARY server --daemon \
    -C conf/../config.xml \
    -P server.pid \
    -L server.log \
    -E server.err.log \
    >/dev/null 2>&1

for _ in {1..600}; do
    grep -q 'Ready for connections' server.log 2>/dev/null && break
    sleep 0.1
done

if ! grep -q 'Ready for connections' server.log 2>/dev/null; then
    echo "server did not start"
    exit 1
fi

[ -d "$work/release_data" ] && echo "the configuration behind the symlink was loaded"
[ -d "$work/decoy_data" ] && echo "the configuration at the collapsed path was loaded"

server_pid=$(cat server.pid 2>/dev/null)
if [ -z "$server_pid" ]; then
    echo "pid file does not point to the running server"
    exit 1
fi

kill -TERM "$server_pid"
for _ in {1..600}; do
    kill -0 "$server_pid" 2>/dev/null || break
    sleep 0.1
done

if kill -0 "$server_pid" 2>/dev/null; then
    echo "server did not stop"
else
    echo "server stopped"
fi
