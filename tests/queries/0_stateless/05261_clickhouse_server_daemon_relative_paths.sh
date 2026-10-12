#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts a separate server process.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `clickhouse server --daemon` changes the current directory before it reads the config and opens the
# pid and log files, so relative `--config-file`, `--pid-file`, `--log-file` and `--errorlog-file`
# must be resolved against the directory the server was started from.
# https://github.com/ClickHouse/ClickHouse/issues/43170

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05261"
rm -rf "$work"
mkdir -p "$work/conf" "$work/run" "$work/data"

server_pid=""
cleanup()
{
    [ -n "$server_pid" ] && kill -9 "$server_pid" 2>/dev/null
    rm -rf "$work"
}
trap cleanup EXIT

# The ports are picked by the kernel, so parallel runs do not collide. The test never connects to them.
cat > "$work/conf/config.xml" <<EOF
<clickhouse>
    <logger><level>information</level><console>0</console></logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>0</tcp_port>
    <path>$work/data/</path>
    <tmp_path>$work/data/tmp/</tmp_path>
    <user_files_path>$work/data/user_files/</user_files_path>
    <users><default><no_password/><profile>default</profile><quota>default</quota>
        <networks><ip>::/0</ip></networks></default></users>
    <profiles><default></default></profiles>
    <quotas><default></default></quotas>
</clickhouse>
EOF

# The config is reached through `..` to check that the path is resolved against the start directory
# rather than merely prefixed with it.
cd "$work/run" || exit 1
$CLICKHOUSE_BINARY server --daemon \
    -C ../conf/config.xml \
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

[ -f server.log ] && echo "log file created"
[ -f server.err.log ] && echo "error log file created"

server_pid=$(cat server.pid 2>/dev/null)
if [ -n "$server_pid" ] && kill -0 "$server_pid" 2>/dev/null; then
    echo "pid file points to the running server"
else
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
    server_pid=""
    echo "server stopped"
fi
