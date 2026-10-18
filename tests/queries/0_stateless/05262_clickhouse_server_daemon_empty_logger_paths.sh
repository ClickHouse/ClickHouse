#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: starts a separate server process.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# An empty `logger.log` or `logger.errorlog` disables the corresponding file channel. It is not a path,
# so resolving relative command line paths against the start directory in daemon mode must leave it
# empty rather than turn it into the start directory, which `Loggers::buildLoggers` would then fail to
# open as a file.
# https://github.com/ClickHouse/ClickHouse/issues/43170

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_05262"
rm -rf "$work"
mkdir -p "$work/run" "$work/data"

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

# The ports are picked by the kernel, so parallel runs do not collide. The test never connects to them.
cat > "$work/run/config.xml" <<EOF
<clickhouse>
    <logger><level>information</level><console>1</console></logger>
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

# Both file channels are disabled, so the console channel is the only way to observe the startup.
cd "$work/run" || exit 1
$CLICKHOUSE_BINARY server --daemon \
    -C config.xml \
    -P server.pid \
    -- --logger.log= --logger.errorlog= --logger.stderr="$work/run/console.log" \
    >/dev/null 2>&1

for _ in {1..600}; do
    grep -q 'Ready for connections' console.log 2>/dev/null && break
    sleep 0.1
done

if ! grep -q 'Ready for connections' console.log 2>/dev/null; then
    echo "server did not start"
    exit 1
fi

# `Loggers::buildLoggers` prints `Logging <level> to <path>` for every file channel it opens. Neither
# channel may appear: that line would mean an empty value had been turned into a path again.
grep -q '^Logging ' console.log || echo "no file log channel was configured"

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
    echo "server stopped"
fi
