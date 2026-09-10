#!/usr/bin/env bash
# Start two minimal isolated ClickHouse servers for local performance comparison.
# This is a convenience helper, not a CI-equivalent environment.

set -Eeuo pipefail

if [[ $# -lt 2 ]]; then
  cat >&2 <<'USAGE'
Usage:
  scripts/local_servers.sh OLD_CLICKHOUSE NEW_CLICKHOUSE [WORKDIR]

Starts by default:
  old/base server:      TCP 9000, HTTP 8123
  new/candidate server: TCP 9001, HTTP 8124

Override ports with OLD_TCP_PORT, NEW_TCP_PORT, OLD_HTTP_PORT, NEW_HTTP_PORT.

Then run:
  mkdir -p tmp
  tests/performance/scripts/perf.py --host 127.0.0.1 127.0.0.1 --port "$OLD_TCP_PORT" "$NEW_TCP_PORT" --runs 7 tests/performance/<test>.xml | tee tmp/<test>.perf.tsv

Stop:
  kill "$(cat WORKDIR/old/clickhouse.pid)" "$(cat WORKDIR/new/clickhouse.pid)"
USAGE
  exit 2
fi

OLD_BIN="$1"
NEW_BIN="$2"
ROOT="${3:-$PWD/tmp/ch-perf-local}"
OLD_TCP_PORT="${OLD_TCP_PORT:-9000}"
NEW_TCP_PORT="${NEW_TCP_PORT:-9001}"
OLD_HTTP_PORT="${OLD_HTTP_PORT:-8123}"
NEW_HTTP_PORT="${NEW_HTTP_PORT:-8124}"

if [[ ! -x "$OLD_BIN" ]]; then
  echo "OLD_CLICKHOUSE is not executable: $OLD_BIN" >&2
  exit 1
fi
if [[ ! -x "$NEW_BIN" ]]; then
  echo "NEW_CLICKHOUSE is not executable: $NEW_BIN" >&2
  exit 1
fi

write_config() {
  local dir="$1"
  local tcp_port="$2"
  local http_port="$3"
  mkdir -p "$dir" "$dir/data" "$dir/tmp" "$dir/user_files" "$dir/format_schemas" "$dir/log"
  cat > "$dir/config.xml" <<XML
<clickhouse>
    <logger>
        <level>warning</level>
        <log>${dir}/log/server.log</log>
        <errorlog>${dir}/log/server.err.log</errorlog>
        <console>false</console>
    </logger>
    <listen_host>127.0.0.1</listen_host>
    <tcp_port>${tcp_port}</tcp_port>
    <http_port>${http_port}</http_port>
    <interserver_http_port>0</interserver_http_port>
    <path>${dir}/data/</path>
    <tmp_path>${dir}/tmp/</tmp_path>
    <user_files_path>${dir}/user_files/</user_files_path>
    <format_schema_path>${dir}/format_schemas/</format_schema_path>
    <users_config>${dir}/users.xml</users_config>
    <mark_cache_size>536870912</mark_cache_size>
    <uncompressed_cache_size>0</uncompressed_cache_size>
    <timezone>UTC</timezone>
</clickhouse>
XML
  cat > "$dir/users.xml" <<XML
<clickhouse>
    <profiles>
        <default>
            <max_memory_usage>0</max_memory_usage>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>
    <users>
        <default>
            <password></password>
            <networks><ip>::/0</ip></networks>
            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>
    <quotas><default><interval><duration>3600</duration><queries>0</queries><errors>0</errors><result_rows>0</result_rows><read_rows>0</read_rows><execution_time>0</execution_time></interval></default></quotas>
</clickhouse>
XML
}

STARTED_DIRS=()

cleanup_started() {
  local status="${1:-1}"
  trap - ERR INT TERM
  if [[ ${#STARTED_DIRS[@]} -gt 0 ]]; then
    echo "Cleaning up partially started ClickHouse servers..." >&2
  fi
  for dir in "${STARTED_DIRS[@]}"; do
    if [[ -s "$dir/clickhouse.pid" ]]; then
      local pid
      pid="$(cat "$dir/clickhouse.pid" 2>/dev/null || true)"
      if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
    fi
  done
  exit "$status"
}

trap 'cleanup_started $?' ERR
trap 'cleanup_started 130' INT
trap 'cleanup_started 143' TERM

start_server() {
  local bin="$1"
  local dir="$2"
  local port="$3"
  echo "Starting $bin on TCP $port in $dir"
  "$bin" server --config-file="$dir/config.xml" --daemon --pid-file="$dir/clickhouse.pid"
  STARTED_DIRS+=("$dir")
  for _ in {1..60}; do
    if "$bin" client --port "$port" --query "SELECT 1" >/dev/null 2>&1; then
      "$bin" client --port "$port" --query "SELECT version(), buildId()"
      return 0
    fi
    sleep 1
  done
  echo "Server on port $port did not become ready. Logs: $dir/log/server.log $dir/log/server.err.log" >&2
  return 1
}

mkdir -p "$ROOT"
write_config "$ROOT/old" "$OLD_TCP_PORT" "$OLD_HTTP_PORT"
write_config "$ROOT/new" "$NEW_TCP_PORT" "$NEW_HTTP_PORT"
start_server "$OLD_BIN" "$ROOT/old" "$OLD_TCP_PORT"
start_server "$NEW_BIN" "$ROOT/new" "$NEW_TCP_PORT"
trap - ERR INT TERM

cat <<EOF

Servers are running.

Old/base:
  TCP: $OLD_TCP_PORT
  PID: $ROOT/old/clickhouse.pid
  Log: $ROOT/old/log/server.log

New/candidate:
  TCP: $NEW_TCP_PORT
  PID: $ROOT/new/clickhouse.pid
  Log: $ROOT/new/log/server.log

Run a performance test:
  tests/performance/scripts/perf.py --host 127.0.0.1 127.0.0.1 --port $OLD_TCP_PORT $NEW_TCP_PORT --runs 7 tests/performance/<test>.xml

Stop:
  kill "\$(cat '$ROOT/old/clickhouse.pid')" "\$(cat '$ROOT/new/clickhouse.pid')"
EOF
