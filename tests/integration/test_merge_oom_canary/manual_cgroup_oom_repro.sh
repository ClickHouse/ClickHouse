#!/bin/bash
# Manually set up a memory cgroup and trigger the kernel OOM killer with a ClickHouse merge workload.
#
# This reproduces the AST-fuzzer OOM
# (https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=107389&sha=ec12cb3ce0a49a403226cb0668b092f02a2fa3f6&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29):
# a server stays under its own (tracked) memory limit, but its RESIDENT memory drifts above the limit
# because the allocator retains freed pages faster than they are returned to the OS, and the kernel OOM
# killer fires. The docker `mem_limit` used by the integration runner is not enforced as a hard
# `memory.max` there (docker-in-docker, --cgroupns=host), so the kill never fires in that harness - this
# script instead creates the cgroup itself, which the kernel DOES enforce.
#
# Mechanism:
#   1. Create a cgroup v2 with a hard `memory.max` and `memory.swap.max = 0` (no swap -> a real OOM).
#   2. Start clickhouse-server INSIDE the cgroup (so all its allocations are charged there). It reads the
#      cgroup and sets max_server_memory_usage to 0.9 of it - the limit is intact and honoured.
#   3. Run sustained, concurrent `groupArrayState` merge/insert churn. The allocator retention gap
#      (resident > tracked) pushes resident memory past the cgroup, and the kernel OOM-kills the server.
#
# Requirements: root (to create the cgroup), cgroup v2, a built `clickhouse` binary. Verified to fire the
# kernel OOM killer reliably (cgroup `memory.events` `oom_kill` increments; `dmesg` shows
# `Memory cgroup out of memory: Killed process ... (clickhouse), oom_memcg=/ch_merge_oom`).
#
# On a server built with the OOM canary enabled (oom_canary_enable=1), the canary is killed first and the
# server survives, running its OOM response (cancel all merges); without it the server itself is killed,
# as the fuzzer's was.
set -u

REPO=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
BIN=${CLICKHOUSE_BINARY:-$REPO/build/programs/clickhouse}
RUN_USER=${SUDO_USER:-$(id -un)}
BASE=$(mktemp -d /tmp/ch_oom_repro.XXXXXX)
CG=/sys/fs/cgroup/ch_merge_oom
PORT=${PORT:-19001}
LIMIT_GB=${LIMIT_GB:-4}

[ "$(id -u)" -eq 0 ] || { echo "must run as root (creates a cgroup); re-run with sudo"; exit 2; }
[ -x "$BIN" ] || { echo "clickhouse binary not found at $BIN (set CLICKHOUSE_BINARY)"; exit 2; }
[ "$(stat -fc %T /sys/fs/cgroup)" = "cgroup2fs" ] || { echo "requires cgroup v2"; exit 2; }

CL(){ sudo -u "$RUN_USER" "$BIN" client --port "$PORT" "$@"; }
cleanup(){
    pkill -9 -f "$BASE/cfg" 2>/dev/null
    sleep 1
    if [ -d "$CG" ]; then echo 1 > "$CG/cgroup.kill" 2>/dev/null; sleep 1; rmdir "$CG" 2>/dev/null; fi
    rm -rf "$BASE"
}
trap cleanup EXIT
cleanup; mkdir -p "$BASE/data" "$BASE/cfg"

cat > "$BASE/cfg/config.xml" <<EOF
<clickhouse>
  <logger><level>warning</level><log>$BASE/ch.log</log></logger>
  <tcp_port>$PORT</tcp_port><path>$BASE/data/</path>
  <user_directories><users_xml><path>users.xml</path></users_xml></user_directories>
  <mark_cache_size>67108864</mark_cache_size><uncompressed_cache_size>0</uncompressed_cache_size>
  <index_mark_cache_size>0</index_mark_cache_size><index_uncompressed_cache_size>0</index_uncompressed_cache_size>
  <mmap_cache_size>0</mmap_cache_size>
</clickhouse>
EOF
cat > "$BASE/cfg/users.xml" <<EOF
<clickhouse><profiles><default/></profiles><users><default><password></password><networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota></default></users><quotas><default/></quotas></clickhouse>
EOF
chown -R "$RUN_USER" "$BASE"

# 1) cgroup with a hard limit and no swap
mkdir -p "$CG"
echo "+memory" > /sys/fs/cgroup/cgroup.subtree_control 2>/dev/null
echo $((LIMIT_GB*1024*1024*1024)) > "$CG/memory.max"
echo 0 > "$CG/memory.swap.max"

# 2) start the server INSIDE the cgroup, as the unprivileged user
( echo $BASHPID > "$CG/cgroup.procs"; exec runuser -u "$RUN_USER" -- "$BIN" server --config-file "$BASE/cfg/config.xml" ) &
for _ in $(seq 1 40); do CL -q "SELECT 1" >/dev/null 2>&1 && break; sleep 1; done
CL -q "SELECT 1" >/dev/null 2>&1 || { echo "server failed to start"; tail -5 "$BASE/ch.log"; exit 1; }
echo "server up in ${LIMIT_GB} GiB cgroup ($(grep -aoE 'Available RAM: [^;]+' "$BASE/ch.log" | head -1))"

CL -q "CREATE TABLE m (id UInt8, s AggregateFunction(groupArray, String)) ENGINE = AggregatingMergeTree ORDER BY id
       SETTINGS min_bytes_for_wide_part = 0, vertical_merge_algorithm_min_rows_to_activate = 1000000000,
                vertical_merge_algorithm_min_columns_to_activate = 1000000000"

oom_before=$(awk '/^oom_kill /{print $2}' "$CG/memory.events")

# 3) sustained concurrent fat-state churn (each state is ~0.2 GiB; merging many of them, and the allocator
#    retention they leave behind, drive resident memory past the cgroup)
for _ in $(seq 1 12); do
( deadline=$((SECONDS + 40)); while [ $SECONDS -lt $deadline ]; do
    CL -q "INSERT INTO m SELECT 0, arrayReduce('groupArrayState', arrayMap(x -> repeat('x', 400000), range(500))) FROM numbers(1)" >/dev/null 2>&1
    CL -q "OPTIMIZE TABLE m FINAL" >/dev/null 2>&1
  done ) &
done
echo "churning 12 workers for ~40s in the ${LIMIT_GB} GiB cgroup ..."
sleep 44

oom_after=$(awk '/^oom_kill /{print $2}' "$CG/memory.events" 2>/dev/null)
echo "cgroup oom_kill: ${oom_before} -> ${oom_after:-NA}"
dmesg 2>/dev/null | grep -iE "oom_memcg=/ch_merge_oom" | tail -1
if [ "${oom_after:-0}" -gt "${oom_before:-0}" ]; then
    echo "RESULT: kernel OOM killer fired"
    exit 0
fi
echo "RESULT: OOM did not fire (try a larger workload or smaller LIMIT_GB)"
exit 1
