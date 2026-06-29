#!/usr/bin/env python3
"""Manually set up a memory cgroup and trigger the kernel OOM killer with a ClickHouse merge workload.

This reproduces the AST-fuzzer OOM
(https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=107389&sha=ec12cb3ce0a49a403226cb0668b092f02a2fa3f6&name_0=PR&name_1=AST%20fuzzer%20%28amd_debug%2C%20targeted%2C%20old_compatibility%29):
a server stays under its own (tracked) memory limit, but its RESIDENT memory drifts above the limit
because the allocator retains freed pages faster than they are returned to the OS, and the kernel OOM
killer fires. The docker `mem_limit` used by the integration runner is not enforced as a hard
`memory.max` there (docker-in-docker, --cgroupns=host), so the kill never fires in that harness - this
script instead creates the cgroup itself, which the kernel DOES enforce.

Mechanism:
  1. Create a cgroup v2 with a hard `memory.max` and `memory.swap.max = 0` (no swap -> a real OOM).
  2. Start clickhouse-server INSIDE the cgroup (so all its allocations are charged there). It reads the
     cgroup and sets max_server_memory_usage to 0.9 of it - the limit is intact and honoured.
  3. Run sustained, concurrent `groupArrayState` merge/insert churn. The allocator retention gap
     (resident > tracked) pushes resident memory past the cgroup, and the kernel OOM-kills the server.

Requirements: root (to create the cgroup), cgroup v2, a built `clickhouse` binary. Verified to fire the
kernel OOM killer reliably (cgroup `memory.events` `oom_kill` increments; `dmesg` shows
`Memory cgroup out of memory: Killed process ... (clickhouse), oom_memcg=/ch_merge_oom`).

On a server built with the OOM canary enabled (oom_canary_enable=1), the canary is killed first and the
server survives, running its OOM response (cancel all merges); without it the server itself is killed,
as the fuzzer's was.

The script fails closed: it aborts before starting the memory-destructive workload unless every cgroup
setup step is proven (the memory controller is enabled, the hard limits read back as written, and the
server is actually charged to the cgroup). Otherwise the 12-worker churn could run outside ch_merge_oom
and OOM the whole host.
"""

import os
import pwd
import shutil
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.environ.get("CLICKHOUSE_BINARY", os.path.join(REPO, "build", "programs", "clickhouse"))
RUN_USER = os.environ.get("SUDO_USER") or pwd.getpwuid(os.getuid()).pw_name
CG = "/sys/fs/cgroup/ch_merge_oom"
PORT = int(os.environ.get("PORT", "19001"))
LIMIT_GB = int(os.environ.get("LIMIT_GB", "4"))
LIMIT_BYTES = LIMIT_GB * 1024 * 1024 * 1024

NUM_WORKERS = 12
CHURN_SECONDS = 40


def die(message, code=1):
    print(message, file=sys.stderr)
    sys.exit(code)


def read_file(path):
    with open(path) as f:
        return f.read().strip()


def write_file(path, value):
    with open(path, "w") as f:
        f.write(str(value))


def oom_kill_count():
    # `memory.events` has lines like "oom_kill 0"; return that counter (0 if absent).
    try:
        for line in read_file(f"{CG}/memory.events").splitlines():
            if line.startswith("oom_kill "):
                return int(line.split()[1])
    except OSError:
        pass
    return 0


def client(*args, user=None):
    return subprocess.run(
        ["sudo", "-u", user or RUN_USER, BIN, "client", "--port", str(PORT), *args],
        capture_output=True,
        text=True,
    )


def cleanup(base):
    subprocess.run(["pkill", "-9", "-f", f"{base}/cfg"], stderr=subprocess.DEVNULL)
    time.sleep(1)
    if os.path.isdir(CG):
        try:
            write_file(f"{CG}/cgroup.kill", 1)
        except OSError:
            pass
        time.sleep(1)
        try:
            os.rmdir(CG)
        except OSError:
            pass
    shutil.rmtree(base, ignore_errors=True)


CONFIG_XML = """<clickhouse>
  <logger><level>warning</level><log>{base}/ch.log</log></logger>
  <tcp_port>{port}</tcp_port><path>{base}/data/</path>
  <user_directories><users_xml><path>users.xml</path></users_xml></user_directories>
  <mark_cache_size>67108864</mark_cache_size><uncompressed_cache_size>0</uncompressed_cache_size>
  <index_mark_cache_size>0</index_mark_cache_size><index_uncompressed_cache_size>0</index_uncompressed_cache_size>
  <mmap_cache_size>0</mmap_cache_size>
</clickhouse>
"""

USERS_XML = (
    "<clickhouse><profiles><default/></profiles><users><default><password></password>"
    "<networks><ip>::/0</ip></networks><profile>default</profile><quota>default</quota></default>"
    "</users><quotas><default/></quotas></clickhouse>"
)


def main():
    if os.geteuid() != 0:
        die("must run as root (creates a cgroup); re-run with sudo", 2)
    if not os.access(BIN, os.X_OK):
        die(f"clickhouse binary not found at {BIN} (set CLICKHOUSE_BINARY)", 2)
    if subprocess.run(
        ["stat", "-fc", "%T", "/sys/fs/cgroup"], capture_output=True, text=True
    ).stdout.strip() != "cgroup2fs":
        die("requires cgroup v2", 2)

    base = tempfile.mkdtemp(prefix="ch_oom_repro.")
    try:
        cleanup(base)  # remove a stale cgroup from a previous run before re-creating it
        os.makedirs(os.path.join(base, "data"))
        os.makedirs(os.path.join(base, "cfg"))
        write_file(os.path.join(base, "cfg", "config.xml"), CONFIG_XML.format(base=base, port=PORT))
        write_file(os.path.join(base, "cfg", "users.xml"), USERS_XML)
        subprocess.run(["chown", "-R", RUN_USER, base], check=True)

        # 1) cgroup with a hard limit and no swap. Every step is verified; a failure here aborts the
        #    script before any workload runs, so the churn can never escape the cgroup onto the host.
        os.makedirs(CG, exist_ok=True)
        # Enabling +memory is a no-op if it is already enabled, so the write itself is tolerated, but the
        # memory controller must then actually be present in the child cgroup (memory.max appears only
        # when it is).
        try:
            write_file("/sys/fs/cgroup/cgroup.subtree_control", "+memory")
        except OSError:
            pass
        if not os.path.exists(f"{CG}/memory.max"):
            die(f"memory controller not available in {CG} (enabling +memory failed)")

        write_file(f"{CG}/memory.max", LIMIT_BYTES)
        write_file(f"{CG}/memory.swap.max", 0)
        # Read the limits back: a stale or unwritten limit would let the workload outgrow the host.
        if read_file(f"{CG}/memory.max") != str(LIMIT_BYTES):
            die(f"memory.max not set to {LIMIT_BYTES} (got {read_file(f'{CG}/memory.max')})")
        if read_file(f"{CG}/memory.swap.max") != "0":
            die(f"memory.swap.max not set to 0 (got {read_file(f'{CG}/memory.swap.max')})")

        # 2) start the server INSIDE the cgroup, as the unprivileged user. The pre-exec hook runs in the
        #    forked child (still root) and moves it into the cgroup before exec, so the server and every
        #    allocation it makes are charged there; membership is inherited by its children.
        def enter_cgroup():
            write_file(f"{CG}/cgroup.procs", os.getpid())

        config_file = os.path.join(base, "cfg", "config.xml")
        server = subprocess.Popen(
            ["runuser", "-u", RUN_USER, "--", BIN, "server", "--config-file", config_file],
            preexec_fn=enter_cgroup,
        )
        for _ in range(40):
            if client("-q", "SELECT 1").returncode == 0:
                break
            time.sleep(1)
        if client("-q", "SELECT 1").returncode != 0:
            log_tail = subprocess.run(
                ["tail", "-5", f"{base}/ch.log"], capture_output=True, text=True
            ).stdout
            die(f"server failed to start\n{log_tail}")
        # The server must be charged to the cgroup; if the cgroup.procs move silently failed, refuse to
        # start the OOM workload - it would otherwise be charged to the host.
        procs = read_file(f"{CG}/cgroup.procs").splitlines()
        if str(server.pid) not in procs:
            die(
                f"server pid {server.pid} is not in {CG}/cgroup.procs; "
                "refusing to run the OOM workload"
            )
        if int(read_file(f"{CG}/memory.current")) <= 0:
            die(
                "cgroup is not charging memory (memory.current = 0); "
                "refusing to run the OOM workload"
            )
        ram = subprocess.run(
            ["grep", "-aoE", "Available RAM: [^;]+", f"{base}/ch.log"],
            capture_output=True,
            text=True,
        ).stdout.splitlines()
        print(f"server up in {LIMIT_GB} GiB cgroup ({ram[0] if ram else ''})")

        client(
            "-q",
            "CREATE TABLE m (id UInt8, s AggregateFunction(groupArray, String)) "
            "ENGINE = AggregatingMergeTree ORDER BY id "
            "SETTINGS min_bytes_for_wide_part = 0, "
            "vertical_merge_algorithm_min_rows_to_activate = 1000000000, "
            "vertical_merge_algorithm_min_columns_to_activate = 1000000000",
        )

        oom_before = oom_kill_count()

        # 3) sustained concurrent fat-state churn (each state is ~0.2 GiB; merging many of them, and the
        #    allocator retention they leave behind, drive resident memory past the cgroup).
        stop = threading.Event()

        def churn():
            while not stop.is_set():
                # Queries are expected to fail once the cgroup OOM fires; keep churning regardless.
                client(
                    "-q",
                    "INSERT INTO m SELECT 0, arrayReduce('groupArrayState', "
                    "arrayMap(x -> repeat('x', 400000), range(500))) FROM numbers(1)",
                )
                client("-q", "OPTIMIZE TABLE m FINAL")

        workers = [threading.Thread(target=churn) for _ in range(NUM_WORKERS)]
        for w in workers:
            w.start()
        print(f"churning {NUM_WORKERS} workers for ~{CHURN_SECONDS}s in the {LIMIT_GB} GiB cgroup ...")
        time.sleep(CHURN_SECONDS)
        stop.set()
        for w in workers:
            w.join(timeout=10)

        oom_after = oom_kill_count()
        print(f"cgroup oom_kill: {oom_before} -> {oom_after}")
        dmesg = subprocess.run(["dmesg"], capture_output=True, text=True).stdout
        for line in reversed(dmesg.splitlines()):
            if "oom_memcg=/ch_merge_oom" in line:
                print(line)
                break
        if oom_after > oom_before:
            print("RESULT: kernel OOM killer fired")
            return 0
        print("RESULT: OOM did not fire (try a larger workload or smaller LIMIT_GB)")
        return 1
    finally:
        cleanup(base)


if __name__ == "__main__":
    sys.exit(main())
