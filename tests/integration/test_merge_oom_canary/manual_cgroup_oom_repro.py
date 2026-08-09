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
`Memory cgroup out of memory: Killed process ... (clickhouse), oom_memcg=/ch_merge_oom.<pid>`).

On a server built with the OOM canary enabled (oom_canary_enable=1), the canary is killed first and the
server survives, running its OOM response (cancel all merges); without it the server itself is killed,
as the fuzzer's was.

The script fails closed: it aborts before starting the memory-destructive workload unless every cgroup
setup step is proven (the memory controller is enabled, the hard limits read back as written, and the
server is actually charged to the cgroup). Otherwise the 12-worker churn could run outside the cgroup
and OOM the whole host. The cgroup name is unique per run and `cleanup` tears it down with
`cgroup.kill` only when this run created it (a pre-existing cgroup - a recycled PID - aborts the run
and is left untouched), so the teardown can only ever kill this run's own processes; the port is one the run binds and
HOLDS from before any setup until the moment the server is started (an explicit PORT is bound the same
way, so a collision fails fast instead of burning the startup loop). That reservation is best-effort
race reduction, not a guarantee: it has to be released for `clickhouse-server` to bind the port, so in
the brief gap between the release and the server's own `bind` another process can still claim it -
which then surfaces as the bounded startup probe failing, not as silent misbehavior. It also refuses to run when the scratch data directory
is on a memory-backed filesystem (`tmpfs`/`ramfs`): there the part bytes written by the churn would be
charged to the cgroup, so an OOM would prove the filesystem choice rather than the merge-memory
mechanism. The scratch root defaults to the repo `tmp` directory (disk-backed) and can be overridden
with BASE_DIR. The pre-OOM churn fails closed as well: until at least one churn cycle has completed
successfully (and no cgroup OOM has fired yet), a failed or timed-out `INSERT`/`OPTIMIZE` aborts the
run with the client's error instead of letting every worker spin uselessly and misreport the broken
workload as "OOM did not fire". After that first successful cycle, failures are the expected way the workload dies
as the cgroup fills - with one exception: a `Memory limit (total) exceeded` rejection at any point
before the OOM aborts the run, because it means tracked memory reached `max_server_memory_usage`, and
a subsequent kernel kill would then demonstrate ordinary tracked-memory exhaustion rather than the
resident > tracked drift this script exists to prove.
"""

import os
import pwd
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
BIN = os.environ.get("CLICKHOUSE_BINARY", os.path.join(REPO, "build", "programs", "clickhouse"))
# Scratch root for the server's data and config. Defaults to the repo `tmp` directory (disk-backed)
# rather than the system temp dir, which is frequently `tmpfs` on developer machines: a memory-backed
# data path would charge the part bytes written by the `INSERT`/`OPTIMIZE` churn to the cgroup's
# `memory.current` and could trip `memory.max` on scratch I/O alone - a false-positive "merge OOM"
# caused by the filesystem choice rather than by the merge-memory retention gap this script demonstrates.
# Override with BASE_DIR (which must also be disk-backed; the check below enforces it).
BASE_DIR = os.environ.get("BASE_DIR", os.path.join(REPO, "tmp"))
RUN_USER = os.environ.get("SUDO_USER") or pwd.getpwuid(os.getuid()).pw_name
# The cgroup name is unique per run: `cleanup` tears the cgroup down with `cgroup.kill`, and with a
# fixed shared name that write would be host-destructive - it would kill a concurrent run's server, or
# any unrelated workload someone had placed in a same-named cgroup, as if it were "stale". With a
# per-run name, `cgroup.kill` can only ever hit processes this run started itself.
CG_NAME = f"ch_merge_oom.{os.getpid()}"
CG = f"/sys/fs/cgroup/{CG_NAME}"
# Set only once this run has actually created `CG` itself. A PID is recyclable, so `CG` can already
# exist when the script starts (a leaked cgroup from a dead run, or - worse - a live cgroup belonging
# to something else that happens to use the same name). The setup step fails closed in that case, but
# the abort still unwinds through `finally: cleanup(base)`, so the teardown must not touch a cgroup
# this run does not own: `cleanup` writes `cgroup.kill` only when this flag is set.
CG_OWNED = False


def reserve_port(port):
    """Bind `port` (0 = let the kernel choose) and KEEP the socket open; return `(socket, port)`.

    The socket is held for the whole setup, until `release_port_reservation` closes it immediately
    before the server is started. Probing a port and closing the probe right away would not make the
    port unique: two concurrent runs can each pick and release the same ephemeral port and only find
    out when one of them burns its whole startup loop on `Address already in use`. Holding the binding
    is what actually reserves it - while this socket is open no other process can bind the port.

    Binding to port 0 makes the kernel pick a currently-unused ephemeral port, which is what "unique
    per run" requires: any function of the PID (e.g. `19001 + pid % 10000`) aliases as soon as two live
    PIDs are congruent modulo the range. An explicit PORT goes through the same bind, so a collision
    fails fast with a clear message instead of silently claiming uniqueness.

    This is best-effort race reduction, NOT an airtight handoff: the reservation must be closed for
    `clickhouse-server` to bind the port, and a plain reservation socket cannot be handed over to the
    server, so the gap between `release_port_reservation` and the server's own `bind` stays open. A
    process that grabs the port in that gap makes the bounded startup probe below fail ("server did
    not start"), which is a loud, immediate error rather than a corrupted result.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", port))
    except OSError as e:
        probe.close()
        die(
            f"TCP port {port} is not free ({e}); another server (or a concurrent run of this "
            "script) is using it - re-run without PORT to let the kernel pick a free port",
            2,
        )
    return probe, probe.getsockname()[1]


def release_port_reservation():
    """Release the held port, so `clickhouse-server` itself can bind it."""
    global PORT_RESERVATION
    if PORT_RESERVATION is not None:
        PORT_RESERVATION.close()
        PORT_RESERVATION = None


LIMIT_GB = int(os.environ.get("LIMIT_GB", "4"))
LIMIT_BYTES = LIMIT_GB * 1024 * 1024 * 1024

NUM_WORKERS = 12
CHURN_SECONDS = 40

# Bounded client timeouts, shared by the startup/setup probes and the churn workers. Without them a
# `clickhouse client` call falls back to the CLI defaults (connect 10s, receive 300s), so a wedged or
# half-started server could park the "40 tries" startup loop or the pre-workload `CREATE TABLE` for
# minutes before any churn begins, defeating the fail-closed / bounded-startup guarantee. The
# `--connect_timeout`/`--receive_timeout` bound the call server-side; the subprocess `timeout` is a
# hard backstop that kills a stuck client.
CLIENT_TIMEOUT_ARGS = ("--connect_timeout", "5", "--receive_timeout", "10")
CLIENT_TIMEOUT_BACKSTOP = 20


def die(message, code=1):
    print(message, file=sys.stderr)
    sys.exit(code)


# The port is taken (and held) right away, before anything is set up: a port that is not free would
# otherwise only surface as 40 failed startup probes, and the reservation is what keeps a concurrent
# run from picking the same port during setup. It is released just before the server binds it, so a
# brief unreserved gap remains (best effort - see `reserve_port`).
PORT_RESERVATION, PORT = reserve_port(int(os.environ.get("PORT", "0")))


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


# The `clickhouse client` subprocesses this script started, tracked so `cleanup` can tear down exactly
# those - and nothing else - instead of a host-wide `pkill -f` over a shared `--port` that could kill
# unrelated `clickhouse client` sessions. Each client runs in its own session/process group, so a stuck
# one can be killed by PGID together with the `sudo` wrapper that spawned it.
_clients = set()
_clients_lock = threading.Lock()


def kill_client_group(proc):
    # SIGKILL the whole session/process group of a client we started, reaping the `sudo` wrapper and the
    # `clickhouse client` grandchild together (killing `proc` alone would orphan the grandchild). Only
    # this script's own clients are ever passed here.
    #
    # Skip a `Popen` that has already finished and been reaped: `cleanup` can snapshot `_clients` in the
    # window between a worker's `communicate()` reaping the child and its `finally` discarding the entry,
    # and once the child is reaped its PID may be recycled by an unrelated process, so signalling the PGID
    # could hit the new owner (as root, no less). While the child is unreaped its PID is held (running or
    # zombie), so the PGID lookup targets our own client. This check narrows the race rather than closing
    # it airtight - a concurrent `communicate` can still reap in the instant between the check and the
    # signal - the same best-effort stance as the port-reservation handoff above; the residual window is
    # microseconds against a kernel PID-recycling cycle.
    if proc.poll() is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError):
        proc.kill()


def client(*args, user=None, timeout=None):
    # Launch in its own session (start_new_session=True -> the child is a process-group leader, PGID ==
    # PID) and register it, so a call that blocks against a wedged or OOM-killed server can be torn down
    # by PGID on timeout or cleanup, and only clients this script started are ever killed.
    proc = subprocess.Popen(
        ["sudo", "-u", user or RUN_USER, BIN, "client", "--port", str(PORT), *args],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        start_new_session=True,
    )
    with _clients_lock:
        _clients.add(proc)
    try:
        try:
            out, err = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            kill_client_group(proc)
            proc.communicate()  # reap the killed process group
            raise
        return subprocess.CompletedProcess(proc.args, proc.returncode, out, err)
    finally:
        with _clients_lock:
            _clients.discard(proc)


def bounded_client(*args, user=None):
    # `client` with the shared bounded timeouts applied, so neither the setup probes nor the churn
    # workers can block on a wedged or OOM-killed server past the backstop. Raises `TimeoutExpired`
    # if even the backstop is hit; callers decide whether that means "not up yet" or a hard failure.
    return client(*CLIENT_TIMEOUT_ARGS, *args, user=user, timeout=CLIENT_TIMEOUT_BACKSTOP)


def cleanup(base):
    subprocess.run(["pkill", "-9", "-f", f"{base}/cfg"], stderr=subprocess.DEVNULL)
    # Also kill any outstanding `clickhouse client` subprocesses this script started: a churn worker
    # may still have one blocked in an `INSERT`/`OPTIMIZE` against a wedged or OOM-killed server, and
    # those clients do not match the `{base}/cfg` pattern above (they are launched with `--port`, not a
    # config file), so the server-side kill alone would leave them running. Kill only the tracked client
    # process groups, never a host-wide `pkill -f` that could hit unrelated sessions on a shared port.
    with _clients_lock:
        outstanding = list(_clients)
    for proc in outstanding:
        kill_client_group(proc)
    time.sleep(1)
    # Only tear down a cgroup this run created. If `CG` already existed at startup the run aborted
    # without adopting it, and killing its processes here would be exactly the host-destructive act the
    # per-run name is meant to rule out.
    if CG_OWNED and os.path.isdir(CG):
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
    os.makedirs(BASE_DIR, exist_ok=True)
    base = tempfile.mkdtemp(prefix="ch_oom_repro.", dir=BASE_DIR)
    try:
        os.makedirs(os.path.join(base, "data"))
        os.makedirs(os.path.join(base, "cfg"))
        # Fail closed if the scratch data path turns out to be memory-backed anyway (BASE_DIR pointed at
        # a `tmpfs`/`ramfs`, or the whole repo lives on one). On such a filesystem the part bytes the
        # churn writes are charged to the cgroup, so an OOM would prove the filesystem choice, not the
        # merge-memory mechanism; refuse to run rather than report a false-positive repro.
        data_dir = os.path.join(base, "data")
        fstype = subprocess.run(
            ["stat", "-fc", "%T", data_dir], capture_output=True, text=True
        ).stdout.strip()
        if fstype in ("tmpfs", "ramfs"):
            die(
                f"scratch data dir {data_dir} is on a memory-backed filesystem ({fstype}); "
                "set BASE_DIR to a disk-backed path so an OOM proves the merge-memory mechanism"
            )
        write_file(os.path.join(base, "cfg", "config.xml"), CONFIG_XML.format(base=base, port=PORT))
        write_file(os.path.join(base, "cfg", "users.xml"), USERS_XML)
        subprocess.run(["chown", "-R", RUN_USER, base], check=True)

        # 1) cgroup with a hard limit and no swap. Every step is verified; a failure here aborts the
        #    script before any workload runs, so the churn can never escape the cgroup onto the host.
        # The per-run name makes a pre-existing path a genuine anomaly (a leaked cgroup from a dead run
        # whose PID was recycled); fail fast rather than `cgroup.kill` something this run does not own.
        if os.path.isdir(CG):
            die(f"cgroup {CG} already exists; remove it first (rmdir {CG})")
        os.makedirs(CG)
        # From here on the cgroup is ours, so `cleanup` may kill and remove it.
        global CG_OWNED
        CG_OWNED = True
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
        # Hand the port over: the reservation socket is closed only here, one statement before the
        # server is spawned, so the port stayed held for the whole setup and the window in which
        # another process could steal it is as small as a plain socket allows - but it still exists
        # (the socket cannot be handed over to the server). A theft in this gap is caught loudly by
        # the bounded startup probe below, not silently.
        release_port_reservation()
        server = subprocess.Popen(
            ["runuser", "-u", RUN_USER, "--", BIN, "server", "--config-file", config_file],
            preexec_fn=enter_cgroup,
        )
        # Bound the startup probe too: a half-started server can accept the connection but stall the
        # query, so an unbounded `SELECT 1` would park the "40 tries" loop for minutes. A backstop
        # `TimeoutExpired` here just means "not up yet" - retry until the loop budget is exhausted.
        started = False
        for _ in range(40):
            try:
                if bounded_client("-q", "SELECT 1").returncode == 0:
                    started = True
                    break
            except subprocess.TimeoutExpired:
                pass
            time.sleep(1)
        if not started:
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

        # Bound the pre-workload setup query as well, so a wedged server fails setup quickly instead
        # of blocking on the default 300s receive timeout.
        try:
            create = bounded_client(
                "-q",
                "CREATE TABLE m (id UInt8, s AggregateFunction(groupArray, String)) "
                "ENGINE = AggregatingMergeTree ORDER BY id "
                "SETTINGS min_bytes_for_wide_part = 0, "
                "vertical_merge_algorithm_min_rows_to_activate = 1000000000, "
                "vertical_merge_algorithm_min_columns_to_activate = 1000000000",
            )
        except subprocess.TimeoutExpired:
            die("timed out creating table m (server wedged during setup)")
        # Fail closed here too: if the table is not created, every churn worker below would just loop
        # on a nonexistent table and the script would misreport "OOM did not fire", hiding the real
        # setup failure as if the workload were merely too small.
        if create.returncode != 0:
            die(f"failed to create table m:\n{create.stdout}{create.stderr}")

        oom_before = oom_kill_count()

        # 3) sustained concurrent fat-state churn (each state is ~0.2 GiB; merging many of them, and the
        #    allocator retention they leave behind, drive resident memory past the cgroup).
        stop = threading.Event()
        # Set once any worker has completed one full successful INSERT+OPTIMIZE cycle. Until then a
        # failed or timed-out query is a failure of the reproducer itself (connection refused, SQL
        # error, a wedged server), not a symptom of the cgroup OOM, and it must abort the run: with every worker silently spinning on
        # a broken workload the script would burn the whole churn window doing nothing and misreport
        # "OOM did not fire". Once a cycle has succeeded - or once `oom_kill_count` has started
        # incrementing - failures are the expected way the workload dies and the workers keep churning
        # through them, except for tracked-memory rejections, which abort at any point before the kill
        # (see below).
        churn_ok = threading.Event()
        # First pre-success failure, recorded for the abort message (`list.append` is atomic, and only
        # the first element is ever read). Holds either a failed `CompletedProcess` or a
        # `subprocess.TimeoutExpired` - a call that timed out before anything ever succeeded is the
        # same "the reproducer is broken" signal as a non-zero exit.
        churn_failures = []
        # First `Memory limit (total) exceeded` rejection observed before the OOM. The server's own
        # tracked-memory limit firing means tracked memory reached `max_server_memory_usage`, so a
        # later kernel kill would prove tracked exhaustion, not the resident > tracked drift - such a
        # run must fail no matter what happens after, and unlike `churn_failures` it is not excused by
        # `churn_ok` (same atomic-append/first-element-only discipline).
        tracked_limit_failures = []

        def churn():
            while not stop.is_set():
                # `bounded_client` caps every call (see CLIENT_TIMEOUT_* above), so after `stop` is
                # set - or once the server is OOM-killed or otherwise wedged - a call cannot block
                # forever and the worker still observes `stop`. A `TimeoutExpired` is expected under
                # OOM (the server stalls before it dies), so once a cycle has succeeded - or once the
                # kill has landed - it is swallowed and the worker re-checks `stop`. Before that,
                # a timeout is just another way the workload never got off the ground: a server
                # wedged from the start would time out every call for the whole churn window and
                # misreport "OOM did not fire", so it follows the same fail-closed rule as the
                # non-timeout failures below (and the same `oom_kill_count` check, re-checked in
                # `main` via `oom_after`, excuses a timeout that raced with the kill).
                try:
                    insert = bounded_client(
                        "-q",
                        "INSERT INTO m SELECT 0, arrayReduce('groupArrayState', "
                        "arrayMap(x -> repeat('x', 400000), range(500))) FROM numbers(1)",
                    )
                    optimize = bounded_client("-q", "OPTIMIZE TABLE m FINAL")
                except subprocess.TimeoutExpired as timeout_error:
                    if not churn_ok.is_set() and oom_kill_count() == oom_before:
                        churn_failures.append(timeout_error)
                        stop.set()
                        return
                    continue
                if insert.returncode == 0 and optimize.returncode == 0:
                    churn_ok.set()
                    continue
                failed = insert if insert.returncode != 0 else optimize
                if "Memory limit (total) exceeded" in failed.stderr and oom_kill_count() == oom_before:
                    # The tracked-memory limit rejected the query before any cgroup OOM: tracked
                    # memory has reached `max_server_memory_usage`, so the run can no longer prove
                    # the resident > tracked mechanism - stop the churn and let `main` abort, even
                    # though a first successful cycle has already been seen.
                    tracked_limit_failures.append(failed)
                    stop.set()
                    return
                if not churn_ok.is_set() and oom_kill_count() == oom_before:
                    # Fail closed: the workload broke before it ever worked and before any OOM - stop
                    # the churn now and let `main` abort with the client's error instead of waiting
                    # out the full churn window and reporting a false negative.
                    churn_failures.append(failed)
                    stop.set()
                    return

        # Daemon threads so a worker that is somehow still blocked cannot keep the interpreter alive at
        # exit (a non-daemon thread would be joined implicitly, hanging the script).
        workers = [threading.Thread(target=churn, daemon=True) for _ in range(NUM_WORKERS)]
        for w in workers:
            w.start()
        print(f"churning {NUM_WORKERS} workers for ~{CHURN_SECONDS}s in the {LIMIT_GB} GiB cgroup ...")
        # A worker that hits a pre-success failure sets `stop` itself (see `churn`), so wait on the
        # event rather than sleeping unconditionally: a broken workload aborts the run immediately
        # instead of idling out the whole churn window first.
        stop.wait(CHURN_SECONDS)
        stop.set()
        # The join window comfortably exceeds the per-call client timeout above, so a worker that was
        # mid-call when `stop` was set still finishes here rather than lingering.
        for w in workers:
            w.join(timeout=30)
        stuck = [w for w in workers if w.is_alive()]
        if stuck:
            print(
                f"WARNING: {len(stuck)} churn worker(s) still running after join "
                "(a client call did not return within its timeout)",
                file=sys.stderr,
            )

        oom_after = oom_kill_count()
        # A tracked-memory rejection is decisive on its own: even if the kernel OOM fired afterwards,
        # tracked memory was at the server limit before the kill, so the kill demonstrates tracked
        # exhaustion, not the resident > tracked drift. No `oom_after` re-check here - unlike
        # `churn_failures`, where a concurrent kill legitimizes the failure, a pre-kill rejection
        # contaminates the run either way.
        if tracked_limit_failures:
            failed = tracked_limit_failures[0]
            die(
                "the churn hit the server's tracked memory limit (`Memory limit (total) exceeded`) "
                "before any cgroup OOM: tracked memory reached `max_server_memory_usage`, so an OOM "
                "in this run would prove ordinary tracked-memory exhaustion rather than the "
                "resident > tracked drift this script demonstrates; lower LIMIT_GB (the retention "
                "gap must outgrow the headroom between the tracked limit and `memory.max`) or use a "
                f"fatter per-state payload:\n{failed.stdout}{failed.stderr}"
            )
        # The churn never worked and no OOM fired: the reproducer itself is broken (server refused
        # connections, the SQL failed, ...) - report that instead of a false "OOM did not fire".
        # `oom_after` is re-checked here because the worker's own check races with the kill: if the
        # OOM did land in between, the failure was the expected death of the workload, not a bug.
        if churn_failures and not churn_ok.is_set() and oom_after == oom_before:
            failed = churn_failures[0]
            if isinstance(failed, subprocess.TimeoutExpired):
                die(
                    "churn workload timed out before completing a single successful cycle and no "
                    "cgroup OOM fired; the server is wedged (or the client cannot reach it) - the "
                    f"reproducer is broken, not the workload too small:\n{failed}"
                )
            die(
                "churn workload failed before completing a single successful cycle "
                f"(exit code {failed.returncode}) and no cgroup OOM fired; the reproducer is "
                f"broken, not the workload too small:\n{failed.stdout}{failed.stderr}"
            )
        print(f"cgroup oom_kill: {oom_before} -> {oom_after}")
        dmesg = subprocess.run(["dmesg"], capture_output=True, text=True).stdout
        for line in reversed(dmesg.splitlines()):
            if f"oom_memcg=/{CG_NAME}" in line:
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
