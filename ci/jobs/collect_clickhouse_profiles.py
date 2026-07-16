"""
Collect PGO and BOLT profiles for the ClickHouse binary.

This job:
  1. Builds ClickHouse with PGO instrumentation (-fprofile-generate)
  2. Runs performance tests to collect representative execution profiles
  3. Merges PGO profiles into a single .profdata file
  4. Rebuilds ClickHouse with --emit-relocs for BOLT
  5. Instruments the binary with llvm-bolt
  6. Runs performance tests again to collect BOLT profiles
  7. Merges BOLT profiles and uploads both artifacts

The collected profiles can be used in release builds with:
  -DCLICKHOUSE_PGO_PROFILE_PATH=<path> and llvm-bolt post-processing.
"""

import glob
import os
import platform
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET

from ci.defs.defs import ToolSet
from ci.jobs.scripts.dataset_download import download_and_extract_datasets
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp"
# In Docker the repo is mounted at /ClickHouse; for local --no-docker runs use cwd.
# Check that /ClickHouse actually resolves to this working directory (not a stale symlink).
repo_path = "/ClickHouse" if os.path.realpath("/ClickHouse") == os.path.realpath(current_directory) else current_directory

# Build directories
PGO_BUILD_DIR = f"{temp_dir}/build_pgo_generate"
BOLT_BUILD_DIR = f"{temp_dir}/build_bolt"

# Profile output paths
PGO_RAW_PROFILES_DIR = f"{temp_dir}/pgo_profiles_raw"
PGO_PROFDATA_PATH = f"{temp_dir}/clickhouse-pgo.profdata"
BOLT_PROFILES_DIR = f"{temp_dir}/bolt_profiles"
BOLT_FDATA_PATH = f"{temp_dir}/clickhouse-bolt.fdata"

# Performance test workdir
PERF_WD = f"{temp_dir}/perf_wd"
PERF_DB_PATH = f"{PERF_WD}/db0"
PERF_SERVER_DIR = f"{PERF_WD}/server"

# Profile collection runs the performance-test suite against the slow instrumented
# / un-BOLTed binary, so it is strictly time-bounded: the whole job must finish
# within its CI timeout, after which nothing is uploaded. The two collection passes
# get fixed budgets, clamped at run time (see `collection_budget`) to the time
# actually left in the job, so a slow build cannot push collection past the hard
# kill. Once a pass exceeds its budget the remaining perf tests are skipped and
# logged (never silently dropped).
JOB_START = time.monotonic()
JOB_TIMEOUT_S = 8 * 3600

PGO_COLLECT_BUDGET_S = 2 * 3600  # PGO-instrumented pass (slowest binary)
BOLT_COLLECT_BUDGET_S = 45 * 60  # BOLT-instrumented pass
# Wall-clock that must remain after the PGO pass for the BOLT build (ThinLTO is slow
# and this is the largest single phase), both profile merges, and the artifact
# upload. Kept generous so a slow BOLT build cannot push the job past its timeout.
BOLT_BUILD_RESERVE_S = 3 * 3600 + 1800
UPLOAD_RESERVE_S = 20 * 60
# Hard per-test timeout so a single heavy test (large fill / slow prewarm) cannot
# stall an entire pass.
PER_TEST_TIMEOUT_S = 120

# Stop launching perf tests once free disk drops below this. Tests that are killed
# by the per-test timeout never get to DROP the (often huge) tables they created, so
# disk usage grows steadily during collection; on the smaller runner this otherwise
# fills the disk and the later BOLT step dies with ENOSPC. The instrumented server's
# data dir (where those tables live) is reclaimed in full before the BOLT build, so
# this floor only has to keep collection itself from running the disk to zero.
MIN_FREE_DISK_GB = 15

PGO_PERF_RUNS = 2  # a couple of runs is enough to mark hot vs cold code for PGO
BOLT_PERF_RUNS = 1  # BOLT only needs hot-path coverage

# Maximum time to wait for a freshly-spawned `clickhouse-server` to start
# accepting connections. PGO-instrumented binaries are noticeably slower at
# startup (every instrumented site updates a counter), so the window has to be
# generous; for a normal binary readiness is reached within a few seconds.
SERVER_READINESS_TIMEOUT_S = 600
SERVER_READINESS_POLL_S = 2
# Hard timeout for a single `select 1` readiness probe. `--receive_timeout` only
# bounds the post-query receive phase, so a client that connects to a server stuck
# in startup (port already open, not yet answering) would block this one call
# forever — and then the deadline above is never re-checked, turning a stuck
# startup into a multi-hour job hang. `timeout` bounds every probe so the deadline
# is honoured and the job fails fast (dumping the server log) instead.
SERVER_READINESS_PROBE_TIMEOUT_S = 15

LLVM_VERSION = "21"


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    BUILD_PGO_INSTRUMENTED = "build_pgo"
    COLLECT_PGO_PROFILES = "collect_pgo"
    MERGE_PGO_PROFILES = "merge_pgo"
    BUILD_FOR_BOLT = "build_bolt"
    COLLECT_BOLT_PROFILES = "collect_bolt"
    MERGE_BOLT_PROFILES = "merge_bolt"


def get_arch():
    machine = platform.machine()
    if machine in ("x86_64", "amd64"):
        return "x86_64"
    elif machine in ("aarch64", "arm64"):
        return "aarch64"
    else:
        raise RuntimeError(f"Unsupported architecture: {machine}")


def get_toolchain_file():
    arch = get_arch()
    if arch == "x86_64":
        return f"{repo_path}/cmake/linux/toolchain-x86_64.cmake"
    else:
        return f"{repo_path}/cmake/linux/toolchain-aarch64.cmake"


def collection_budget(fixed_budget_s, reserve_after_s):
    """Clamp a fixed profile-collection budget to the time actually left in the job.

    `reserve_after_s` is wall-clock that must remain for work happening *after* this
    collection pass (e.g. the BOLT build, profile merges, artifact upload), so
    collection never eats into the time those steps need to finish before the job's
    hard timeout. Returns a non-negative number of seconds.
    """
    hard_deadline = JOB_START + JOB_TIMEOUT_S - reserve_after_s
    return max(0, min(fixed_budget_s, hard_deadline - time.monotonic()))


def run_shell(name, command, **kwargs):
    print(f"\n>>>> {name}\n")
    Shell.check(command, **kwargs)
    print(f"\n<<<< {name}\n")


def log_resources(stage):
    """Log free disk and memory at a stage boundary.

    The heavy phases (the instrumented build, profile collection against large
    datasets, and especially the second full ThinLTO build that coexists with the
    first build tree) can exhaust the runner's disk or RAM, which shows up only as
    an abrupt mid-operation kill. Emitting `df`/`free`/`du` here makes the resource
    that ran out unambiguous in the job log.
    """
    print(f"--- resources at [{stage}] ---")
    Shell.check(f"df -h {temp_dir} 2>/dev/null || df -h", verbose=True)
    Shell.check("free -h 2>/dev/null || head -3 /proc/meminfo", verbose=True)
    Shell.check(
        f"du -sh {PGO_BUILD_DIR} {BOLT_BUILD_DIR} {PERF_DB_PATH} {PGO_RAW_PROFILES_DIR} 2>/dev/null || :",
        verbose=True,
    )
    print("--- end resources ---")


def install_clickhouse(binary_path, server_dir):
    """Install ClickHouse binary and configs for running performance tests."""
    config_dir = f"{server_dir}/config"
    Shell.check(f"mkdir -p {config_dir}/config.d {config_dir}/users.d {server_dir}/db/user_files {server_dir}/top_level_domains")
    Shell.check(f"cp {repo_path}/programs/server/config.xml {config_dir}/")
    Shell.check(f"cp {repo_path}/programs/server/users.xml {config_dir}/")
    Shell.check(f"cp -r --dereference {repo_path}/programs/server/config.d/* {config_dir}/config.d/ || :")
    Shell.check(f"cp {repo_path}/tests/performance/scripts/config/config.d/*xml {config_dir}/config.d/")
    Shell.check(f"cp -r {repo_path}/tests/performance/scripts/config/users.d/* {config_dir}/users.d/ || :")
    Shell.check(f"cp -r {repo_path}/tests/config/top_level_domains/* {server_dir}/top_level_domains/ || :")
    # Remove configs that may cause issues
    Shell.check(f"rm -f {config_dir}/config.d/text_log.xml")
    Shell.check(f"rm -f {config_dir}/config.d/memory_profiler.yaml")
    Shell.check(f"rm -f {config_dir}/config.d/serverwide_trace_collector.xml")
    Shell.check(f"rm -f {config_dir}/config.d/jemalloc_flush_profile.yaml")
    Shell.check(f"rm -f {config_dir}/config.d/keeper_max_request_size.xml")
    Shell.check(f"rm -f {config_dir}/config.d/backups.xml")
    Shell.check(f"rm -f {config_dir}/config.d/ssh.xml")
    Shell.check(f"rm -f {config_dir}/config.d/storage_conf_local.xml")

    # The perf-comparison config (`zzz-perf-comparison-tweaks-config.xml`) force-enables
    # `remap_executable`. Remapping the `.text` segment of the running, PGO-instrumented
    # (and non-self-extracting) binary segfaults it on startup, so strip the setting; the
    # default `false` is what we want here — remapping to huge pages is an irrelevant
    # micro-optimization for profile collection.
    Shell.check(
        f"find {config_dir}/config.d -name '*.xml' -exec sed -i '/remap_executable/d' {{}} +"
    )

    Shell.check(f"chmod +x {binary_path}")
    Shell.check(f"ln -sf {binary_path} {server_dir}/clickhouse")
    Shell.check(f"ln -sf {binary_path} {server_dir}/clickhouse-server")
    Shell.check(f"ln -sf {binary_path} {server_dir}/clickhouse-client")
    Shell.check(f"ln -sf {binary_path} {server_dir}/clickhouse-local")
    Shell.check(f"ln -sf {binary_path} {server_dir}/clickhouse-keeper")


def download_datasets():
    """Download performance test datasets."""
    if os.path.exists(f"{PERF_DB_PATH}/.done"):
        print("Datasets already downloaded")
        return True
    Shell.check(f"mkdir -p {PERF_DB_PATH}/data/default/")
    # Deliberately omit the 100M-row hits dataset (`hits_100m_single`). For profile
    # *collection* the 10M-row set exercises the same code paths, while querying 100M
    # rows on the instrumented binary is the heaviest disk+memory consumer and the
    # most likely cause of a mid-collection out-of-resource kill. Perf tests that need
    # `hits_100m_single` simply fail their precondition and are skipped.
    dataset_paths = {
        "hits10": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_10m_single.tar",
        "hits1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
        "values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
        "tpch10": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch.tar",
    }
    errors = download_and_extract_datasets(dataset_paths.values(), PERF_DB_PATH)
    for error in errors:
        print(f"ERROR: {error}")
    if errors:
        return False
    Shell.check(f"touch {PERF_DB_PATH}/.done")
    return True


def dump_log_tail(log_path, lines=200):
    """Print the tail of a server log to stdout for CI diagnostics."""
    print(f"--- tail of {log_path} (last {lines} lines) ---")
    if not os.path.exists(log_path):
        print(f"(log file does not exist: {log_path})")
        print("--- end ---")
        return
    Shell.check(f"tail -n {lines} {log_path}", verbose=True)
    print("--- end ---")


def wait_for_server_ready(proc, server_dir, port, log_file):
    """Poll `select 1` until the server responds or the timeout elapses.

    Returns True on success. On failure prints the tail of `log_file` so the
    underlying reason (startup crash, slow init, port conflict, …) is visible
    in the job log instead of just a wall of failed `select 1` retries.
    """
    print(
        f"Waiting up to {SERVER_READINESS_TIMEOUT_S}s for server on port {port}"
    )
    start = time.monotonic()
    deadline = start + SERVER_READINESS_TIMEOUT_S
    next_progress = start + 30
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            print(f"Server process exited prematurely with code {proc.returncode}")
            dump_log_tail(log_file)
            return False
        # Polling is intentionally quiet — at one attempt every 2s a 10-minute
        # window would otherwise produce hundreds of identical `Run command`
        # lines; emit a progress heartbeat every 30s instead.
        res, out, _ = Shell.get_res_stdout_stderr(
            f"timeout -s KILL {SERVER_READINESS_PROBE_TIMEOUT_S} "
            f"{server_dir}/clickhouse-client --port {port} "
            f'--connect_timeout 5 --receive_timeout 5 --query "select 1"'
        )
        if out.strip() == "1":
            elapsed = time.monotonic() - start
            print(f"Server ready after {elapsed:.0f}s")
            return True
        now = time.monotonic()
        if now >= next_progress:
            print(f"  still waiting, {now - start:.0f}s elapsed")
            next_progress = now + 30
        time.sleep(SERVER_READINESS_POLL_S)

    print(
        f"Server did not become ready within {SERVER_READINESS_TIMEOUT_S}s"
    )
    dump_log_tail(log_file)
    return False


def start_server(server_dir, port=9000, keeper_port=9181, raft_port=9234):
    """Start a ClickHouse server and wait for it to be ready."""
    config_file = f"{server_dir}/config/config.xml"
    db_path = f"{server_dir}/db"
    log_file = f"{server_dir}/server.log"
    cmd = (
        f"{server_dir}/clickhouse-server --config-file={config_file} "
        f"-- --path {db_path} --user_files_path {db_path}/user_files "
        f"--top_level_domains_path {server_dir}/top_level_domains "
        f"--keeper_server.storage_path {server_dir}/coordination "
        f"--keeper_server.tcp_port {keeper_port} "
        f"--keeper_server.raft_configuration.server.port {raft_port} "
        f"--zookeeper.node.port {keeper_port} "
        f"--tcp_port {port}"
    )

    log_fd = open(log_file, "w")
    # Start in a new session so the spawned shell becomes the leader of its own
    # process group; otherwise `terminate_process_group(proc.pid)` in `stop_server`
    # could target the entire job's process group instead of just the server tree.
    proc = subprocess.Popen(
        cmd, stderr=subprocess.STDOUT, stdout=log_fd, shell=True, start_new_session=True
    )

    if wait_for_server_ready(proc, server_dir, port, log_file):
        return proc, log_fd

    # `shell=True` means `proc` is the wrapper shell, not `clickhouse-server`.
    # Tear down the whole process group via `stop_server` so the server itself
    # doesn't survive and clash with later stages on the same ports.
    stop_server(proc, log_fd)
    return None, None


def stop_server(proc, log_fd):
    """Stop a ClickHouse server."""
    if proc:
        Utils.terminate_process_group(proc.pid)
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except Exception:
            Utils.terminate_process_group(proc.pid, force=True)
            proc.wait()
    if log_fd:
        log_fd.close()


def install_perf_python_deps():
    """Install Python packages required by `tests/performance/scripts/perf.py`.

    The `clickhouse/binary-builder` Docker image used by this job inherits
    `scipy` from `clickhouse/fasttest` but does not bundle `clickhouse-driver`
    (it is only needed for running perf tests, not for builds). Install it on
    demand so `perf.py` can `import clickhouse_driver`.
    """
    Shell.check(
        "pip3 install --no-cache-dir 'clickhouse-driver==0.2.7'",
        verbose=True,
    )


def test_has_shell_query(test_path):
    """Whether a performance-test file contains a shell-script query.

    Profile collection runs every `tests/performance/*.xml` against a single,
    instrumented server started with only `--tcp_port` (its perf config removes
    `<http_port>`), and it invokes `perf.py` without `--binary` / `--http-port`.
    Shell-script queries (`<query type="shell">`) rely on exactly those: they build
    `$CLICKHOUSE_BINARY` / `$CLICKHOUSE_LOCAL` from `--binary` and `$CLICKHOUSE_URL`
    from `--http-port`. Here they would pick up whatever `clickhouse` is in `PATH`
    (not the instrumented binary) and hit an HTTP endpoint that is not listening, so
    such tests are skipped for profile collection rather than collecting profiles for
    the wrong executable or failing the pass. A parse error is treated as "no shell
    query" so the test still runs and `perf.py` reports the real error.
    """
    try:
        root = ET.parse(test_path).getroot()
    except ET.ParseError:
        return False
    return any(q.get("type") == "shell" for q in root.findall("query"))


def run_performance_tests(server_dir, port, runs, max_queries, time_budget_s):
    """Run performance tests against a single server to exercise code paths.

    This is profile *collection*, not benchmarking: we only need to drive the hot
    code paths, not produce stable timings. The instrumented (and, for the BOLT
    pass, un-optimized) binary is several times slower than a release build and
    some individual perf tests would otherwise run for tens of minutes, so three
    independent limits keep the pass inside its CI budget:

      * `time_budget_s`     - overall wall-clock budget for this pass; once it is
                              exhausted no further test files are started and the
                              remaining ones are logged as skipped (never silently
                              dropped);
      * `PER_TEST_TIMEOUT_S` - a hard per-test timeout via `timeout(1)`, so a single
                              heavy test (large table fill, slow prewarm) cannot
                              stall the whole pass;
      * small `--max-query-seconds` / `--prewarm-max-query-seconds` passed to
                              `perf.py`, so individual queries return quickly.
    """
    test_files = sorted(
        f for f in os.listdir(f"{repo_path}/tests/performance/") if f.endswith(".xml")
    )
    print(
        f"Running up to {len(test_files)} performance tests "
        f"(runs={runs}, max_queries={max_queries}, budget={time_budget_s:.0f}s, "
        f"per-test timeout={PER_TEST_TIMEOUT_S}s)"
    )

    deadline = time.monotonic() + time_budget_s
    ran = 0
    # For profile collection we run against a single server (left=right on same port)
    for i, test_file in enumerate(test_files):
        test_name = test_file.removesuffix(".xml")
        # Shell-script queries need the instrumented `--binary` and an HTTP port,
        # neither of which profile collection passes; they exercise startup / HTTP
        # timing rather than query code paths, so they are useless for PGO/BOLT
        # profiles. Skip them here (logged, never silently dropped).
        if test_has_shell_query(f"{repo_path}/tests/performance/{test_file}"):
            print(f"  Skipping {test_name}: shell-script query test, not used for profile collection")
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            print(
                f"  Time budget of {time_budget_s:.0f}s exhausted after {ran} test(s); "
                f"skipping {len(test_files) - i} remaining test(s)"
            )
            break
        free_gb = shutil.disk_usage(temp_dir).free / (1024 ** 3)
        if free_gb < MIN_FREE_DISK_GB:
            print(
                f"  Free disk {free_gb:.1f}G below {MIN_FREE_DISK_GB}G floor after {ran} test(s); "
                f"stopping collection to leave room for the BOLT build; "
                f"skipping {len(test_files) - i} remaining test(s)"
            )
            break
        print(f"  Running: {test_name}")
        # Never let the last test overrun the overall deadline.
        per_test = int(min(PER_TEST_TIMEOUT_S, remaining))
        res, out, err = Shell.get_res_stdout_stderr(
            f"timeout -s KILL {per_test} "
            f"{repo_path}/tests/performance/scripts/perf.py "
            f"--host localhost localhost "
            f"--port {port} {port} "
            f"--runs {runs} --max-queries {max_queries} "
            f"--max-query-seconds 15 --prewarm-max-query-seconds 15 "
            f"--profile-seconds 0 "
            f"{repo_path}/tests/performance/{test_file}",
            verbose=True,
            strip=False,
        )
        ran += 1
        if res != 0:
            # Non-zero is expected and harmless here (a query slower than the per-query
            # cap, or `timeout` killing a long test); the profiles gathered so far are
            # still valid, so log it and move on.
            print(f"  WARNING: test {test_name} did not complete cleanly (exit {res}); continuing: {err[:200]}")
        # Periodic resource snapshot: collection against large datasets accumulates
        # disk (profraw files) and server memory, and an abrupt kill mid-collection
        # otherwise gives no clue which ran out.
        if ran % 20 == 0:
            log_resources(f"after {ran} perf tests")
    print(f"Ran {ran} performance test file(s) for profile collection")


def configure_datasets(server_dir, port=9000):
    """Start server with preconfigured datasets, then set up the database."""
    Shell.check(
        f'echo "ATTACH DATABASE default ENGINE=Ordinary" > {PERF_DB_PATH}/metadata/default.sql'
    )
    Shell.check(
        f'echo "ATTACH DATABASE datasets ENGINE=Ordinary" > {PERF_DB_PATH}/metadata/datasets.sql'
    )

    # Start a temporary server to set up the datasets
    config_file = f"{server_dir}/config/config.xml"
    log_file = f"{server_dir}/preconfig.log"
    cmd = (
        f"{server_dir}/clickhouse-server --config-file={config_file} "
        f"-- --path {PERF_DB_PATH} --user_files_path {PERF_DB_PATH}/user_files "
        f"--top_level_domains_path {server_dir}/top_level_domains "
        f"--keeper_server.storage_path {PERF_WD}/coordination0 "
        f"--tcp_port {port}"
    )
    log_fd = open(log_file, "w")
    # See note in `start_server`: dedicated session keeps `terminate_process_group`
    # scoped to the server tree.
    proc = subprocess.Popen(
        cmd, stderr=subprocess.STDOUT, stdout=log_fd, shell=True, start_new_session=True
    )
    if not wait_for_server_ready(proc, server_dir, port, log_file):
        stop_server(proc, log_fd)
        return False

    client = f"{server_dir}/clickhouse-client --port {port}"
    if not Shell.check(
        f"{client} --query 'CREATE DATABASE IF NOT EXISTS test'",
        verbose=True,
    ):
        stop_server(proc, log_fd)
        return False
    # The dataset directory is shared between PGO and BOLT passes, so the rename
    # only happens on the first run; skip it when the source table is gone.
    res, out, _ = Shell.get_res_stdout_stderr(
        f"{client} --query 'EXISTS TABLE datasets.hits_v1'",
        verbose=True,
    )
    if res != 0:
        stop_server(proc, log_fd)
        return False
    if out.strip() == "1":
        if not Shell.check(
            f"{client} --query 'RENAME TABLE datasets.hits_v1 TO test.hits'",
            verbose=True,
        ):
            stop_server(proc, log_fd)
            return False
    stop_server(proc, log_fd)
    time.sleep(3)

    # Copy database for server
    Shell.check(f"rm -rf {server_dir}/db")
    Shell.check(f"rm -rf {PERF_DB_PATH}/preprocessed_configs {PERF_DB_PATH}/data/system {PERF_DB_PATH}/metadata/system {PERF_DB_PATH}/status")
    Shell.check(f"cp -al {PERF_DB_PATH} {server_dir}/db || cp -r {PERF_DB_PATH} {server_dir}/db")
    Shell.check(f"cp -R {PERF_WD}/coordination0 {server_dir}/coordination || mkdir -p {server_dir}/coordination")
    # Symlink user_files from the repository
    Shell.check(
        f'for f in {repo_path}/tests/performance/user_files/*; do [ -e "$f" ] || continue; '
        f'ln -sf "$(readlink -f "$f")" {server_dir}/db/user_files/; done'
    )
    return True


def parse_args():
    import argparse

    parser = argparse.ArgumentParser(description="Collect ClickHouse PGO/BOLT profiles")
    parser.add_argument(
        "--param",
        help="Start from this stage (for resuming after partial runs)",
        default=None,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(temp_dir, exist_ok=True)

    stages = list(JobStages)
    if args.param:
        assert args.param in JobStages, f"--param must be one of {list(JobStages)}"
        print(f"Resuming from stage [{args.param}]")
        while stages and stages[0] != args.param:
            stages.pop(0)

    res = True
    results = []

    toolchain_file = get_toolchain_file()

    if os.getuid() == 0:
        Shell.check(f"git config --global --add safe.directory {current_directory}")

    # --- Stage: Checkout submodules ---
    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        def do_checkout():
            r = Shell.check(f"mkdir -p {PGO_BUILD_DIR} && git submodule sync && git submodule init")
            r = r and Shell.check("contrib/update-submodules.sh --max-procs 10", retries=3)
            return r

        results.append(
            Result.from_commands_run(name="Checkout Submodules", command=do_checkout)
        )
        res = results[-1].is_ok()

    # --- Stage: Build PGO-instrumented ClickHouse ---
    if res and JobStages.BUILD_PGO_INSTRUMENTED in stages:
        os.makedirs(PGO_BUILD_DIR, exist_ok=True)
        os.makedirs(PGO_RAW_PROFILES_DIR, exist_ok=True)
        # Set LLVM_PROFILE_FILE so that profraw files go to a known directory
        os.environ["LLVM_PROFILE_FILE"] = f"{PGO_RAW_PROFILES_DIR}/default_%m_%p.profraw"

        cmake_cmd = (
            f"cmake -DCMAKE_VERBOSE_MAKEFILE=1 -LA "
            f"-DCMAKE_BUILD_TYPE=None "
            f"-DENABLE_THINLTO=0 "
            f"-DENABLE_CLICKHOUSE_PGO_GENERATE=ON "
            f"-DSANITIZE= "
            f"-DBUILD_STRIPPED_BINARY=0 "
            f"-DENABLE_CLICKHOUSE_SELF_EXTRACTING=0 "
            f"-DCMAKE_C_COMPILER={ToolSet.COMPILER_C} "
            f"-DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} "
            f"-DCMAKE_TOOLCHAIN_FILE={toolchain_file} "
            f"-DENABLE_TESTS=0 "
            f"-DENABLE_UTILS=0 "
            f"-DCHECK_LARGE_OBJECT_SIZES=0 "
            f"-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON "
            f"{repo_path} -B {PGO_BUILD_DIR}"
        )
        results.append(
            Result.from_commands_run(
                name="CMake (PGO instrumented)",
                command=cmake_cmd,
                workdir=PGO_BUILD_DIR,
            )
        )
        res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Build (PGO instrumented)",
                    command="command time -v ninja clickhouse",
                    workdir=PGO_BUILD_DIR,
                )
            )
            res = results[-1].is_ok()
            if res:
                log_resources("after PGO instrumented build")

    # --- Stage: Collect PGO profiles ---
    if res and JobStages.COLLECT_PGO_PROFILES in stages:
        pgo_binary = f"{PGO_BUILD_DIR}/programs/clickhouse"
        pgo_server_dir = f"{PERF_WD}/pgo_server"
        os.makedirs(pgo_server_dir, exist_ok=True)

        # Ensure LLVM_PROFILE_FILE is set for the server process
        os.environ["LLVM_PROFILE_FILE"] = f"{PGO_RAW_PROFILES_DIR}/server_%m_%p.profraw"

        def collect_pgo():
            install_clickhouse(pgo_binary, pgo_server_dir)
            install_perf_python_deps()
            if not download_datasets():
                return False
            if not configure_datasets(pgo_server_dir, port=9000):
                return False

            proc, log_fd = start_server(pgo_server_dir, port=9000)
            if not proc:
                return False
            try:
                # Reserve time for the BOLT build, both merges and the upload that
                # still have to run after this pass.
                budget = collection_budget(
                    PGO_COLLECT_BUDGET_S,
                    BOLT_BUILD_RESERVE_S + BOLT_COLLECT_BUDGET_S + UPLOAD_RESERVE_S,
                )
                run_performance_tests(
                    pgo_server_dir,
                    port=9000,
                    runs=PGO_PERF_RUNS,
                    max_queries=10,
                    time_budget_s=budget,
                )
            finally:
                stop_server(proc, log_fd)
                # Give time for profraw files to be flushed
                time.sleep(5)
            return True

        results.append(
            Result.from_commands_run(name="Collect PGO profiles", command=collect_pgo)
        )
        res = results[-1].is_ok()

    # --- Stage: Merge PGO profiles ---
    if res and JobStages.MERGE_PGO_PROFILES in stages:
        profraw_files = glob.glob(f"{PGO_RAW_PROFILES_DIR}/*.profraw")
        print(f"Found {len(profraw_files)} profraw files")

        if not profraw_files:
            print("ERROR: No profraw files found")
            results.append(Result(name="Merge PGO profiles", status=Result.Status.ERROR))
            res = False
        else:
            results.append(
                Result.from_commands_run(
                    name="Merge PGO profiles",
                    command=f"llvm-profdata-{LLVM_VERSION} merge -output={PGO_PROFDATA_PATH} {PGO_RAW_PROFILES_DIR}/*.profraw",
                )
            )
            res = results[-1].is_ok()
            if res:
                size = os.path.getsize(PGO_PROFDATA_PATH)
                print(f"PGO profile size: {size / 1024 / 1024:.1f} MB")

    # --- Stage: Build ClickHouse for BOLT ---
    if res and JobStages.BUILD_FOR_BOLT in stages:
        # The instrumented build, its raw profraw files, and the instrumented server's
        # data directory (which holds tens of GB of tables left behind by perf tests
        # whose `perf.py` was killed by the per-test timeout before it could DROP them)
        # are all unneeded once the profile is merged — the BOLT build only reads
        # PGO_PROFDATA_PATH, and the BOLT pass starts its own fresh server. Remove them
        # before the second full build so two complete build trees plus the datasets
        # don't exhaust the runner's disk.
        log_resources("before freeing PGO build dir")
        Shell.check(f"rm -rf {PGO_BUILD_DIR} {PGO_RAW_PROFILES_DIR} {PERF_WD}/pgo_server")
        log_resources("before BOLT build")
        os.makedirs(BOLT_BUILD_DIR, exist_ok=True)

        cmake_cmd = (
            f"cmake -DCMAKE_VERBOSE_MAKEFILE=1 -LA "
            f"-DCMAKE_BUILD_TYPE=None "
            f"-DENABLE_THINLTO=1 "
            f"-DCLICKHOUSE_PGO_PROFILE_PATH={PGO_PROFDATA_PATH} "
            f"-DENABLE_CLICKHOUSE_BOLT=ON "
            f"-DSANITIZE= "
            f"-DBUILD_STRIPPED_BINARY=0 "
            f"-DENABLE_CLICKHOUSE_SELF_EXTRACTING=0 "
            f"-DCMAKE_C_COMPILER={ToolSet.COMPILER_C} "
            f"-DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} "
            f"-DCMAKE_TOOLCHAIN_FILE={toolchain_file} "
            f"-DENABLE_TESTS=0 "
            f"-DENABLE_UTILS=0 "
            f"-DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON "
            f"-DSPLIT_DEBUG_SYMBOLS=OFF "
            f"{repo_path} -B {BOLT_BUILD_DIR}"
        )
        results.append(
            Result.from_commands_run(
                name="CMake (BOLT)",
                command=cmake_cmd,
                workdir=BOLT_BUILD_DIR,
            )
        )
        res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Build (BOLT)",
                    command="command time -v ninja clickhouse",
                    workdir=BOLT_BUILD_DIR,
                )
            )
            res = results[-1].is_ok()

    # --- Stage: Collect BOLT profiles ---
    if res and JobStages.COLLECT_BOLT_PROFILES in stages:
        bolt_binary = f"{BOLT_BUILD_DIR}/programs/clickhouse"
        bolt_instrumented = f"{BOLT_BUILD_DIR}/programs/clickhouse.bolt-inst"
        bolt_server_dir = f"{PERF_WD}/bolt_server"
        os.makedirs(bolt_server_dir, exist_ok=True)
        os.makedirs(BOLT_PROFILES_DIR, exist_ok=True)

        # Instrument with BOLT
        bolt_instrument_cmd = (
            f"llvm-bolt-{LLVM_VERSION} {bolt_binary} "
            f"-instrument "
            f"-o {bolt_instrumented} "
            f"--instrumentation-file-append-pid "
            f"--instrumentation-file={BOLT_PROFILES_DIR}/prof"
        )

        results.append(
            Result.from_commands_run(
                name="BOLT instrument",
                command=bolt_instrument_cmd,
            )
        )
        if not results[-1].is_ok():
            # BOLT instrumentation is best-effort; aarch64 may fail
            print("WARNING: BOLT instrumentation failed, skipping BOLT profile collection")
            results[-1] = Result(
                name="BOLT instrument (skipped)",
                status=Result.Status.OK,
                info="BOLT instrumentation failed (best-effort), skipping",
            )
        else:
            def collect_bolt():
                install_clickhouse(bolt_instrumented, bolt_server_dir)
                # Reuse datasets already downloaded
                if not configure_datasets(bolt_server_dir, port=9100):
                    return False

                proc, log_fd = start_server(bolt_server_dir, port=9100, keeper_port=9281, raft_port=9334)
                if not proc:
                    return False
                try:
                    # Last collection pass: only the merges and the upload follow.
                    budget = collection_budget(BOLT_COLLECT_BUDGET_S, UPLOAD_RESERVE_S)
                    run_performance_tests(
                        bolt_server_dir,
                        port=9100,
                        runs=BOLT_PERF_RUNS,
                        max_queries=5,
                        time_budget_s=budget,
                    )
                finally:
                    stop_server(proc, log_fd)
                    time.sleep(5)
                return True

            results.append(
                Result.from_commands_run(name="Collect BOLT profiles", command=collect_bolt)
            )
            res = results[-1].is_ok()

    # --- Stage: Merge BOLT profiles ---
    if res and JobStages.MERGE_BOLT_PROFILES in stages:
        fdata_files = glob.glob(f"{BOLT_PROFILES_DIR}/prof*")
        if not fdata_files:
            print("No BOLT profile data found (BOLT was likely skipped)")
            # Create an empty marker so the artifact still uploads
            Shell.check(f"touch {BOLT_FDATA_PATH}")
        else:
            print(f"Found {len(fdata_files)} BOLT fdata files")
            results.append(
                Result.from_commands_run(
                    name="Merge BOLT profiles",
                    command=f"merge-fdata-{LLVM_VERSION} {BOLT_PROFILES_DIR}/prof* > {BOLT_FDATA_PATH}",
                )
            )
            if results[-1].is_ok():
                size = os.path.getsize(BOLT_FDATA_PATH)
                print(f"BOLT profile size: {size / 1024 / 1024:.1f} MB")

    # Compress profiles for upload
    if os.path.exists(PGO_PROFDATA_PATH):
        Shell.check(f"zstd -19 -f {PGO_PROFDATA_PATH} -o {temp_dir}/clickhouse-pgo.profdata.zst")
    if os.path.exists(BOLT_FDATA_PATH):
        Shell.check(f"zstd -19 -f {BOLT_FDATA_PATH} -o {temp_dir}/clickhouse-bolt.fdata.zst")

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
