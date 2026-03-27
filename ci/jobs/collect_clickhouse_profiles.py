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
import subprocess
import time

from ci.defs.defs import BuildTypes, ToolSet
from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp"
repo_path = "/ClickHouse"

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

# Limits for BOLT profile collection (disk space)
BOLT_PROFILE_TIMEOUT = 1800  # 30 minutes
BOLT_PERF_RUNS = 3  # fewer runs, just need hot paths

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


def run_shell(name, command, **kwargs):
    print(f"\n>>>> {name}\n")
    Shell.check(command, **kwargs)
    print(f"\n<<<< {name}\n")


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
    dataset_paths = {
        "hits10": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_10m_single.tar",
        "hits100": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_100m_single.tar",
        "hits1": "https://clickhouse-datasets.s3.amazonaws.com/hits/partitions/hits_v1.tar",
        "values": "https://clickhouse-datasets.s3.amazonaws.com/values_with_expressions/partitions/test_values.tar",
        "tpch10": "https://clickhouse-datasets.s3.amazonaws.com/h/10/tpch.tar",
    }
    cmds = []
    for dataset_path in dataset_paths.values():
        cmds.append(
            f'wget -nv -nd -c "{dataset_path}" -O- | tar --extract --verbose -C {PERF_DB_PATH}'
        )
    res = Shell.check_parallel(cmds, verbose=True)
    if res:
        Shell.check(f"touch {PERF_DB_PATH}/.done")
    return res


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
    proc = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=log_fd, shell=True)
    time.sleep(2)
    if proc.poll() is not None:
        log_fd.close()
        print(f"Server failed to start, check {log_file}")
        return None, None

    # Wait for readiness
    for attempt in range(30):
        res, out, _ = Shell.get_res_stdout_stderr(
            f'clickhouse-client --port {port} --query "select 1"', verbose=True
        )
        if out.strip() == "1":
            print("Server ready")
            return proc, log_fd
        time.sleep(2)

    print("Server did not become ready")
    proc.terminate()
    log_fd.close()
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


def run_performance_tests(server_dir, port=9000, runs=7, max_queries=10):
    """Run all performance tests against a single server to exercise code paths."""
    test_files = sorted(
        f for f in os.listdir(f"{repo_path}/tests/performance/") if f.endswith(".xml")
    )
    print(f"Running {len(test_files)} performance tests with {runs} runs each")

    # For profile collection we run against a single server (left=right on same port)
    for test_file in test_files:
        test_name = test_file.removesuffix(".xml")
        print(f"  Running: {test_name}")
        benchmarks = {"clickbench.xml", "tpch.xml"}
        mq = 0 if test_file in benchmarks else max_queries
        res, out, err = Shell.get_res_stdout_stderr(
            f"{repo_path}/tests/performance/scripts/perf.py "
            f"--host localhost localhost "
            f"--port {port} {port} "
            f"--runs {runs} --max-queries {mq} "
            f"--profile-seconds 0 "
            f"{repo_path}/tests/performance/{test_file}",
            verbose=True,
            strip=False,
        )
        if res != 0:
            print(f"  WARNING: test {test_name} failed (continuing): {err[:200]}")


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
    cmd = (
        f"{server_dir}/clickhouse-server --config-file={config_file} "
        f"-- --path {PERF_DB_PATH} --user_files_path {PERF_DB_PATH}/user_files "
        f"--top_level_domains_path {server_dir}/top_level_domains "
        f"--keeper_server.storage_path {PERF_WD}/coordination0 "
        f"--tcp_port {port}"
    )
    log_fd = open(f"{server_dir}/preconfig.log", "w")
    proc = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=log_fd, shell=True)
    time.sleep(2)
    for attempt in range(30):
        res, out, _ = Shell.get_res_stdout_stderr(
            f'clickhouse-client --port {port} --query "select 1"', verbose=True
        )
        if out.strip() == "1":
            break
        time.sleep(2)
    else:
        stop_server(proc, log_fd)
        return False

    Shell.check(
        f"clickhouse-client --port {port} --query 'create database IF NOT EXISTS test' "
        f"&& clickhouse-client --port {port} --query 'rename table datasets.hits_v1 to test.hits'",
        verbose=True,
    )
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


def main():
    os.makedirs(temp_dir, exist_ok=True)

    stages = list(JobStages)
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
                    command="command time -v ninja clickhouse-bundle",
                    workdir=PGO_BUILD_DIR,
                )
            )
            res = results[-1].is_ok()

    # --- Stage: Collect PGO profiles ---
    if res and JobStages.COLLECT_PGO_PROFILES in stages:
        pgo_binary = f"{PGO_BUILD_DIR}/programs/clickhouse"
        pgo_server_dir = f"{PERF_WD}/pgo_server"
        os.makedirs(pgo_server_dir, exist_ok=True)

        # Ensure LLVM_PROFILE_FILE is set for the server process
        os.environ["LLVM_PROFILE_FILE"] = f"{PGO_RAW_PROFILES_DIR}/server_%m_%p.profraw"

        def collect_pgo():
            install_clickhouse(pgo_binary, pgo_server_dir)
            if not download_datasets():
                return False
            if not configure_datasets(pgo_server_dir, port=9000):
                return False

            proc, log_fd = start_server(pgo_server_dir, port=9000)
            if not proc:
                return False
            try:
                run_performance_tests(pgo_server_dir, port=9000, runs=5, max_queries=10)
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
                status=Result.Status.SUCCESS,
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
                    run_performance_tests(bolt_server_dir, port=9100, runs=BOLT_PERF_RUNS, max_queries=5)
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
                    command=f"merge-fdata {BOLT_PROFILES_DIR}/prof* > {BOLT_FDATA_PATH}",
                )
            )
            if results[-1].is_ok():
                size = os.path.getsize(BOLT_FDATA_PATH)
                print(f"BOLT profile size: {size / 1024 / 1024:.1f} MB")

    # Compress profiles for upload
    if os.path.exists(PGO_PROFDATA_PATH):
        Shell.check(f"zstd -19 -f {PGO_PROFDATA_PATH} -o {temp_dir}/clickhouse-pgo.profdata.zst")
    if os.path.exists(BOLT_FDATA_PATH) and os.path.getsize(BOLT_FDATA_PATH) > 0:
        Shell.check(f"zstd -19 -f {BOLT_FDATA_PATH} -o {temp_dir}/clickhouse-bolt.fdata.zst")

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
