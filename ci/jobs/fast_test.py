import argparse
import os
import platform
import time
import sys
from pathlib import Path

repo_path = Path(__file__).resolve().parent.parent.parent
repo_path_normalized = str(repo_path)
sys.path.append(str(repo_path / "ci"))

from ci.defs.defs import ToolSet, chcache_secret
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import ContextManager, MetaClasses, Shell, Utils

current_directory = Utils.cwd()
build_dir = f"{current_directory}/ci/tmp/fast_build"
temp_dir = f"{current_directory}/ci/tmp/"
build_dir_normalized = str(repo_path / "ci" / "tmp" / "fast_build")


def clone_submodules():
    submodules_to_update = [
        "contrib/sysroot",
        "contrib/magic_enum",
        "contrib/abseil-cpp",
        "contrib/boost",
        "contrib/zlib-ng",
        "contrib/libxml2",
        "contrib/fmtlib",
        "contrib/base64",
        "contrib/cctz",
        "contrib/libcpuid",
        "contrib/libdivide",
        "contrib/double-conversion",
        "contrib/llvm-project",
        "contrib/lz4",
        "contrib/zstd",
        "contrib/fastops",
        "contrib/rapidjson",
        "contrib/re2",
        "contrib/sparsehash-c11",
        "contrib/croaring",
        "contrib/miniselect",
        "contrib/xz",
        "contrib/zmij",
        "contrib/fast_float",
        "contrib/NuRaft",
        "contrib/jemalloc",
        "contrib/replxx",
        "contrib/wyhash",
        "contrib/c-ares",
        "contrib/morton-nd",
        "contrib/xxHash",
        "contrib/simdjson",
        "contrib/simdcomp",
        "contrib/liburing",
        "contrib/libfiu",
        "contrib/yaml-cpp",
        "contrib/corrosion",
        "contrib/StringZilla",
        "contrib/rust_vendor",
        "contrib/clickstack",
    ]

    res = Shell.check("git submodule sync", verbose=True, strict=True)
    res = res and Shell.check(
        # Init only the needed submodules, not all 129
        command="git submodule init -- " + " ".join(submodules_to_update),
        verbose=True,
        strict=True,
    )

    if os.path.isdir(".git/modules/contrib") and os.listdir(".git/modules/contrib"):
        # Submodule cache was restored by runner.py — just populate working trees
        print("Submodule cache detected, populating working trees from cache")
        res = res and Shell.check(
            command="git submodule update --depth 1 --single-branch -- " + " ".join(submodules_to_update),
            timeout=300,
            retries=3,
            verbose=True,
        )
    else:
        res = res and Shell.check(
            command=f"xargs --max-procs={min([Utils.cpu_count(), 20])} --null --no-run-if-empty --max-args=1 git submodule update --depth 1 --single-branch",
            stdin_str="\0".join(submodules_to_update) + "\0",
            timeout=300,
            retries=3,
            verbose=True,
        )
    # NOTE: the three "git submodule foreach" cleanup commands (reset --hard,
    # checkout @ -f, clean -xfd) that used to run here were removed because
    # "git submodule update" already checks out the correct commit into a
    # fresh clone.  The foreach commands added ~7s of sequential overhead
    # iterating over every submodule for no benefit in the fast-test context.
    return res


def update_path_ch_config(config_file_path=""):
    print("Updating path in clickhouse config")
    config_file_path = (
        config_file_path or f"{temp_dir}/etc/clickhouse-server/config.xml"
    )
    ssl_config_file_path = f"{temp_dir}/etc/clickhouse-server/config.d/ssl_certs.xml"
    try:
        with open(config_file_path, "r", encoding="utf-8") as file:
            content = file.read()

        with open(ssl_config_file_path, "r", encoding="utf-8") as file:
            ssl_config_content = file.read()
        content = content.replace(">/var/", f">{temp_dir}/var/")
        content = content.replace(">/etc/", f">{temp_dir}/etc/")
        ssl_config_content = ssl_config_content.replace(">/etc/", f">{temp_dir}/etc/")
        with open(config_file_path, "w", encoding="utf-8") as file:
            file.write(content)
        with open(ssl_config_file_path, "w", encoding="utf-8") as file:
            file.write(ssl_config_content)
    except Exception as e:
        print(f"ERROR: failed to update config, exception: {e}")
        return False
    return True


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    BUILD = "build"
    CONFIG = "config"
    TEST = "test"


def _load_darwin_skip_tests():
    skip_file = Path(__file__).resolve().parent.parent / "defs" / "darwin.skip"
    return tuple(line for line in skip_file.read_text().splitlines() if line.strip())


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Fast Test Job")
    parser.add_argument(
        "--test",
        help="Optional. Space-separated test name patterns",
        default=[],
        nargs="+",
        action="extend")
    parser.add_argument(
        "--skip",
        help="Optional. Space-separated test names to skip",
        default=[],
        nargs="+",
        action="extend")
    parser.add_argument("--param", help="Optional custom job start stage", default=None)
    parser.add_argument("--set-status-success", help="Forcefully set a green status", action="store_true")
    return parser.parse_args()

def main():
    args = parse_args()
    if platform.system() == "Darwin":
        args.skip = list(_load_darwin_skip_tests()) + args.skip
    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    clickhouse_bin_path = Path(f"{build_dir}/programs/clickhouse")

    for path in [
        Path(temp_dir) / "clickhouse",
        clickhouse_bin_path,
        Path(current_directory) / "clickhouse",
    ]:
        if path.is_file():
            clickhouse_bin_path = path
            print(f"NOTE: clickhouse binary is found [{clickhouse_bin_path}] - skip the build")

            stages = [JobStages.CONFIG, JobStages.TEST]
            resolved_clickhouse_bin_path = clickhouse_bin_path.resolve()
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-server")
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-client")
            Utils.link(resolved_clickhouse_bin_path, resolved_clickhouse_bin_path.parent / "clickhouse-local")
            Shell.check(f"chmod +x {resolved_clickhouse_bin_path}", strict=True)

            break
    else:
        print(
            f"NOTE: clickhouse binary is not found [{clickhouse_bin_path}] - will be built"
        )

    # Global sccache settings for local and CI runs
    os.environ["SCCACHE_DIR"] = f"{temp_dir}/sccache"
    os.environ["SCCACHE_CACHE_SIZE"] = "40G"
    os.environ["SCCACHE_IDLE_TIMEOUT"] = "7200"
    os.environ["SCCACHE_BUCKET"] = Settings.S3_ARTIFACT_PATH
    os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    os.environ["SCCACHE_ERROR_LOG"] = f"{build_dir}/sccache.log"
    os.environ["SCCACHE_LOG"] = "info"
    info = Info()
    if info.is_local_run:
        print("NOTE: It's a local run")
        if os.environ.get("SCCACHE_ENDPOINT"):
            print(f"NOTE: Using custom sccache endpoint: {os.environ['SCCACHE_ENDPOINT']}")
        if os.environ.get("AWS_ACCESS_KEY_ID"):
            print("NOTE: Using custom AWS credentials for sccache")
        else:
            os.environ["SCCACHE_S3_NO_CREDENTIALS"] = "true"
    else:
        os.environ["CH_HOSTNAME"] = (
            "https://build-cache.eu-west-1.aws.clickhouse-staging.com"
        )
        os.environ["CH_USER"] = "ci_builder"
        os.environ["CH_PASSWORD"] = chcache_secret.get_value()
        os.environ["CH_USE_LOCAL_CACHE"] = "false"

    Utils.add_to_PATH(
        f"{os.path.dirname(clickhouse_bin_path)}:{current_directory}/tests"
    )

    res = True
    results = []
    attach_files = []
    job_info = ""

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        results.append(
            Result.from_commands_run(
                name="Checkout Submodules",
                command=clone_submodules,
            )
        )
        res = results[-1].is_ok()

    os.makedirs(build_dir, exist_ok=True)

    if res and JobStages.CMAKE in stages:
        # The sccache server sometimes fails to start because of issues with S3.
        # Start it explicitly with retries before cmake, since cmake can invoke
        # the compiler during configuration. Non-fatal: build can proceed without it.
        if not Shell.check("sccache --start-server", retries=3):
            print("WARNING: sccache server failed to start, build will proceed without it")
        results.append(
            # TODO: commented out to make job platform agnostic
            #   -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake \
            Result.from_commands_run(
                name="Cmake configuration",
                command=f"cmake {repo_path_normalized} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} \
                -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} \
                -DCOMPILER_CACHE={ToolSet.COMPILER_CACHE} \
                -DENABLE_LIBRARIES=0 \
                -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 -DENABLE_NURAFT=1 -DENABLE_SIMDJSON=1 \
                -DENABLE_LEXER_TEST=1 \
                -DBUILD_STRIPPED_BINARY=1 \
                -DENABLE_JEMALLOC=1 -DENABLE_LIBURING=1 -DENABLE_YAML_CPP=1 -DENABLE_RUST=1 \
                -DUSE_SYSTEM_COMPILER_RT=1 \
                -B {build_dir_normalized}",
                workdir=repo_path_normalized,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        Shell.check("sccache --show-stats")
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"command time -v cmake --build {build_dir_normalized} --"
                " clickhouse-bundle clickhouse-stripped lexer_test",
            )
        )
        Shell.check(f"{build_dir}/rust/chcache/chcache stats")
        Shell.check("sccache --show-stats")
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        commands = [
            "sccache --show-stats",
            "clickhouse-client --version",
        ]
        results.append(
            Result.from_commands_run(
                name="Check and Compress binary",
                command=commands,
                workdir=build_dir_normalized,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CONFIG in stages:
        commands = [
            f"mkdir -p {temp_dir}/etc/clickhouse-server",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {temp_dir}/etc/clickhouse-server/",
            f"./tests/config/install.sh {temp_dir}/etc/clickhouse-server {temp_dir}/etc/clickhouse-client --fast-test",
            # f"cp -a {current_directory}/programs/server/config.d/log_to_console.xml {temp_dir}/etc/clickhouse-server/config.d/",
            f"rm -f {temp_dir}/etc/clickhouse-server/config.d/secure_ports.xml",
            update_path_ch_config,
        ]
        results.append(
            Result.from_commands_run(
                name="Install ClickHouse Config",
                command=commands,
            )
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc(
        ch_config_dir=f"{temp_dir}/etc/clickhouse-server",
        ch_var_lib_dir=f"{temp_dir}/var/lib/clickhouse",
    )
    CH.install_configs()

    attach_debug = False
    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        res = CH.start()
        res = res and CH.wait_ready()
        results.append(
            Result.create_from(name=step_name, status=res, stopwatch=stop_watch_)
        )
        if not results[-1].is_ok():
            attach_debug = True

    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)

        # Fast test runs lightweight SQL tests that are not CPU-bound,
        # so we can use more parallelism than the default cpu_count/2.
        nproc_fast = max(1, int(Utils.cpu_count() * 3 / 4))

        fast_test_command = f"cd {temp_dir} && clickhouse-test --hung-check --trace --capture-client-stacktrace --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --check-zookeeper-session --order random --report-logs-stats --fast-tests-only --no-stateful --timeout 60 --jobs {nproc_fast}"
        if args.skip:
            skip_args = " ".join(args.skip)
            fast_test_command += f" --skip {skip_args}"
        if args.test:
            test_pattern = "|".join(args.test)
            fast_test_command += f" -- '{test_pattern}'"

        res = CH.run_test(fast_test_command)

        test_results = FTResultsProcessor(wd=Settings.OUTPUT_DIR).run()
        if not res:
            test_results.results.append(
                Result.create_from(
                    name="clickhouse-test",
                    status=Result.Status.FAIL,
                    info="clickhouse-test error",
                )
            )
            attach_debug = True

        results.append(test_results)
        results[-1].set_timing(stopwatch=stop_watch_)
        if not results[-1].is_ok():
            attach_debug = True
        job_info = results[-1].info

    if attach_debug:
        attach_files += [
            clickhouse_bin_path,
            *CH.prepare_logs(info=info, all=True),
        ]

    CH.terminate(force=True)

    status = Result.Status.OK if args.set_status_success else ""
    Result.create_from(
        results=results, status=status, stopwatch=stop_watch, files=attach_files, info=job_info
    ).complete_job()


if __name__ == "__main__":
    main()
