import argparse

from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.jobs.scripts.functional_tests_results import FTResultsProcessor
from ci.workflows.defs import ToolSet


def clone_submodules():
    submodules_to_update = [
        "contrib/sysroot",
        "contrib/magic_enum",
        "contrib/abseil-cpp",
        "contrib/boost",
        "contrib/zlib-ng",
        "contrib/libxml2",
        "contrib/libunwind",
        "contrib/fmtlib",
        "contrib/aklomp-base64",
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
        "contrib/dragonbox",
        "contrib/fast_float",
        "contrib/NuRaft",
        "contrib/jemalloc",
        "contrib/replxx",
        "contrib/wyhash",
        "contrib/c-ares",
        "contrib/morton-nd",
        "contrib/xxHash",
        "contrib/expected",
        "contrib/simdjson",
        "contrib/liburing",
        "contrib/libfiu",
        "contrib/incbin",
        "contrib/yaml-cpp",
    ]

    res = Shell.check("git submodule sync", verbose=True, strict=True)
    res = res and Shell.check("git submodule init", verbose=True, strict=True)
    res = res and Shell.check(
        command=f"xargs --max-procs={min([Utils.cpu_count(), 20])} --null --no-run-if-empty --max-args=1 git submodule update --depth 1 --single-branch",
        stdin_str="\0".join(submodules_to_update) + "\0",
        timeout=120,
        retries=3,
        verbose=True,
    )
    res = res and Shell.check("git submodule foreach git reset --hard", verbose=True)
    res = res and Shell.check("git submodule foreach git checkout @ -f", verbose=True)
    res = res and Shell.check("git submodule foreach git clean -xfd", verbose=True)
    return res


def update_path_ch_config(config_file_path=""):
    print("Updating path in clickhouse config")
    config_file_path = (
        config_file_path or f"{Settings.TEMP_DIR}/etc/clickhouse-server/config.xml"
    )
    ssl_config_file_path = (
        f"{Settings.TEMP_DIR}/etc/clickhouse-server/config.d/ssl_certs.xml"
    )
    try:
        with open(config_file_path, "r", encoding="utf-8") as file:
            content = file.read()

        with open(ssl_config_file_path, "r", encoding="utf-8") as file:
            ssl_config_content = file.read()
        content = content.replace(">/var/", f">{Settings.TEMP_DIR}/var/")
        content = content.replace(">/etc/", f">{Settings.TEMP_DIR}/etc/")
        ssl_config_content = ssl_config_content.replace(
            ">/etc/", f">{Settings.TEMP_DIR}/etc/"
        )
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


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Fast Test Job")
    parser.add_argument("--param", help="Optional custom job start stage", default=None)
    return parser.parse_args()


def main():
    args = parse_args()
    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    current_directory = Utils.cwd()
    build_dir = f"{Settings.TEMP_DIR}/build"

    Utils.add_to_PATH(f"{build_dir}/programs:{current_directory}/tests")

    res = True
    results = []

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        Shell.check(f"rm -rf {build_dir} && mkdir -p {build_dir}")
        results.append(
            Result.from_commands_run(
                name="Checkout Submodules",
                command=clone_submodules,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CMAKE in stages:
        results.append(
            Result.from_commands_run(
                name="Cmake configuration",
                command=f"cmake {current_directory} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} \
                -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} \
                -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake \
                -DENABLE_LIBRARIES=0 \
                -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 -DENABLE_NURAFT=1 -DENABLE_SIMDJSON=1 \
                -DENABLE_JEMALLOC=1 -DENABLE_LIBURING=1 -DENABLE_YAML_CPP=1 -DCOMPILER_CACHE=sccache",
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        Shell.check("sccache --show-stats")
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command="ninja clickhouse-bundle clickhouse-stripped",
                workdir=build_dir,
                with_log=True,
            )
        )
        Shell.check("sccache --show-stats")
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        commands = [
            f"mkdir -p {Settings.OUTPUT_DIR}/binaries",
            f"cp ./programs/clickhouse {Settings.OUTPUT_DIR}/binaries/clickhouse",
            f"zstd --threads=0 --force programs/clickhouse-stripped -o {Settings.OUTPUT_DIR}/binaries/clickhouse-stripped.zst",
            "sccache --show-stats",
            "clickhouse-client --version",
            "clickhouse-test --help",
        ]
        results.append(
            Result.from_commands_run(
                name="Check and Compress binary",
                command=commands,
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CONFIG in stages:
        commands = [
            f"rm -rf {Settings.TEMP_DIR}/etc/ && mkdir -p {Settings.TEMP_DIR}/etc/clickhouse-client {Settings.TEMP_DIR}/etc/clickhouse-server",
            f"cp ./programs/server/config.xml ./programs/server/users.xml {Settings.TEMP_DIR}/etc/clickhouse-server/",
            f"./tests/config/install.sh {Settings.TEMP_DIR}/etc/clickhouse-server {Settings.TEMP_DIR}/etc/clickhouse-client --fast-test",
            # f"cp -a {current_directory}/programs/server/config.d/log_to_console.xml {Settings.TEMP_DIR}/etc/clickhouse-server/config.d/",
            f"rm -f {Settings.TEMP_DIR}/etc/clickhouse-server/config.d/secure_ports.xml",
            update_path_ch_config,
        ]
        results.append(
            Result.from_commands_run(
                name="Install ClickHouse Config",
                command=commands,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc(fast_test=True)
    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        res = CH.start()
        res = res and CH.wait_ready()
        results.append(
            Result.create_from(name=step_name, status=res, stopwatch=stop_watch_)
        )

    if res and JobStages.TEST in stages:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Tests"
        print(step_name)
        res = res and CH.run_fast_test()
        if res:
            results.append(FTResultsProcessor(wd=Settings.OUTPUT_DIR).run())
        results[-1].set_timing(stopwatch=stop_watch_)

    CH.terminate()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
