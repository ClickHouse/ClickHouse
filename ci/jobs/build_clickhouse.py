import argparse

from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    BUILD = "build"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument(
        "--build-type",
        help="Type: <amd|arm>,<debug|release>,<asan|msan|..>",
    )
    parser.add_argument(
        "--param",
        help="Optional user-defined job start stage (for local run)",
        default=None,
    )
    return parser.parse_args()


CMAKE_CMD = """cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA \
-DCMAKE_BUILD_TYPE={BUILD_TYPE} \
-DSANITIZE={SANITIZER} \
-DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 \
-DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr \
-DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON \
{AUX_DEFS} \
-DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 \
-DCOMPILER_CACHE={CACHE_TYPE} \
-DENABLE_BUILD_PROFILING=1 {DIR}"""


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

    build_type = args.build_type
    assert (
        build_type
    ), "build_type must be provided either as input argument or as a parameter of parametrized job in CI"
    build_type = build_type.lower()

    CACHE_TYPE = "sccache"

    BUILD_TYPE = "RelWithDebInfo"
    SANITIZER = ""
    AUX_DEFS = " -DENABLE_TESTS=0 "

    if "debug" in build_type:
        print("Build type set: debug")
        BUILD_TYPE = "Debug"
        AUX_DEFS = " -DENABLE_TESTS=1 "
    elif "release" in build_type:
        print("Build type set: release")
        AUX_DEFS = (
            " -DENABLE_TESTS=0 -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1 "
        )
    elif "asan" in build_type:
        print("Sanitizer set: address")
        SANITIZER = "address"
    else:
        assert False

    cmake_cmd = CMAKE_CMD.format(
        BUILD_TYPE=BUILD_TYPE,
        CACHE_TYPE=CACHE_TYPE,
        SANITIZER=SANITIZER,
        AUX_DEFS=AUX_DEFS,
        DIR=Utils.cwd(),
    )

    build_dir = f"{Settings.TEMP_DIR}/build"

    res = True
    results = []

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        Shell.check(f"rm -rf {build_dir} && mkdir -p {build_dir}")
        results.append(
            Result.create_from_command_execution(
                name="Checkout Submodules",
                command=f"git submodule sync --recursive && git submodule init && git submodule update --depth 1 --recursive --jobs {min([Utils.cpu_count(), 20])}",
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CMAKE in stages:
        results.append(
            Result.create_from_command_execution(
                name="Cmake configuration",
                command=cmake_cmd,
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        Shell.check("sccache --show-stats")
        results.append(
            Result.create_from_command_execution(
                name="Build ClickHouse",
                command="ninja clickhouse-bundle clickhouse-odbc-bridge clickhouse-library-bridge",
                workdir=build_dir,
                with_log=True,
            )
        )
        Shell.check("sccache --show-stats")
        Shell.check(f"ls -l {build_dir}/programs/")
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
