import argparse
import os

from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.workflows.defs import CIFiles, ToolSet
from ci.workflows.pull_request import S3_BUILDS_BUCKET


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    UNSHALLOW = "unshallow"
    BUILD = "build"
    PACKAGE = "package"
    UNIT = "unit"


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
-DCMAKE_C_COMPILER={COMPILER} -DCMAKE_CXX_COMPILER={COMPILER_CPP} \
-DCOMPILER_CACHE={CACHE_TYPE} -DENABLE_BUILD_PROFILING=1 {DIR}"""

# release:          cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1 -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 ..
# binary release:   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 ..
# release coverage: cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 -DSANITIZE_COVERAGE=1 -DBUILD_STANDALONE_KEEPER=0 -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 ..


def main():
    args = parse_args()

    # # for sccache
    # os.environ["SCCACHE_BUCKET"] = S3_BUILDS_BUCKET
    # os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    # TODO: check with  SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1

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
    AUX_DEFS = " -DENABLE_TESTS=1 "
    cmake_cmd = None

    if "debug" in build_type:
        print("Build type set: debug")
        BUILD_TYPE = "Debug"
        AUX_DEFS = " -DENABLE_TESTS=0 "
        package_type = "debug"
    elif "release" in build_type:
        print("Build type set: release")
        AUX_DEFS = (
            " -DENABLE_TESTS=0 -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1 "
        )
        package_type = "release"
    elif "asan" in build_type:
        print("Sanitizer set: address")
        SANITIZER = "address"
        package_type = "asan"
    elif "tsan" in build_type:
        print("Sanitizer set: thread")
        SANITIZER = "thread"
        package_type = "tsan"
    elif "msan" in build_type:
        print("Sanitizer set: memory")
        SANITIZER = "memory"
        package_type = "msan"
    elif "ubsan" in build_type:
        print("Sanitizer set: undefined")
        SANITIZER = "undefined"
        package_type = "ubsan"
    elif "binary" in build_type:
        package_type = "binary"
        cmake_cmd = f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None -DSANITIZE= -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 {Utils.cwd()}"
    else:
        assert False

    if not cmake_cmd:
        cmake_cmd = CMAKE_CMD.format(
            BUILD_TYPE=BUILD_TYPE,
            CACHE_TYPE=CACHE_TYPE,
            SANITIZER=SANITIZER,
            AUX_DEFS=AUX_DEFS,
            DIR=Utils.cwd(),
            COMPILER=ToolSet.COMPILER_C,
            COMPILER_CPP=ToolSet.COMPILER_CPP,
        )

    build_dir = f"{Settings.TEMP_DIR}/build"

    res = True
    results = []
    version = ""

    if res and JobStages.UNSHALLOW in stages:
        results.append(
            Result.from_commands_run(
                name="Repo Unshallow",
                command="git rev-parse --is-shallow-repository | grep -q true && git fetch --depth 10000 --no-tags --filter=tree:0 origin $(git rev-parse --abbrev-ref HEAD)",
                with_log=True,
            )
        )
        res = results[-1].is_ok()
        if res:
            try:
                version = CHVersion.get_version()
                assert version
                print(f"Got version from repo [{version}]")
            except Exception as e:
                results[-1].set_failed().set_info(
                    f"Failed to get version from repo, ex [{e}]"
                )
                res = False

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        Shell.check(f"rm -rf {build_dir} && mkdir -p {build_dir}")
        results.append(
            Result.from_commands_run(
                name="Checkout Submodules",
                command=f"git submodule sync --recursive && git submodule init && git submodule update --depth 1 --recursive --jobs {min([Utils.cpu_count(), 20])}",
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.CMAKE in stages:
        results.append(
            Result.from_commands_run(
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
            Result.from_commands_run(
                name="Build ClickHouse",
                command="ninja clickhouse-bundle clickhouse-odbc-bridge clickhouse-library-bridge",
                workdir=build_dir,
                with_log=True,
            )
        )
        Shell.check("sccache --show-stats")
        Shell.check(f"ls -l {build_dir}/programs/")
        Shell.check(f"pwd")
        Shell.check(f"find {build_dir} -name unit_tests_dbms")
        Shell.check(f"find . -name unit_tests_dbms")
        res = results[-1].is_ok()

    if res and JobStages.PACKAGE in stages and "binary" not in build_type:
        assert package_type
        if "amd" in build_type:
            deb_arch = "amd64"
        else:
            deb_arch = "arm64"

        output_dir = "/tmp/praktika/output/"
        assert Shell.check(f"rm -f {output_dir}/*.deb")

        results.append(
            Result.from_commands_run(
                name="Build Packages",
                command=[
                    f"DESTDIR={build_dir}/root ninja programs/install",
                    f"ln -sf {build_dir}/root {Utils.cwd()}/packages/root",
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={output_dir} BUILD_TYPE={package_type} VERSION_STRING={version} DEB_ARCH={deb_arch} ./build --deb",
                ],
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.UNIT in stages and (SANITIZER or "binary" in build_type):
        # TODO: parallel execution
        results.append(
            Result.from_gtest_run(
                name="Unit Tests",
                unit_tests_path=CIFiles.UNIT_TESTS_BIN,
                with_log=False,
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
