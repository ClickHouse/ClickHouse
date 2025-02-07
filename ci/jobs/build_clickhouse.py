import argparse
import os
import traceback
from pathlib import Path

from praktika.info import Info
from praktika.result import Result
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.jobs.scripts.log_cluster import LogClusterBuildProfileQueries
from ci.workflows.defs import BuildTypes, CIFiles, ToolSet
from ci.workflows.pull_request import S3_BUILDS_BUCKET

current_directory = Utils.cwd()
build_dir = f"{current_directory}/ci/tmp/build/"
temp_dir = f"{current_directory}/ci/tmp/"

BUILD_TYPE_TO_CMAKE = {
    Build(
        1
    ): f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    Build(
        BuildTypes.AMD_RELEASE
    ): f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.AMD_BINARY)": f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.AMD_ASAN)": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.AMD_TSAN)": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.AMD_MSAN)": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=memory    -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.AMD_UBSAN)": f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    f"Build (BuildTypes.ARM_RELEASE)": f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.ARM_ASAN})": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-aarch64.cmake -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_TESTS=1 -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_COVERAGE})": f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DSANITIZE_COVERAGE=1 -DBUILD_STANDALONE_KEEPER=0 -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.ARM_BINARY})": f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_TIDY})": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_CLANG_TIDY=1 -DENABLE_TESTS=1 -DENABLE_EXAMPLES=1 -DENABLE_UTILS=1 -DENABLE_RUST=0 -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_DARWIN})": f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/darwin/toolchain-x86_64.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.ARM_DARWIN})": f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/darwin/toolchain-x86_64.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.ARM_V80COMPAT})": f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-aarch64.cmake -DNO_ARMV81_OR_HIGHER=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_FREEBSD})": f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/freebsd/toolchain-x86_64.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.PPC64LE})": f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-ppc64le.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_COMPAT})": f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DNO_SSE3_OR_HIGHER=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.AMD_MUSL})": f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.RISCV64})": f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-riscv64.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.S390X})": f"        cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-s390x.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.LOONGARCH64})": f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-loongarch64.cmake -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1",
    f"Build ({BuildTypes.FUZZERS})": f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_FUZZING=1 -DENABLE_PROTOBUF=1 -DWITH_COVERAGE=1 -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_BUZZHOUSE=1 -DCLICKHOUSE_OFFICIAL_BUILD=1",
}

# TODO: for legacy packaging script - remove
BUILD_TYPE_TO_DEB_PACKAGE_TYPE = {
    "Build (AMD_DEBUG)": "debug",
    "Build (AMD_RELEASE)": "release",
    "Build (ARM_RELEASE)": "release",
    "Build (AMD_ASAN)": "asan",
    "Build (ARM_ASAN)": "asan",
    "Build (AMD_MSAN)": "msan",
    "Build (AMD_UBSAN)": "ubsan",
    "Build (AMD_TSAN)": "tsan",
    "Build (AMD_COVERAGE)": "release",
}

build_dir = f"{Utils.cwd()}/ci/tmp/build"


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    UNSHALLOW = "unshallow"
    BUILD = "build"
    PACKAGE = "package"
    UNIT = "unit"
    UPLOAD_PROFILE_DATA = "profile"


def parse_args():
    parser = argparse.ArgumentParser(description="ClickHouse Build Job")
    parser.add_argument(
        "--build-type",
        help="see BuildTypes.*",
    )
    parser.add_argument(
        "--param",
        help="Optional user-defined job start stage (for local run)",
        default=None,
    )
    return parser.parse_args()


def main():
    args = parse_args()

    # # for sccache
    os.environ["SCCACHE_BUCKET"] = S3_BUILDS_BUCKET
    os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"

    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    build_type = args.build_type.lower()
    assert (
        build_type
    ), "--build-type must be provided either as input argument or as a parameter of parametrized job in CI"
    assert (
        build_type in BUILD_TYPE_TO_CMAKE
    ), f"--build_type option is invalid [{build_type}]"

    cmake_cmd = BUILD_TYPE_TO_CMAKE[build_type] + " " + current_directory

    res = True
    results = []
    version = CHVersion.get_version()

    # if res and JobStages.UNSHALLOW in stages:
    #     results.append(
    #         Result.from_commands_run(
    #             name="Repo Unshallow",
    #             command="git rev-parse --is-shallow-repository | grep -q true && git fetch --depth 10000 --no-tags --filter=tree:0 origin $(git rev-parse --abbrev-ref HEAD) ||:",
    #             with_log=True,
    #         )
    #     )
    #     res = results[-1].is_ok()
    #     if res:
    #         try:
    #             version = CHVersion().get_version()
    #             assert version
    #             print(f"Got version from repo [{version}]")
    #         except Exception as e:
    #             results[-1].set_failed().set_info(
    #                 f"Failed to get version from repo, ex [{e}]"
    #             )
    #             res = False

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
        CHVersion.set_build_version()
        if "darwin" in build_type:
            Shell.check(
                f"rm -rf {current_directory}/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-aarch64"
            )
            Shell.check(
                f"ln -sf /build/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-x86_64"
            )
            Shell.check(
                f"ln -sf /build/cmake/toolchain/darwin-x86_64 {current_directory}/cmake/toolchain/darwin-aarch64"
            )
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
        if build_type in BUILD_TYPE_TO_DEB_PACKAGE_TYPE:
            targets = (
                "clickhouse-bundle clickhouse-odbc-bridge clickhouse-library-bridge"
            )
        elif build_type == BuildTypes.FUZZERS:
            targets = "fuzzers"
        else:
            targets = "clickhouse-bundle"
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"ninja {targets}",
                workdir=build_dir,
                with_log=True,
            )
        )
        Shell.check("sccache --show-stats")
        Shell.check(f"ls -l {build_dir}/programs/")
        Shell.check(f"pwd")
        res = results[-1].is_ok()

    if (
        res
        and JobStages.PACKAGE in stages
        and build_type in BUILD_TYPE_TO_DEB_PACKAGE_TYPE
    ):
        if "amd" in build_type:
            deb_arch = "amd64"
        else:
            deb_arch = "arm64"

        assert Shell.check(f"rm -f {temp_dir}/*.deb")

        results.append(
            Result.from_commands_run(
                name="Build Packages",
                command=[
                    f"DESTDIR={build_dir}/root ninja programs/install",
                    f"ln -sf {build_dir}/root {Utils.cwd()}/packages/root",
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={temp_dir} BUILD_TYPE={BUILD_TYPE_TO_DEB_PACKAGE_TYPE[build_type]} VERSION_STRING={version} DEB_ARCH={deb_arch} ./build --deb {'--rpm --tgz' if 'release' in build_type else ''}",
                ],
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if (
        res
        and JobStages.UNIT in stages
        and ("binary" in build_type or "san" in build_type)
        and "amd" in build_type  # until arm unit tests are fixed
    ):
        # TODO: parallel execution
        results.append(
            Result.from_gtest_run(
                name="Unit Tests",
                unit_tests_path=CIFiles.UNIT_TESTS_BIN,
                with_log=False,
            )
        )
        if not results[-1].is_ok():
            results[-1].set_files(CIFiles.UNIT_TESTS_BIN)

        res = results[-1].is_ok()

    # TODO:
    # profile_result = None
    # if (
    #     res and JobStages.UPLOAD_PROFILE_DATA in stages
    #     and "release" in build_type
    #     and not Info().is_local_run
    # ):
    #     sw_ = Utils.Stopwatch()
    #     print("Prepare build profile data")
    #     profiles_dir = Path(temp_dir) / "profiles_source"
    #     profiles_dir.mkdir(parents=True, exist_ok=True)
    #     is_success = True
    #     try:
    #         Shell.check(
    #             "./utils/prepare-time-trace/prepare-time-trace.sh "
    #             f"{build_dir} {profiles_dir.absolute()}",
    #             strict=True,
    #             verbose=True,
    #         )
    #         profile_data_file = Path(temp_dir) / "profile.json"
    #         with open(profile_data_file, "wb") as profile_fd:
    #             for profile_source in profiles_dir.iterdir():
    #                 if profile_source.name not in (
    #                     "binary_sizes.txt",
    #                     "binary_symbols.txt",
    #                 ):
    #                     with open(profiles_dir / profile_source, "rb") as ps_fd:
    #                         profile_fd.write(ps_fd.read())
    #         LogClusterBuildProfileQueries().insert_profile_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profile_data_file,
    #         )
    #         LogClusterBuildProfileQueries().insert_build_size_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profiles_dir / "binary_sizes.txt",
    #         )
    #         LogClusterBuildProfileQueries().insert_binary_symbol_data(
    #             build_name=build_type,
    #             start_time=stop_watch.start_time,
    #             file=profiles_dir / "binary_symbols.txt",
    #         )
    #     except Exception as e:
    #         is_success = False
    #         traceback.print_exc()
    #         print(f"ERROR: Failed to upload build profile data. ex: [{e}]")
    #     profile_result = Result.create_from(
    #         name="Build Profile", status=is_success, stopwatch=sw_
    #     )
    #
    R = Result.create_from(results=results, stopwatch=stop_watch)
    # if profile_result:
    #     # append profile sub result after R is created to not affect overall job status
    #     R.results.append(profile_result)
    R.complete_job()


if __name__ == "__main__":
    main()
