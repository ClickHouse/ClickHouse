import argparse
import os

from ci.defs.defs import BuildTypes, ToolSet
from ci.jobs.scripts.clickhouse_version import CHVersion
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.settings import Settings
from ci.praktika.utils import MetaClasses, Shell, Utils

current_directory = Utils.cwd()
build_dir = f"{current_directory}/ci/tmp/build"
temp_dir = f"{current_directory}/ci/tmp"

BUILD_TYPE_TO_CMAKE = {
    BuildTypes.AMD_DEBUG: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_XRAY=1",
    BuildTypes.AMD_RELEASE: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1",
    BuildTypes.AMD_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_ASAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.AMD_TSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=thread    -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.AMD_MSAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=memory    -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.AMD_UBSAN: f"    cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=undefined -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON",
    BuildTypes.ARM_RELEASE: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1",
    BuildTypes.ARM_ASAN: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-aarch64.cmake",
    BuildTypes.ARM_COVERAGE: f" cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DSANITIZE_COVERAGE=1",
    BuildTypes.ARM_BINARY: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON",
    BuildTypes.AMD_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.ARM_TIDY: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=Debug -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=1 -DENABLE_LEXER_TEST=1 -DENABLE_UTILS=1 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_CLANG_TIDY=1 -DENABLE_EXAMPLES=1 -DENABLE_BUZZHOUSE=1",
    BuildTypes.AMD_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/darwin/toolchain-x86_64.cmake -DCMAKE_AR:FILEPATH=/cctools/bin/x86_64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/x86_64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/x86_64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/x86_64-apple-darwin-ld",
    BuildTypes.ARM_DARWIN: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/darwin/toolchain-aarch64.cmake -DCMAKE_AR:FILEPATH=/cctools/bin/aarch64-apple-darwin-ar -DCMAKE_INSTALL_NAME_TOOL=/cctools/bin/aarch64-apple-darwin-install_name_tool -DCMAKE_RANLIB:FILEPATH=/cctools/bin/aarch64-apple-darwin-ranlib -DLINKER_NAME=/cctools/bin/aarch64-apple-darwin-ld",
    BuildTypes.ARM_V80COMPAT: f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-aarch64.cmake -DNO_ARMV81_OR_HIGHER=1",
    BuildTypes.AMD_FREEBSD: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/freebsd/toolchain-x86_64.cmake",
    BuildTypes.PPC64LE: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-ppc64le.cmake",
    BuildTypes.AMD_COMPAT: f"   cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DNO_SSE3_OR_HIGHER=1",
    BuildTypes.AMD_MUSL: f"     cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake",
    BuildTypes.RISCV64: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-riscv64.cmake",
    BuildTypes.S390X: f"        cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=1 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DENABLE_TESTS=0 -DENABLE_LEXER_TEST=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-s390x.cmake",
    BuildTypes.LOONGARCH64: f"  cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=          -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=1 -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-loongarch64.cmake -DENABLE_BUZZHOUSE=1",
    BuildTypes.FUZZERS: f"      cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE=None  -DENABLE_THINLTO=0 -DSANITIZE=address   -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP} -DCOMPILER_CACHE=sccache -DENABLE_BUILD_PROFILING=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DENABLE_FUZZING=1 -DENABLE_PROTOBUF=1 -DWITH_COVERAGE=1 -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON -DENABLE_BUZZHOUSE=1",
}

# TODO: for legacy packaging script - remove
BUILD_TYPE_TO_DEB_PACKAGE_TYPE = {
    BuildTypes.AMD_DEBUG: "debug",
    BuildTypes.AMD_RELEASE: "release",
    BuildTypes.ARM_RELEASE: "release",
    BuildTypes.AMD_ASAN: "asan",
    BuildTypes.ARM_ASAN: "asan",
    BuildTypes.AMD_MSAN: "msan",
    BuildTypes.AMD_UBSAN: "ubsan",
    BuildTypes.AMD_TSAN: "tsan",
    BuildTypes.ARM_COVERAGE: "release",
}


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


def run_shell(name, command, **kwargs):
    print(f"\n>>>> {name}\n")
    Shell.check(command, **kwargs)
    print(f"\n<<<< {name}\n")


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

    build_type = args.build_type.lower()
    assert (
        build_type
    ), "--build-type must be provided either as input argument or as a parameter of parametrized job in CI"
    assert (
        build_type in BUILD_TYPE_TO_CMAKE
    ), f"--build_type option is invalid [{build_type}]"

    cmake_cmd = BUILD_TYPE_TO_CMAKE[build_type]
    info = Info()
    if not info.is_local_run:
        # Default timeout (10min), can be too low, we run this in docker
        # anyway, will be terminated once the build is finished
        os.environ["SCCACHE_IDLE_TIMEOUT"] = "7200"
        os.environ["SCCACHE_BUCKET"] = Settings.S3_ARTIFACT_PATH
        os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
        os.environ["CTCACHE_DIR"] = f"{build_dir}/ccache/clang-tidy-cache"
        os.environ["CTCACHE_S3_BUCKET"] = Settings.S3_ARTIFACT_PATH
        os.environ["CTCACHE_S3_FOLDER"] = "ccache/clang-tidy-cache"
    if info.pr_number == 0:
        cmake_cmd += " -DCLICKHOUSE_OFFICIAL_BUILD=1"
    cmake_cmd += f" {current_directory}"

    res = True
    results = []

    if res and JobStages.CHECKOUT_SUBMODULES in stages:
        Shell.check(f"mkdir -p {build_dir}")
        results.append(
            Result.from_commands_run(
                name="Checkout Submodules",
                command=f"git submodule sync --recursive && git submodule init && git submodule update --depth 1 --recursive --jobs {min([Utils.cpu_count(), 20])}",
                retries=3,
            )
        )
        res = results[-1].is_ok()

    if info.pr_number == 0 and info.is_push_event:
        version_dict = info.get_custom_data("version")
    else:
        version_dict = CHVersion.get_current_version_as_dict()

    if res and JobStages.CMAKE in stages:
        assert version_dict, "Failed to determine build version"
        CHVersion.set_binary_version(version_dict=version_dict)
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
            )
        )
        res = results[-1].is_ok()

    if res and JobStages.BUILD in stages:
        run_shell("sccache stats", "sccache --show-stats")
        if build_type in BUILD_TYPE_TO_DEB_PACKAGE_TYPE:
            targets = "clickhouse-bundle"
        elif build_type == BuildTypes.FUZZERS:
            targets = "fuzzers"
        elif build_type == BuildTypes.AMD_DEBUG:
            targets = "-k0 all"
        elif build_type in (BuildTypes.AMD_TIDY, BuildTypes.ARM_TIDY):
            targets = "-k0 all"
            run_shell("clang-tidy-cache stats", "clang-tidy-cache --show-stats")
        else:
            targets = "clickhouse-bundle"
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=f"ninja {targets}",
                workdir=build_dir,
            )
        )
        run_shell("sccache stats", "sccache --show-stats")
        if build_type in (BuildTypes.AMD_TIDY, BuildTypes.ARM_TIDY):
            run_shell("clang-tidy-cache stats", "clang-tidy-cache --show-stats")
        run_shell("Output programs", f"ls -l {build_dir}/programs/", verbose=True)
        Shell.check("pwd")
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
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={temp_dir} BUILD_TYPE={BUILD_TYPE_TO_DEB_PACKAGE_TYPE[build_type]} VERSION_STRING={version_dict['string']} DEB_ARCH={deb_arch} ./build --deb {'--rpm --tgz' if 'release' in build_type else ''}",
                ],
                workdir=build_dir,
            )
        )
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
