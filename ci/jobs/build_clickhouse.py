import argparse
from typing import List, Tuple

from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils

from ci.jobs.scripts.git_helper import Git, unshallow_cmd
from ci.jobs.scripts.version_helper import (
    ClickHouseVersion,
    get_version_from_repo,
    update_cmake_version,
)
from ci.settings import settings
from ci.workflows.defs import CIFiles, ToolSet


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    UNSHALLOW = "unshallow"
    BUILD = "build"
    PACKAGE = "package"
    UNIT = "unit"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="ClickHouse Build Job",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--build-type",
        help="Type: <binary|package>_<amd|arm>_<debug|release|asan|msan|..>",
    )
    parser.add_argument(
        "--param",
        help="Optional user-defined job start stage (for local run)",
        default=JobStages.CHECKOUT_SUBMODULES,
        choices=JobStages,
    )
    return parser.parse_args()


CMAKE_BASE = (
    "cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1"
    " -LA -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_BUILD_PROFILING=1"
    f" -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} "
    f"-DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP}"
)

SANITIZERS = {
    "asan": "address",
    "msan": "memory",
    "tsan": "thread",
    "ubsan": "undefined",
}


def build_args(build_type: List[str]) -> Tuple[str, str]:
    cmake = CMAKE_BASE
    ninja = "ninja"
    build_target = "clickhouse-bundle"
    if "package" in build_type:
        # Packaging
        cmake = (
            f"{cmake} -DCMAKE_INSTALL_PREFIX=/usr"
            " -DCMAKE_INSTALL_SYSCONFDIR=/etc"
            " -DCMAKE_INSTALL_LOCALSTATEDIR=/var"
            # Reduce linking and building time by avoid *install/all dependencies
            " -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON"
        )
        build_target = (
            f"{build_target} clickhouse-odbc-bridge clickhouse-library-bridge"
        )

    if all(arg in build_type for arg in ["package", "release"]):
        cmake = f"{cmake} -DSPLIT_DEBUG_SYMBOLS=ON -DBUILD_STANDALONE_KEEPER=1"

    if "fuzzers" in build_type:
        cmake = (
            f"{cmake} -DENABLE_FUZZING=1"
            " -DENABLE_PROTOBUF=1"
            " -DWITH_COVERAGE=1"
            # Reduce linking and building time by avoid *install/all dependencies
            " -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON"
        )

    if "coverage" in build_type:
        cmake = f"{cmake} -DSANITIZE_COVERAGE=1 -DBUILD_STANDALONE_KEEPER=0"

    try:
        sanitizer = next(san for san in SANITIZERS if san in build_type)
    except StopIteration:
        sanitizer = ""

    if sanitizer:
        cmake = f"{cmake} -DENABLE_TESTS=1 -DSANITIZE={SANITIZERS.get(sanitizer, '')}"

    if "debug" in build_type:
        cmake = f"{cmake} -DCMAKE_BUILD_TYPE=Debug"
    else:
        cmake = f"{cmake} -DCMAKE_BUILD_TYPE=None"

    cmake = f"{cmake} -DCOMPILER_CACHE=sccache"

    prefix = (
        f"SCCACHE_BUCKET={settings.S3_BUCKET_NAME} "
        "SCCACHE_S3_KEY_PREFIX=ccache/sccache SCCACHE_S3_NO_CREDENTIALS=true "
    )

    if "clang_tidy" in build_type:
        cmake = f"{cmake} -DENABLE_CLANG_TIDY=1 -DENABLE_TESTS=1 -DENABLE_EXAMPLES=1"
        ninja = f"{ninja} -k0"
        build_target = "all"
        prefix = (
            f"{prefix} CTCACHE_S3_FOLDER=ccache/sccache "
            f"CTCACHE_S3_BUCKET={settings.S3_BUCKET_NAME} "
            "CTCACHE_S3_NO_CREDENTIALS=true"
        )

    ninja = f"{ninja} {build_target}"

    return f"{prefix} {cmake} {Utils.cwd()}", f"{prefix} {ninja}"


def main():
    args = parse_args()

    # # for sccache
    # os.environ["SCCACHE_BUCKET"] = S3_BUILDS_BUCKET
    # os.environ["SCCACHE_S3_KEY_PREFIX"] = "ccache/sccache"
    # TODO: check with  SCCACHE_LOG=debug SCCACHE_NO_DAEMON=1

    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = args.param
    print(f"Job will start from stage [{stage}]")
    while stage in stages:
        stages.pop(0)
    stages.insert(0, stage)

    build_type = args.build_type.lower().split("_")
    assert (
        build_type
    ), "build_type must be provided either as input argument or as a parameter of parametrized job in CI"

    for package in ("debug", "release", "binary", *SANITIZERS):
        if package in build_type:
            package_type = package
            break
    else:
        assert False

    cmake_cmd, ninja_cmd = build_args(build_type)

    build_dir = f"{Settings.TEMP_DIR}/build"

    res = True
    results = []
    version = get_version_from_repo()

    if res and JobStages.UNSHALLOW in stages:
        results.append(
            Result.from_commands_run(
                name="Repo Unshallow",
                command=unshallow_cmd,
                with_log=True,
            )
        )
        res = results[-1].is_ok()
        if res:
            try:
                version = get_version_from_repo(git=Git())
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
                # TODO: think of agressive parallel update, it should use async run
                command=[
                    "git submodule sync --recursive",
                    "git submodule init",
                    f"git submodule update --depth 1 --recursive --jobs {min([Utils.cpu_count(), 20])}",
                ],
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
        Shell.check("SCCACHE_NO_DAEMON=1 sccache --show-stats")
        results.append(
            Result.from_commands_run(
                name="Build ClickHouse",
                command=ninja_cmd,
                workdir=build_dir,
                with_log=True,
            )
        )
        Shell.check("SCCACHE_NO_DAEMON=1 sccache --show-stats")
        Shell.check(f"ls -l {build_dir}/programs/")
        Shell.check("pwd")
        Shell.check(f"find {build_dir} -name unit_tests_dbms")
        Shell.check("find . -name unit_tests_dbms")
        res = results[-1].is_ok()

    if res and JobStages.PACKAGE in stages and "binary" not in build_type:
        assert package_type
        deb_arch = "amd64" if "amd" in build_type else "arm64"

        output_dir = "/tmp/praktika/output/"
        assert Shell.check(f"rm -f {output_dir}/*.deb")

        results.append(
            Result.from_commands_run(
                name="Build Packages",
                command=[
                    f"DESTDIR={build_dir}/root ninja programs/install",
                    f"ln -sf {build_dir}/root {Utils.cwd()}/packages/root",
                    f"cd {Utils.cwd()}/packages/ && OUTPUT_DIR={output_dir} "
                    f"BUILD_TYPE={package_type} VERSION_STRING={version} DEB_ARCH={deb_arch} ./build --deb",
                ],
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if (
        res
        and JobStages.UNIT in stages
        and any(build in build_type for build in (*SANITIZERS, "binary"))
    ):
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
