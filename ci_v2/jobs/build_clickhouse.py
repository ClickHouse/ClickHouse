from ci_v2.settings.definitions import JobNames
from praktika.environment import Environment
from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import MetaClasses, Shell, Utils


class JobStages(metaclass=MetaClasses.WithIter):
    CHECKOUT_SUBMODULES = "checkout"
    CMAKE = "cmake"
    BUILD = "build"


def main():
    stop_watch = Utils.Stopwatch()

    stages = list(JobStages)
    stage = Environment.LOCAL_RUN_PARAM or JobStages.CHECKOUT_SUBMODULES
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    if Environment.JOB_NAME in (JobNames.BUILD_AMD_DEBUG,):
        cmake_build_type = "Debug"
        sanitizer = ""
    else:
        assert False

    if Environment.is_local_run():
        build_cache_type = "disabled"
    else:
        build_cache_type = "sccache"

    current_directory = Utils.cwd()
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
                command=f"cmake --debug-trycompile -DCMAKE_VERBOSE_MAKEFILE=1 -LA -DCMAKE_BUILD_TYPE={cmake_build_type} \
                 -DSANITIZE={sanitizer} -DENABLE_CHECK_HEAVY_BUILDS=1 -DENABLE_CLICKHOUSE_SELF_EXTRACTING=1 -DENABLE_TESTS=0 \
                 -DENABLE_UTILS=0 -DCMAKE_FIND_PACKAGE_NO_PACKAGE_REGISTRY=ON -DCMAKE_INSTALL_PREFIX=/usr \
                 -DCMAKE_INSTALL_SYSCONFDIR=/etc -DCMAKE_INSTALL_LOCALSTATEDIR=/var -DCMAKE_SKIP_INSTALL_ALL_DEPENDENCY=ON \
                 -DCMAKE_C_COMPILER=clang-18 -DCMAKE_CXX_COMPILER=clang++-18 -DCOMPILER_CACHE={build_cache_type} -DENABLE_TESTS=1 \
                 -DENABLE_BUILD_PROFILING=1 {current_directory}",
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
        res = results[-1].is_ok()

    Result.create_from(results=results, stopwatch=stop_watch).finish_job_accordingly()


if __name__ == "__main__":
    main()
