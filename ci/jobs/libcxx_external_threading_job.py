import sys

from ci.defs.defs import ToolSet
from ci.jobs.fast_test import clone_submodules
from ci.praktika.result import Result
from ci.praktika.utils import Utils

repo_path = Utils.cwd()
build_dir = f"{repo_path}/ci/tmp/build_libcxx_external_threading"
sanitizers = {"release": "", "tsan": "thread", "asan": "address"}


def main():
    sanitizer = sanitizers[sys.argv[1]]
    stop_watch = Utils.Stopwatch()
    results = [
        Result.from_commands_run(
            name="Checkout Submodules",
            command=[
                clone_submodules,
                "git submodule update --init --depth 1 --single-branch -- contrib/silk contrib/librseq contrib/libucontext",
            ],
        )
    ]
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Cmake configuration",
                command=f"cmake -S {repo_path} -B {build_dir} -G Ninja"
                f" -DCMAKE_C_COMPILER={ToolSet.COMPILER_C} -DCMAKE_CXX_COMPILER={ToolSet.COMPILER_CPP}"
                " -DCOMPILER_CACHE=disabled -DENABLE_LIBRARIES=0 -DENABLE_TESTS=0 -DENABLE_UTILS=0"
                " -DENABLE_THINLTO=0 -DENABLE_RUST=0 -DENABLE_LIBURING=1 -DENABLE_SILK=1"
                f" -DENABLE_LIBCXX_EXTERNAL_THREADING_TESTS=1 -DSANITIZE={sanitizer}",
            )
        )
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Build",
                command=f"cmake --build {build_dir} --target libcxx_external_threading_tests",
                info_extractor=Result.cpp_build_info_extractor,
            )
        )
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Tests",
                command=f"ctest --test-dir {build_dir} -L libcxx_external_threading_tests"
                f" --timeout 300 --output-on-failure --parallel {Utils.cpu_count()}",
            )
        )
    Result.create_from(results=results, stopwatch=stop_watch).complete_job()


if __name__ == "__main__":
    main()
