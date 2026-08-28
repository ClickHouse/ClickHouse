"""Builds the standalone WebAssembly SQL parser (`utils/wasm-parser`) and publishes it.

The module is not part of the server build - it is a CMake project of its own, cross-compiled to
`wasm32-wasip1` with a wasi-sdk toolchain that cannot be mixed into a tree configured for the host
- so nothing else in CI notices when it stops compiling, which it did within a day of being
merged, twice over: a source went missing from the list, and the `setjmp` boundary turned into an
uncaught WebAssembly exception under LTO. This job is what notices.

Everything runs in `clickhouse/wasm-builder` (wasi-sdk plus the Node.js of the base image). Each
configuration is configured, built, and then handed to CTest, which runs the two tests the CMake
project defines: `parser-cases` drives the module through `utils/wasm-parser/test.mjs`, and
`parser-size` asserts the byte ceiling for that configuration. Only the two extreme configurations
are built, because each build is a few hundred translation units of full LTO. The ceilings for
`-DENABLE_DCL=OFF` and `-DENABLE_FORMATTING=OFF` on their own are therefore not asserted here;
build them locally as described in `utils/wasm-parser/README.md`.

Both modules are published under the `CH_WASM_PARSER_BIN` artifact, which is what a consumer -
a browser, or a Node.js tool - downloads.
"""

import os
import shutil
from pathlib import Path

from ci.praktika.result import Result
from ci.praktika.utils import Shell, Utils

current_directory = Utils.cwd()
source_dir = f"{current_directory}/utils/wasm-parser"
build_dir = f"{current_directory}/ci/tmp/build"

# The two extreme configurations, named as in the size table of utils/wasm-parser/README.md:
# everything, and the smallest build the project offers. Each is built in its own tree under
# `build_dir` and its `parser.wasm` is then published from `build_dir` itself under the name
# given here, so that one artifact path names one configuration.
CONFIGURATIONS = (
    ("everything", [], "parser.wasm"),
    (
        "no-formatting-no-dcl",
        ["-DENABLE_FORMATTING=OFF", "-DENABLE_DCL=OFF"],
        "parser-no-formatting-no-dcl.wasm",
    ),
)

# Every contrib submodule the CMake project reaches (the rest of contrib it names is vendored
# in-tree). The CI workspace is checked out without submodule working trees - `needs_submodules`
# only restores the `.git/modules` metadata - so the tree handed to `cmake` would otherwise be
# missing these sources, and the very first `add_executable` would fail on them.
SUBMODULES = (
    "contrib/abseil-cpp",
    "contrib/boost",
    "contrib/cctz",
    "contrib/croaring",
    "contrib/double-conversion",
    "contrib/fast_float",
    "contrib/fmtlib",
    "contrib/libdivide",
    "contrib/magic_enum",
    "contrib/miniselect",
    "contrib/re2",
    "contrib/sparsehash-c11",
    "contrib/wyhash",
    "contrib/xxHash",
    "contrib/zmij",
)

# A no-op when the working trees are already there (a local run). The CI checkout is owned by
# another user while this job runs as `root` in the image, and `safe.directory` also guards every
# submodule worktree, so trust them all - for these commands only; `-c` reaches the spawned
# per-submodule processes. `--init` covers both the restored-cache path and a cold checkout.
GIT = "git -c safe.directory='*'"
CHECKOUT_SUBMODULES = (
    f"{GIT} submodule sync -- " + " ".join(SUBMODULES),
    f"{GIT} submodule update --init --depth 1 --single-branch --jobs 10 -- "
    + " ".join(SUBMODULES),
)


# The deleted `test_wasm_parser` fixture mounted the checkout read-only, so a build that wrote
# anything into the source tree failed on the spot. Praktika hands the job a writable checkout
# instead, so the contract is asserted afterwards: an out-of-tree build leaves the sources it
# reads exactly as it found them. Submodules are ignored - the checkout step above is what
# touched them.
def check_source_tree_is_clean():
    def check():
        dirty = Shell.get_output(
            f"{GIT} status --porcelain --untracked-files=all --ignore-submodules=all"
            " -- utils/wasm-parser src base"
        )
        if dirty:
            print(f"The build wrote into the source tree:\n{dirty}")
            return False
        return True

    return Result.from_commands_run(name="Source tree is clean", command=check)


def main():
    wasi_sdk = os.getenv("WASI_SDK")
    assert wasi_sdk, (
        "WASI_SDK is not set. This job runs in `clickhouse/wasm-builder`, which sets it; "
        "outside that image, point it at a wasi-sdk as utils/wasm-parser/README.md describes"
    )

    Path(build_dir).mkdir(parents=True, exist_ok=True)

    results = [
        Result.from_commands_run(
            name="Checkout Submodules",
            command=list(CHECKOUT_SUBMODULES),
            with_log=True,
            retries=3,
        )
    ]
    # Without the sources there is nothing to configure, and every configuration below would
    # fail the same way, on the same missing file.
    if not results[-1].is_ok():
        Result.create_from(results=results).complete_job()

    for name, options, artifact in CONFIGURATIONS:
        config_dir = f"{build_dir}/{name}"
        results.append(
            Result.from_commands_run(
                name=f"Build and test ({name})",
                command=[
                    f"rm -rf {config_dir}",
                    f"cmake -S {source_dir} -B {config_dir} -G Ninja"
                    f" -DCMAKE_TOOLCHAIN_FILE={wasi_sdk}/share/cmake/wasi-sdk-p1.cmake"
                    f" -DWASI_SDK_PREFIX={wasi_sdk}" + "".join(f" {o}" for o in options),
                    # Every source the parser needs is named in the CMake project, so a source
                    # that grows a new dependency shows up right here, as an undefined symbol.
                    f"cmake --build {config_dir}",
                    # `parser-cases` and `parser-size`; see the module docstring.
                    f"ctest --test-dir {config_dir} --output-on-failure",
                ],
                with_log=True,
            )
        )
        # A configuration that failed has nothing worth publishing, and the next one is still
        # worth building: a size ceiling is hit by one configuration at a time, and a report
        # naming only the first is a report that has to be regenerated to be read.
        if not results[-1].is_ok():
            continue
        published = f"{build_dir}/{artifact}"
        shutil.copy2(f"{config_dir}/parser.wasm", published)
        results[-1].set_info(f"{artifact}: {Path(published).stat().st_size} bytes")

    results.append(check_source_tree_is_clean())

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
