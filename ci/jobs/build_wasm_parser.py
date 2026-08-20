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
from ci.praktika.utils import Utils

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


def main():
    wasi_sdk = os.getenv("WASI_SDK")
    assert wasi_sdk, (
        "WASI_SDK is not set. This job runs in `clickhouse/wasm-builder`, which sets it; "
        "outside that image, point it at a wasi-sdk as utils/wasm-parser/README.md describes"
    )

    Path(build_dir).mkdir(parents=True, exist_ok=True)

    results = []
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

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
