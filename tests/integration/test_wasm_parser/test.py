"""Builds the standalone WebAssembly SQL parser (`utils/wasm-parser`) and drives it.

The stateless suite has neither a WebAssembly toolchain nor a JavaScript runtime, and the
module is not part of the server build, so nothing else notices when it stops compiling -
which it did within a day of being merged, twice over: a source went missing from the list,
and the `setjmp` boundary turned into an uncaught WebAssembly exception under LTO.

Everything here runs inside `clickhouse/wasm-builder` (wasi-sdk plus the Node.js of the base
image), with the ClickHouse tree bind-mounted read-only. Each configuration is configured,
built, and then handed to CTest, which runs the two tests the CMake project defines:
`parser-cases` drives the module through `utils/wasm-parser/test.mjs`, and `parser-size`
asserts the byte ceiling for that configuration.

Only the two extreme configurations are built, because each build is a few hundred
translation units of full LTO. The ceilings for `-DENABLE_DCL=OFF` and
`-DENABLE_FORMATTING=OFF` on their own are therefore not asserted here; build them locally as
described in `utils/wasm-parser/README.md`.
"""

import os

import docker
import pytest

from helpers.cluster import ClickHouseCluster, get_docker_compose_path, run_and_check

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CLICKHOUSE_DIR = os.path.abspath(os.path.join(SCRIPT_DIR, "..", "..", ".."))
DOCKER_COMPOSE_PATH = get_docker_compose_path()

cluster = ClickHouseCluster(__file__)

CONFIGURATIONS = [
    pytest.param([], id="everything"),
    pytest.param(
        ["-DENABLE_FORMATTING=OFF", "-DENABLE_DCL=OFF"], id="no-formatting-no-dcl"
    ),
]

# Every contrib submodule the CMake project reaches (the rest of contrib it names is vendored
# in-tree). The CI workspace is checked out without submodules - only the build jobs fetch
# them - so the tree handed to the builder would otherwise be missing these sources.
SUBMODULES = [
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
]


@pytest.fixture(scope="module")
def builder():
    # A no-op when the submodules are already there (a local run). The CI checkout is owned
    # by another user, and `safe.directory` also guards every submodule worktree, so trust
    # them all - for this command only; `-c` reaches the spawned per-submodule processes.
    run_and_check(
        [
            "git",
            "-c",
            "safe.directory=*",
            "-C",
            CLICKHOUSE_DIR,
            "submodule",
            "update",
            "--init",
            "--depth",
            "1",
            "--single-branch",
            "--jobs",
            "10",
            "--",
        ]
        + SUBMODULES,
        timeout=600,
    )

    compose = os.path.join(DOCKER_COMPOSE_PATH, "docker_compose_wasm_builder.yml")
    os.environ["WASM_BUILDER_SOURCE_DIR"] = CLICKHOUSE_DIR
    run_and_check(
        cluster.compose_cmd(
            "-f", compose, "up", "--force-recreate", "-d", "--no-build"
        )
    )
    try:
        yield docker.DockerClient(
            base_url="unix:///var/run/docker.sock",
            version=cluster.docker_api_version,
            # One LTO link of the whole parser, and the pull that precedes it.
            timeout=3600,
        ).containers.get(cluster.get_instance_docker_id("wasmbuilder"))
    finally:
        run_and_check(
            cluster.compose_cmd("-f", compose, "down", "--volumes"), nothrow=True
        )


def run(builder, *argv):
    code, (stdout, stderr) = builder.exec_run(list(argv), demux=True)
    out = (stdout or b"").decode(errors="replace")
    err = (stderr or b"").decode(errors="replace")
    assert code == 0, "`{}` exited with {}:\n{}\n{}".format(
        " ".join(argv), code, out, err
    )
    return out


@pytest.mark.parametrize("options", CONFIGURATIONS)
def test_wasm_parser(builder, options, request):
    build_dir = "/build/" + request.node.callspec.id
    wasi_sdk = "/opt/wasi-sdk"

    run(
        builder,
        "cmake",
        "-S",
        "/ClickHouse/utils/wasm-parser",
        "-B",
        build_dir,
        "-G",
        "Ninja",
        "-DCMAKE_TOOLCHAIN_FILE={}/share/cmake/wasi-sdk-p1.cmake".format(wasi_sdk),
        "-DWASI_SDK_PREFIX={}".format(wasi_sdk),
        *options,
    )

    # Every source the parser needs is named in the CMake project, so a source that grows a new
    # dependency shows up right here, as an undefined symbol at link time.
    run(builder, "cmake", "--build", build_dir)

    # `parser-cases` and `parser-size`; see the module docstring.
    output = run(builder, "ctest", "--test-dir", build_dir, "--output-on-failure")
    assert "100% tests passed" in output, output
