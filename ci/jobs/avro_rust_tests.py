"""Runs the Rust regression suite of the Avro decoder (`contrib/avro-rust-cmake`).

The server build links only the `staticlib` of the crate and the unit test job runs only
`unit_tests_dbms`, so nothing else executes the crate's own tests: the input-cut and continuation
checks (`streaming_input.rs`), the reservation-work bounds (`need_space_work.rs`), the
uninitialized-output checks (`uninit_output.rs`), the container-file corpus (`corpus.rs`), the
random-schema comparison with `fastavro` (`random.rs`) and the C ABI smoke test (`c_abi.rs`).

Several of these tests read fixtures that `tests/gen_fixtures.py` and `tests/gen_random.py` generate
with `fastavro`, and return successfully without testing anything when the fixtures are missing.
The job therefore generates them and sets `AVRO_REQUIRE_FIXTURES=1`, which turns a missing fixture
(or a missing C compiler for `c_abi.rs`) into a failure.

The tests run in the `dev` profile (overflow checks and debug assertions on, as in the Debug builds)
and in `release` (as the optimized builds are compiled). The `#[ignore]`d exhaustive test is not run.
Offline, with the vendored crates of `contrib/rust_vendor`, the same way the server build resolves them.
Cargo loads the whole `contrib/rust-contrib-workspace`, so the submodules behind its other members' path dependencies
(`contrib/pcodec`, `contrib/wasmtime`) are checked out too.

Miri is not part of this job.
"""

import os
import shlex
import shutil
from pathlib import Path

from ci.defs.defs import ToolSet
from ci.praktika.result import Result
from ci.praktika.utils import Utils

current_directory = Utils.cwd()
temp_dir = f"{current_directory}/ci/tmp"
crate_dir = f"{current_directory}/contrib/avro-rust-cmake"
fixtures_dir = f"{temp_dir}/avro_fixtures"
python_deps_dir = f"{temp_dir}/avro_python_deps"
cargo_home = f"{temp_dir}/avro_cargo_home"
cargo_target_dir = f"{temp_dir}/avro_cargo_target"

# The CI checkout is owned by another user while this job runs as `root` in the image, and
# `safe.directory` also guards every submodule worktree. A no-op when the working tree is already
# there (a local run).
GIT = "git -c safe.directory='*'"

# The cargo configuration that `contrib/corrosion-cmake/config.toml.in` writes for the server
# build, reduced to what resolving the workspace needs: the vendored crates in place of crates.io.
# It lives in `CARGO_HOME` rather than in `contrib/rust-contrib-workspace/.cargo`, so that it leaves the checkout
# alone and still applies to the nested `cargo build` of `c_abi.rs`, which `--config` would not reach.
CARGO_CONFIG = f"""\
[source.vendored-sources]
directory = "{current_directory}/contrib/rust_vendor"

[source.crates-io]
replace-with = "vendored-sources"
"""


def write_cargo_config():
    Path(cargo_home).mkdir(parents=True, exist_ok=True)
    Path(cargo_home, "config.toml").write_text(CARGO_CONFIG)
    return True


def remove_fixtures():
    # A Python callable rather than `rm -rf` in a shell: the checkout path is not shell-safe, and an
    # error other than a missing directory must fail the step instead of being ignored.
    if os.path.lexists(fixtures_dir):
        shutil.rmtree(fixtures_dir)
    return True


def cargo_test(profile_args):
    manifest_path = shlex.quote(f"{current_directory}/contrib/rust-contrib-workspace/Cargo.toml")
    return f"cargo test -p _ch_rust_avro --offline --locked {profile_args} --manifest-path {manifest_path}"


def main():
    Path(temp_dir).mkdir(parents=True, exist_ok=True)

    # Every child process inherits these.
    os.environ["CARGO_HOME"] = cargo_home
    os.environ["CARGO_TARGET_DIR"] = cargo_target_dir
    os.environ["CARGO_TERM_COLOR"] = "never"
    # The image has only the versioned clang, no `cc`: cargo links build scripts and test binaries with
    # it, through lld as the server build does, and `c_abi.rs` compiles its C and C++ programs with it.
    os.environ["CC"] = ToolSet.COMPILER_C
    os.environ["CXX"] = ToolSet.COMPILER_CPP
    os.environ["RUSTFLAGS"] = f"-C linker={ToolSet.COMPILER_C} -C link-arg=-fuse-ld=lld"
    os.environ["AVRO_FIXTURES_DIR"] = fixtures_dir
    os.environ["AVRO_REQUIRE_FIXTURES"] = "1"
    os.environ["PYTHONPATH"] = (python_deps_dir + os.pathsep + os.environ.get("PYTHONPATH", "")).rstrip(os.pathsep)

    results = [
        Result.from_commands_run(
            name="Checkout Vendored Crates",
            command=[
                # Cargo loads every member of `contrib/rust-contrib-workspace`, so the submodules behind the path
                # dependencies of the other members (`pco`, `wasmtime`) must be present as well.
                f"{GIT} submodule sync -- contrib/rust_vendor contrib/pcodec contrib/wasmtime",
                f"{GIT} submodule update --init --depth 1 --single-branch -- contrib/rust_vendor contrib/pcodec contrib/wasmtime",
            ],
            with_log=True,
            retries=3,
        )
    ]
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Install Fixture Generator",
                command=(
                    f"python3 -m pip install --no-cache-dir --target {shlex.quote(python_deps_dir)} "
                    f"-r {shlex.quote(f'{crate_dir}/tests/requirements.txt')}"
                ),
                with_log=True,
                retries=3,
            )
        )
    if results[-1].is_ok():
        results.append(
            Result.from_commands_run(
                name="Generate Fixtures",
                command=[
                    remove_fixtures,
                    f"python3 {shlex.quote(f'{crate_dir}/tests/gen_fixtures.py')} {shlex.quote(current_directory)} {shlex.quote(fixtures_dir)}",
                    f"python3 {shlex.quote(f'{crate_dir}/tests/gen_random.py')} {shlex.quote(fixtures_dir)}",
                ],
                with_log=True,
            )
        )
    if results[-1].is_ok():
        results.append(Result.from_commands_run(name="Configure Cargo", command=write_cargo_config))
    # The two profiles are independent, so a failure in one does not hide the other.
    if results[-1].is_ok():
        for name, profile_args in (("dev", ""), ("release", "--release")):
            results.append(
                Result.from_commands_run(
                    name=f"Rust tests ({name})",
                    command=cargo_test(profile_args),
                    with_log=True,
                )
            )

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
