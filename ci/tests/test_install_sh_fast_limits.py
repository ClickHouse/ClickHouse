"""
Shell-level contract test for the `limits_fast.yaml` gate in `tests/config/install.sh`.

`users.d/limits_fast.yaml` pins `max_execution_time` / `max_execution_time_leaf` to 60
seconds. It is installed by one branch near the end of `install.sh`, and that branch was
dead from #98026 (2026-02-26) until this test was added: the guard read
`[[ $(is_fast_build) == 1 ]]` while `is_fast_build` only `return`s an exit code and prints
nothing, so the command substitution was always empty and the symlink was never created on
any job. Nothing asserted on the symlink, so reverting the fix passed CI.

The gate is a conjunction of two independent decisions and this test pins both:

  - job scope: only the Fast test job (`--fast-test`), whose runner already kills a test
    file after 60 seconds of wall clock (`ci/jobs/fast_test.py:378` `--timeout 60`), so a
    60 second per-query limit cannot fire there on a healthy test. Other jobs satisfying
    `is_fast_build` run the long tests that Fast test skips, where the cap would redden
    green runs.
  - `is_fast_build` itself: not a sanitizer or debug build, not a coverage build (the
    reason #98026 reverted the original fix), and no object storage or encrypted storage.

`install.sh` is driven for real, with a stub `clickhouse` earlier on `PATH` that answers
the `system.build_options` probes. The shell function under test is never stubbed, so the
test cannot pass because of a mock. Assertions are on the resulting symlink rather than on
`set -x` trace text, so they do not drift with logging changes.
"""

import os
import subprocess

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_INSTALL_SH = os.path.join(_REPO_ROOT, "tests", "config", "install.sh")

# Answers the two `system.build_options` probes in `is_fast_build`, plus the
# `clickhouse --query` calls the surrounding script makes. The probes are SQL expressions
# over the row, not the row itself, so the stub models the row VALUE (`STUB_WITH_COVERAGE`)
# and evaluates the predicate the way the server would; returning the raw value instead
# would make the coverage cases pass for the wrong reason. `STUB_WITH_COVERAGE_ROW` = "0"
# models the row being absent, where a real server returns zero rows, i.e. no output.
_CLICKHOUSE_STUB = r"""#!/usr/bin/env python3
import os
import sys

argv = sys.argv[1:]
if argv and argv[0] == "--version":
    print("ClickHouse local version 26.8.1.1.")
    sys.exit(0)
if len(argv) >= 3 and argv[0] == "local" and argv[1] == "--query":
    query = argv[2]
    if "WITH_COVERAGE" in query:
        # SELECT upper(value) IN ('ON', '1') FROM system.build_options WHERE name = ...
        if os.environ.get("STUB_WITH_COVERAGE_ROW", "1") != "1":
            print("")  # no such row -> zero rows -> empty output
        else:
            value = os.environ.get("STUB_WITH_COVERAGE", "OFF")
            print("1" if value.upper() in ("ON", "1") else "0")
    elif "-fsanitize=" in query and "NOT LIKE" in query:
        # the is_fast_build predicate: 1 means "this IS a fast build"
        if os.environ.get("STUB_CXX_FLAGS_ROW", "1") != "1":
            print("")  # no such row -> zero rows -> empty output
        else:
            print(os.environ.get("STUB_IS_FAST_BUILD", "1"))
    elif "-fsanitize=" in query:
        # is_sanitizer_build
        print("0")
    elif "USE_OPENSSL_FIPS" in query:
        print("0")
    else:
        print("")
    sys.exit(0)
sys.exit(0)
"""


def _run_install(tmp_path, script=None, args=(), env=None):
    """Run install.sh into a scratch destination and report whether the symlink appeared.

    `install.sh` derives SRC_PATH from its own location, so a script under test must live
    in `tests/config/`; a copy elsewhere makes every `ln -sf` source path wrong and the
    script dies before reaching the gate, which looks exactly like "no symlink" and would
    turn every negative case into a false pass. Guard against that by requiring the gate
    to have been reached (the last `ln -sf` of the run must have happened).
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "clickhouse"
    stub.write_text(_CLICKHOUSE_STUB, encoding="utf-8")
    stub.chmod(0o755)

    server_dir = tmp_path / "etc" / "clickhouse-server"
    client_dir = tmp_path / "etc" / "clickhouse-client"
    server_dir.mkdir(parents=True)
    client_dir.mkdir(parents=True)

    run_env = dict(os.environ)
    run_env["PATH"] = f"{bin_dir}{os.pathsep}{run_env['PATH']}"
    # install.sh reads these unconditionally; keep every case explicit rather than
    # inheriting whatever the CI job exported.
    for name in (
        "USE_S3_STORAGE_FOR_MERGE_TREE",
        "USE_AZURE_STORAGE_FOR_MERGE_TREE",
        "USE_ENCRYPTED_STORAGE",
    ):
        run_env.pop(name, None)
    run_env.update(env or {})

    proc = subprocess.run(
        ["bash", script or _INSTALL_SH, str(server_dir), str(client_dir), *args],
        cwd=_REPO_ROOT,
        env=run_env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    # Deliberately not asserting proc.returncode == 0: install.sh runs `set -e` and its
    # later steps assume a real server environment, so a nonzero exit is acceptable here.
    # What matters is that the gate was reached, and that the symlink decision is right.
    return proc, (server_dir / "users.d" / "limits_fast.yaml").is_symlink()


def _assert_gate_reached(proc):
    # `limits.yaml` is symlinked immediately before the gate, unconditionally. Its
    # presence in the trace proves the run got that far, so an "absent symlink" verdict
    # below means the guard declined rather than that the script died early.
    trace = proc.stdout + proc.stderr
    assert "users.d/limits.yaml" in trace, (
        "install.sh did not reach the limits_fast gate; the negative cases would be "
        f"false passes. stdout+stderr tail:\n{trace[-2000:]}"
    )


def test_fast_test_job_installs_limits_fast(tmp_path):
    """The one job that gets the profile: --fast-test on a non-sanitizer build."""
    proc, linked = _run_install(tmp_path, args=["--fast-test"])
    _assert_gate_reached(proc)
    assert linked, "limits_fast.yaml must be installed for the Fast test job"


def test_stateless_job_does_not_install_limits_fast(tmp_path):
    """A stateless job satisfies is_fast_build but must NOT get the 60s cap.

    Without --fast-test the profile would apply to jobs that run the `long` tests Fast
    test skips, several of which exceed 60 seconds in a single query on every run.
    """
    proc, linked = _run_install(tmp_path)
    _assert_gate_reached(proc)
    assert not linked, "limits_fast.yaml must not be installed without --fast-test"


def test_sanitizer_or_debug_build_does_not_install_limits_fast(tmp_path):
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_IS_FAST_BUILD": "0"}
    )
    _assert_gate_reached(proc)
    assert not linked, "sanitizer and debug builds are too slow for a 60s cap"


def test_coverage_build_does_not_install_limits_fast(tmp_path):
    """The #98026 revert reason: a coverage build passes the CXX_FLAGS predicate.

    Coverage flags are applied per-directory, so they never reach CXX_FLAGS; the guard
    must use the dedicated `WITH_COVERAGE` row instead.
    """
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE": "ON"}
    )
    _assert_gate_reached(proc)
    assert not linked, "coverage builds are ~2-3x slower and must not be capped"


def test_coverage_row_value_is_matched_case_insensitively(tmp_path):
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE": "on"}
    )
    _assert_gate_reached(proc)
    assert not linked, "the WITH_COVERAGE check must accept 'on' as well as 'ON'"


def test_s3_storage_does_not_install_limits_fast(tmp_path):
    proc, linked = _run_install(tmp_path, args=["--fast-test", "--s3-storage"])
    _assert_gate_reached(proc)
    assert not linked, "tests on MinIO are slow and must not be capped"


def test_azure_storage_does_not_install_limits_fast(tmp_path):
    proc, linked = _run_install(tmp_path, args=["--fast-test", "--azure"])
    _assert_gate_reached(proc)
    assert not linked, "tests on azurite are slow and must not be capped"


def test_exported_encrypted_storage_does_not_install_limits_fast(tmp_path):
    """The guard must respect an EXPORTED USE_ENCRYPTED_STORAGE.

    `stress_runner.sh` exports a randomized value rather than passing the flag, so
    `is_fast_build` must not shadow it with a local default of its own.
    """
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"USE_ENCRYPTED_STORAGE": "1"}
    )
    _assert_gate_reached(proc)
    assert not linked, "encrypted storage is slow and must not be capped"


def test_missing_coverage_row_fails_open(tmp_path):
    """An absent `WITH_COVERAGE` row must degrade to the pre-guard behaviour.

    A previous-release binary can lack the row (it exists on 26.5+ but not 25.8), and
    `upgrade_runner.sh` runs install.sh with such a binary on PATH. The probe then yields
    an empty string, which must not be read as an integer nor abort the script.
    """
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE_ROW": "0"}
    )
    _assert_gate_reached(proc)
    assert linked, "an absent WITH_COVERAGE row must not exclude a fast build"
    assert "integer expression expected" not in (proc.stdout + proc.stderr), (
        "the guards must compare strings, not integers, so an empty probe result is "
        "not a shell error"
    )


def test_missing_cxx_flags_row_is_not_a_shell_error(tmp_path):
    """The build-kind probe must also tolerate an empty result as a plain string.

    Same previous-release reachability as above. Comparing an empty result with `-eq`
    makes bash print `integer expression expected` and return 2, which is fail-open by
    accident rather than by construction; the noise appears on a path the script really
    reaches, so compare strings instead.
    """
    proc, linked = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_CXX_FLAGS_ROW": "0"}
    )
    _assert_gate_reached(proc)
    assert not linked, "an unknown build kind must not be treated as a fast build"
    assert "integer expression expected" not in (
        proc.stdout + proc.stderr
    ), "the build-kind check must compare strings, not integers"
