"""
Shell-level contract test for the `limits_fast.yaml` gate in `tests/config/install.sh`.

The gate is a conjunction of two independent decisions and this test pins both: the job
scope (`--fast-test` only) and `is_fast_build` (not a sanitizer, debug, coverage, object
storage or encrypted storage build). Assertions are on the resulting symlink and its
target, not on `set -x` trace text, so they do not drift with logging changes.

`install.sh` is driven for real. A stub `clickhouse` earlier on `PATH` supplies the
`system.build_options` ROWS as a synthetic table and hands the query text through to a real
`clickhouse local`, so the SQL predicates in `is_fast_build` are genuinely evaluated and a
mutation to them fails these tests. Neither the shell function nor its SQL is stubbed.

Gotcha: `install.sh` derives SRC_PATH from its own location, so a script under test must
live in `tests/config/`; a copy elsewhere makes every `ln -sf` source wrong and the script
dies before reaching the gate, which looks exactly like "no symlink" and would turn every
negative case into a false pass. `_assert_gate_reached` is what rules that out.
"""

import os
import shutil
import subprocess

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
_INSTALL_SH = os.path.join(_REPO_ROOT, "tests", "config", "install.sh")
_LIMITS_FAST = os.path.join(
    _REPO_ROOT, "tests", "config", "users.d", "limits_fast.yaml"
)

# A fast build: optimized, NDEBUG, no sanitizer. The stub feeds these to a real server as
# row VALUES, so the predicates under test decide the verdict, not the stub.
_FAST_CXX_FLAGS = "-O3 -DNDEBUG -std=c++2c"
_SANITIZER_CXX_FLAGS = "-O2 -DNDEBUG -fsanitize=address,undefined"
_DEBUG_CXX_FLAGS = "-O0 -g -std=c++2c"

# Rewrites `system.build_options` to a synthetic table carrying the requested rows and
# forwards the rest of the query text unchanged to a real `clickhouse local`. `*_ROW` set to
# anything but "1" omits that row, reproducing a previous-release binary that lacks it,
# where a real server returns zero rows and therefore an empty result.
_CLICKHOUSE_STUB = r"""#!/usr/bin/env python3
import os
import sys

argv = sys.argv[1:]
if argv and argv[0] == "--version":
    print("ClickHouse local version 26.8.1.1.")
    sys.exit(0)


def sql_str(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def build_options_rows():
    rows = []
    if os.environ.get("STUB_CXX_FLAGS_ROW", "1") == "1":
        rows.append(("CXX_FLAGS", os.environ["STUB_CXX_FLAGS"]))
    if os.environ.get("STUB_WITH_COVERAGE_ROW", "1") == "1":
        rows.append(("WITH_COVERAGE", os.environ["STUB_WITH_COVERAGE"]))
    rows.append(("USE_OPENSSL_FIPS", os.environ["STUB_USE_OPENSSL_FIPS"]))
    literals = ", ".join(
        "(" + sql_str(name) + ", " + sql_str(value) + ")" for name, value in rows
    )
    return (
        "WITH build_options AS (SELECT arrayJoin(CAST(["
        + literals
        + "], 'Array(Tuple(String, String))')) AS t, t.1 AS name, t.2 AS value) "
    )


if len(argv) >= 3 and argv[0] == "local" and argv[1] == "--query":
    query = argv[2]
    if "system.build_options" not in query:
        print("")
        sys.exit(0)
    real = os.environ["STUB_REAL_CLICKHOUSE"]
    rewritten = build_options_rows() + query.replace(
        "system.build_options", "build_options"
    )
    os.execv(real, [real, "local", "--query", rewritten])
sys.exit(0)
"""


def _real_clickhouse():
    """The binary the stub delegates to.

    The `CI Tests` job puts one on PATH before pytest starts: `ci_tests_job.py` enters
    `ClickHouseService`, whose `__enter__` calls `Utils.add_to_PATH` and downloads
    `clickhouse` into that directory. Deliberately not skipping when it is missing -- a
    skip would let the suite go silently vacuous if that plumbing ever changes.
    """
    path = shutil.which("clickhouse")
    assert path, (
        "no `clickhouse` binary on PATH; these tests evaluate the install.sh SQL "
        "predicates against a real server and cannot run without one"
    )
    return path


def _run_install(tmp_path, script=None, args=(), env=None):
    """Run install.sh into a scratch destination and report on the limits_fast symlink."""
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
    run_env["STUB_REAL_CLICKHOUSE"] = _real_clickhouse()
    run_env["PATH"] = f"{bin_dir}{os.pathsep}{run_env['PATH']}"
    # install.sh reads these unconditionally; keep every case explicit rather than
    # inheriting whatever the CI job exported.
    for name in (
        "USE_S3_STORAGE_FOR_MERGE_TREE",
        "USE_AZURE_STORAGE_FOR_MERGE_TREE",
        "USE_ENCRYPTED_STORAGE",
    ):
        run_env.pop(name, None)
    run_env.setdefault("STUB_CXX_FLAGS", _FAST_CXX_FLAGS)
    run_env.setdefault("STUB_WITH_COVERAGE", "OFF")
    run_env.setdefault("STUB_USE_OPENSSL_FIPS", "0")
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
    link = server_dir / "users.d" / "limits_fast.yaml"
    return proc, link


def _assert_gate_reached(proc):
    # `limits.yaml` is symlinked immediately before the gate, unconditionally. Its
    # presence in the trace proves the run got that far, so an "absent symlink" verdict
    # below means the guard declined rather than that the script died early.
    trace = proc.stdout + proc.stderr
    assert "users.d/limits.yaml" in trace, (
        "install.sh did not reach the limits_fast gate; the negative cases would be "
        f"false passes. stdout+stderr tail:\n{trace[-2000:]}"
    )


def _assert_installed(proc, link):
    _assert_gate_reached(proc)
    assert link.is_symlink(), "limits_fast.yaml must be installed for this job"
    assert os.path.realpath(link) == os.path.realpath(_LIMITS_FAST), (
        f"limits_fast.yaml points at {os.path.realpath(link)}, expected "
        f"{os.path.realpath(_LIMITS_FAST)}"
    )


def _assert_not_installed(proc, link, why):
    _assert_gate_reached(proc)
    assert not link.is_symlink(), why


def test_fast_test_job_installs_limits_fast(tmp_path):
    """The one job that gets the profile: --fast-test on a non-sanitizer build."""
    proc, link = _run_install(tmp_path, args=["--fast-test"])
    _assert_installed(proc, link)


def test_stateless_job_does_not_install_limits_fast(tmp_path):
    """A stateless job satisfies is_fast_build but must not get the 60s cap."""
    proc, link = _run_install(tmp_path)
    _assert_not_installed(
        proc, link, "limits_fast.yaml must not be installed without --fast-test"
    )


def test_sanitizer_build_does_not_install_limits_fast(tmp_path):
    """A sanitizer build carries -fsanitize= in CXX_FLAGS and is too slow for the cap."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_CXX_FLAGS": _SANITIZER_CXX_FLAGS}
    )
    _assert_not_installed(proc, link, "sanitizer builds are too slow for a 60s cap")


def test_debug_build_does_not_install_limits_fast(tmp_path):
    """A debug build lacks -DNDEBUG; the predicate must require it, not just no sanitizer."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_CXX_FLAGS": _DEBUG_CXX_FLAGS}
    )
    _assert_not_installed(proc, link, "debug builds are too slow for a 60s cap")


def test_coverage_build_does_not_install_limits_fast(tmp_path):
    """Coverage flags are per-directory and never reach CXX_FLAGS, so the dedicated
    WITH_COVERAGE row is what has to exclude such a build."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE": "ON"}
    )
    _assert_not_installed(proc, link, "coverage builds are ~2-3x slower")


def test_coverage_row_value_is_matched_case_insensitively(tmp_path):
    """cmake can render the row as 'on'; the check must not be case sensitive."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE": "on"}
    )
    _assert_not_installed(
        proc, link, "the WITH_COVERAGE check must accept 'on' as well as 'ON'"
    )


def test_s3_storage_does_not_install_limits_fast(tmp_path):
    """Tests on MinIO are slow."""
    proc, link = _run_install(tmp_path, args=["--fast-test", "--s3-storage"])
    _assert_not_installed(proc, link, "object storage runs must not be capped")


def test_azure_storage_does_not_install_limits_fast(tmp_path):
    """Tests on azurite are slow."""
    proc, link = _run_install(tmp_path, args=["--fast-test", "--azure"])
    _assert_not_installed(proc, link, "object storage runs must not be capped")


def test_exported_encrypted_storage_does_not_install_limits_fast(tmp_path):
    """`stress_runner.sh` exports a randomized USE_ENCRYPTED_STORAGE rather than passing a
    flag, so is_fast_build must not shadow it with a local default of its own."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"USE_ENCRYPTED_STORAGE": "1"}
    )
    _assert_not_installed(proc, link, "encrypted storage is slow and must not be capped")


def test_missing_coverage_row_fails_open(tmp_path):
    """A previous-release binary can lack the WITH_COVERAGE row (26.5+ has it, 25.8 does
    not) and `upgrade_runner.sh` runs install.sh with one on PATH; the empty result must
    degrade to the pre-guard behaviour rather than abort or be read as an integer."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_WITH_COVERAGE_ROW": "0"}
    )
    _assert_installed(proc, link)
    assert "integer expression expected" not in (proc.stdout + proc.stderr), (
        "the guards must compare strings, not integers, so an empty probe result is "
        "not a shell error"
    )


def test_missing_cxx_flags_row_is_not_a_shell_error(tmp_path):
    """Same previous-release reachability for the build-kind probe: an empty result must be
    compared as a string, otherwise bash prints `integer expression expected` on a path the
    script really reaches and the fail-open is accidental rather than by construction."""
    proc, link = _run_install(
        tmp_path, args=["--fast-test"], env={"STUB_CXX_FLAGS_ROW": "0"}
    )
    _assert_not_installed(
        proc, link, "an unknown build kind must not be treated as a fast build"
    )
    assert "integer expression expected" not in (
        proc.stdout + proc.stderr
    ), "the build-kind check must compare strings, not integers"
