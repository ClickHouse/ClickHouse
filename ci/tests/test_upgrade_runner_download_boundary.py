"""
Shell-level contract test for the previous-release download boundary in
`tests/docker_scripts/upgrade_runner.sh`.

`download_release_packages` (see `tests/ci/download_release_packages.py`) exits
nonzero when a required previous-release package is missing or fails to download.
The runner must stop at that boundary instead of recording success and reaching
`install_packages`, which would otherwise die later with an opaque `dpkg` glob
error.

The Python unit tests in `test_download_release_packages.py` prove the exit code;
this test proves the shell caller honours it. It extracts the real download block
from the runner (between stable anchor comments) and runs it under bash with a
stub `download_release_packages`, so the test cannot drift from the script it
guards.

See https://github.com/ClickHouse/ClickHouse/pull/107916
"""

import os
import subprocess

_RUNNER = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "tests",
        "docker_scripts",
        "upgrade_runner.sh",
    )
)

_BEGIN = (
    "# --- download previous release packages: "
    "fail closed on a missing required one ---"
)
_END = "# --- end download previous release packages ---"

# Mirrors the `OK`/`FAIL` status columns defined in `stress_tests.lib`. A raw
# string keeps the backslashes literal so the bash harness sees exactly what the
# library defines.
_PREAMBLE = r"""set -ex
OK="\tOK\t\\N\t"
FAIL="\tFAIL\t\\N\t"
previous_release_tag=v1.2.3.4-stable
"""


def _extract_download_block():
    with open(_RUNNER, encoding="utf-8") as f:
        text = f.read()
    begin = text.index(_BEGIN)
    end = text.index(_END, begin)
    # Keep the BEGIN anchor, drop the END marker line.
    return text[begin:end]


def _run_block(tmp_path, download_exit_code):
    block = _extract_download_block()

    # The runner writes to the absolute path /test_output; redirect it into the
    # test's temporary directory.
    test_output = tmp_path / "test_output"
    test_output.mkdir()
    block = block.replace("/test_output", str(test_output))

    # Stub `download_release_packages` (a PATH command in the real job, symlinked
    # to the Python script) so the block's only moving part is its own control
    # flow. It drains stdin (the piped release tag) and exits with the given code.
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    stub = bin_dir / "download_release_packages"
    stub.write_text(f"#!/bin/bash\ncat >/dev/null\nexit {download_exit_code}\n")
    stub.chmod(0o755)

    # `REACHED_INSTALL` stands in for the `install_packages` call that follows the
    # block in the real runner: it must be reached on success and skipped on
    # failure.
    script = _PREAMBLE + block + "\necho REACHED_INSTALL\n"

    env = dict(os.environ, PATH=f"{bin_dir}:{os.environ['PATH']}")
    proc = subprocess.run(
        ["bash", "-c", script],
        capture_output=True,
        text=True,
        env=env,
        cwd=str(tmp_path),
        check=False,
    )
    return proc, test_output


def test_runner_stops_when_download_fails(tmp_path):
    proc, test_output = _run_block(tmp_path, download_exit_code=1)

    # The runner must exit before reaching `install_packages`.
    assert proc.returncode != 0
    assert "REACHED_INSTALL" not in proc.stdout
    assert (test_output / "check_status.tsv").read_text().startswith("failure")
    assert "FAIL" in (test_output / "test_results.tsv").read_text()


def test_runner_continues_when_download_succeeds(tmp_path):
    proc, test_output = _run_block(tmp_path, download_exit_code=0)

    # A successful download records the OK row and proceeds to `install_packages`.
    assert proc.returncode == 0, proc.stderr
    assert "REACHED_INSTALL" in proc.stdout
    assert not (test_output / "check_status.tsv").exists()
    assert "OK" in (test_output / "test_results.tsv").read_text()
