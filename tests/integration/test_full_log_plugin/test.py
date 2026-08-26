"""Regression test for https://github.com/ClickHouse/ClickHouse/issues/92886.

Runs a pytest session in a subprocess with full_log_plugin active and asserts the
resulting complete log carries print()/stdout from setup, call and teardown - which
the plain log_cli/log_file stream dropped - each line timestamped. Fails before the
fix (no plugin, so no such log). Pure Python, no cluster; runs in the integration
suite rather than a separate job.
"""

import os
import re
import subprocess
import sys
import textwrap
from pathlib import Path

_SAMPLE = textwrap.dedent("""
    import logging
    import pytest

    log = logging.getLogger(__name__)

    @pytest.fixture
    def res():
        log.info("SETUP log line")
        print("SETUP print line")
        yield
        log.info("TEARDOWN log line")
        print("TEARDOWN print line")

    def test_it(res):
        log.info("CALL log line")
        print("CALL print line")
    """)

_CONFTEST = textwrap.dedent("""
    from full_log_plugin import FullLogPlugin, full_log_path

    def pytest_configure(config):
        config.pluginmanager.register(
            FullLogPlugin(full_log_path(config)), "full_log_plugin"
        )
    """)

_INI = textwrap.dedent("""
    [pytest]
    python_files = test_*/test*.py
    log_level = DEBUG
    log_format = %(asctime)s [ %(process)d ] %(levelname)s : %(message)s
    log_date_format = %Y-%m-%d %H:%M:%S.%f
    """)

_TS = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+ ")


def test_full_log_captures_print_from_all_phases(tmp_path):
    (tmp_path / "test_s").mkdir()
    (tmp_path / "test_s" / "test_x.py").write_text(_SAMPLE)
    (tmp_path / "conftest.py").write_text(_CONFTEST)
    (tmp_path / "pytest.ini").write_text(_INI)

    integration_dir = Path(__file__).resolve().parent.parent
    env = dict(os.environ, PYTHONPATH=str(integration_dir))
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(tmp_path / "test_s"),
            "-o",
            f"log_file={tmp_path / 'pytest_x.log'}",
            "-p",
            "no:cacheprovider",
            "-q",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        check=True,
    )

    full = tmp_path / "pytest_x.full.log"
    assert full.exists(), "full_log_plugin did not produce a complete log"
    text = full.read_text()

    for phase in ("setup", "call", "teardown"):
        assert f"[{phase}]" in text

    for line in ("SETUP print line", "CALL print line", "TEARDOWN print line"):
        stamped = [l for l in text.splitlines() if l.endswith(line)]
        assert stamped, f"missing captured print(): {line}"
        assert all(
            _TS.match(l) for l in stamped
        ), f"print() line not timestamped: {line}"
