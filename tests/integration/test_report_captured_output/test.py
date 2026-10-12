"""Regression test for https://github.com/ClickHouse/ClickHouse/issues/92886.

End-to-end: run a real pytest whose failing test writes output, feed the real
--report-log through the same ResultTranslator the integration job uses, and assert
a failed test's captured output reaches its report info. Fails before capture
became the default in from_pytest_jsonl.
"""

import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from ci.praktika.result import Result, ResultTranslator

# The marker is assembled at runtime so it cannot leak into info via the traceback's
# echo of the print() source line - only real captured stdout can carry it.
_SAMPLE = textwrap.dedent("""
    def test_fails_with_output():
        marker = "-".join(["RUNTIME", "ONLY", "MARKER"])
        print(marker)
        assert False, "boom"
    """)


def test_failed_test_captured_output_reaches_report_info():
    work = Path(tempfile.mkdtemp())
    (work / "test_s").mkdir()
    (work / "test_s" / "test_x.py").write_text(_SAMPLE)
    report = work / "report.jsonl"

    subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            str(work / "test_s"),
            f"--report-log={report}",
            "-p",
            "no:cacheprovider",
            "-q",
        ],
        cwd=work,
        capture_output=True,
        check=False,
    )

    result = ResultTranslator.from_pytest_jsonl(str(report))
    failed = [t for t in result.results if t.status == Result.Status.FAIL]
    assert len(failed) == 1, f"expected one failed test, got {result.results}"
    assert "RUNTIME-ONLY-MARKER" in (failed[0].info or ""), failed[0].info
