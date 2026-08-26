"""
Regression test for https://github.com/ClickHouse/ClickHouse/issues/92886.

A failed test's captured output (stdout/print and log, across setup/call/teardown)
must be attached to its report info by from_pytest_jsonl, so the report shows it
without unpacking the artifact archive. Fails before capture became the default.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultTranslator


def _run(entries):
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    )
    for entry in entries:
        f.write(json.dumps(entry) + "\n")
    f.close()
    try:
        return ResultTranslator.from_pytest_jsonl(f.name)
    finally:
        os.unlink(f.name)


def test_failed_test_carries_captured_sections():
    entries = [
        {"pytest_version": "8.0.0", "$report_type": "SessionStart"},
        {
            "$report_type": "TestReport",
            "nodeid": "t.py::test_x",
            "when": "call",
            "outcome": "failed",
            "duration": 0.1,
            "longrepr": {"reprcrash": {"message": "boom"}},
            "sections": [
                ["Captured stdout call", "PRINT from the test body\n"],
                [
                    "Captured log call",
                    "2020-01-01 00:00:00 INFO : LOG from the test body",
                ],
            ],
        },
        {"exitstatus": 1, "$report_type": "SessionFinish"},
    ]
    result = _run(entries)

    assert len(result.results) == 1
    test = result.results[0]
    assert test.status == Result.Status.FAIL
    assert "PRINT from the test body" in test.info
    assert "LOG from the test body" in test.info
