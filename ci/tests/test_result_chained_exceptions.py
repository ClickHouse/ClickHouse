"""
Tests for chained-exception handling in ResultTranslator.from_pytest_jsonl.

A chained failure (a test body raises, then a context manager's __exit__, a
`finally` or an `except` handler raises too) puts every exception in longrepr's
"chain", oldest first, while "reprtraceback" is only its last entry.  The
rendered info must name the original cause, not just the final exception.
"""

import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.result import Result, ResultTranslator

CAUSE = "TimeoutError: REAL_CAUSE"
FINAL = "AssertionError: postcondition"


def _traceback(lines, path, lineno, message):
    """A serialized pytest ReprTraceback with a single frame."""
    return {
        "reprentries": [
            {
                "type": "ReprEntry",
                "data": {
                    "lines": lines,
                    "reprfuncargs": {"args": []},
                    "reprlocals": None,
                    "reprfileloc": {
                        "path": path,
                        "lineno": lineno,
                        "message": message,
                    },
                    "style": "long",
                },
            }
        ],
        "extraline": None,
        "style": "long",
    }


_CAUSE_TB = _traceback(
    [">   def _raise(): raise TimeoutError('REAL_CAUSE')", f"E   {CAUSE}"],
    "t.py",
    2,
    "TimeoutError",
)
_CAUSE_CRASH = {"path": "/abs/t.py", "lineno": 2, "message": CAUSE}
_FINAL_TB = _traceback(
    [">       assert False, 'postcondition'", f"E       {FINAL}"],
    "t.py",
    11,
    "AssertionError",
)
_FINAL_CRASH = {"path": "/abs/t.py", "lineno": 11, "message": FINAL}
SEPARATOR = "During handling of the above exception, another exception occurred:"


def _longrepr(chain):
    """pytest sets reprtraceback/reprcrash from the LAST chain entry."""
    return {
        "reprcrash": chain[-1][1],
        "reprtraceback": chain[-1][0],
        "sections": [],
        "chain": chain,
    }


def _render(longrepr):
    """Run from_pytest_jsonl over a one-failure report-log, return its info."""
    entries = (
        [{"pytest_version": "8.0.0", "$report_type": "SessionStart"}]
        + [
            {
                "$report_type": "TestReport",
                "nodeid": "t.py::test_x",
                "when": "call",
                "outcome": "failed",
                "duration": 0.1,
                "sections": [],
                "longrepr": longrepr,
            }
        ]
        + [{"exitstatus": 1, "$report_type": "SessionFinish"}]
    )
    f = tempfile.NamedTemporaryFile(
        mode="w", suffix=".jsonl", delete=False, encoding="utf-8"
    )
    for entry in entries:
        f.write(json.dumps(entry) + "\n")
    f.close()
    try:
        r = ResultTranslator.from_pytest_jsonl(f.name)
        assert len(r.results) == 1
        test = r.results[0]
        assert test.status == Result.Status.FAIL
        return test.info or ""
    finally:
        os.unlink(f.name)


CHAINED = _longrepr(
    [
        [_CAUSE_TB, _CAUSE_CRASH, SEPARATOR],
        [_FINAL_TB, _FINAL_CRASH, None],
    ]
)
UNCHAINED = _longrepr([[_FINAL_TB, _FINAL_CRASH, None]])


def test_chained_failure_keeps_the_original_cause():
    """The cause must be present, not only the exception that masked it."""
    info = _render(CHAINED)
    assert CAUSE in info, f"original cause erased from:\n{info}"
    assert FINAL in info, f"final exception missing from:\n{info}"


def test_chained_failure_is_cause_first_with_separator():
    """Causal order, with pytest's own separator between the two exceptions."""
    info = _render(CHAINED)
    assert info.index(CAUSE) < info.index(FINAL), f"reverse causal order:\n{info}"
    assert SEPARATOR in info, f"causal separator dropped from:\n{info}"
    assert info.index(CAUSE) < info.index(SEPARATOR) < info.index(FINAL)


def test_chained_failure_does_not_duplicate_messages():
    """A rendered traceback already ends with its message; do not repeat it."""
    info = _render(CHAINED)
    assert info.count(CAUSE) == 1, f"cause rendered {info.count(CAUSE)}x:\n{info}"
    assert info.count(FINAL) == 1, f"final rendered {info.count(FINAL)}x:\n{info}"


def test_unchained_failure_render_is_unchanged():
    """The common single-exception case must render exactly as it always has."""
    info = _render(UNCHAINED)
    assert info == (
        "File: t.py:11 - AssertionError\n"
        ">       assert False, 'postcondition'\n"
        f"E       {FINAL}"
    ), f"unchained rendering changed:\n{info}"
    assert SEPARATOR not in info


if __name__ == "__main__":
    for _name, _fn in sorted(list(globals().items())):
        if _name.startswith("test_"):
            _fn()
            print(f"PASS {_name}")
    print("All tests passed.")
