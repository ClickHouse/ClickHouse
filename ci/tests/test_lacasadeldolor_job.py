"""
Contract tests between `ci/jobs/lacasadeldolor_job.py` and `tests/casa_del_dolor/dolor.py`.

The job has no way to ask `dolor.py` what went wrong: it greps the run log for the
messages `dolor.py` printed. That coupling is invisible to both files, so a reworded
message silently stops setting its flag and the run goes green on a real failure. These
tests read the messages out of `dolor.py` itself, so the drift fails here instead.
"""

import ast
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.lacasadeldolor_job import FORCED_STOP_MESSAGE, STOP_FAILED_MESSAGE

DOLOR_PY = Path(__file__).resolve().parents[2] / "tests/casa_del_dolor/dolor.py"
LOG_CALLS = {"error", "warning", "critical"}
LOGGERS = {"logger", "logging"}


def _static_text(node: ast.AST) -> str:
    """The literal parts of a string argument, with `{...}` for each interpolation."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.JoinedStr):
        return "".join(
            (
                v.value
                if isinstance(v, ast.Constant) and isinstance(v.value, str)
                else "{}"
            )
            for v in node.values
        )
    return ""


def _logged_messages() -> list[str]:
    tree = ast.parse(DOLOR_PY.read_text(encoding="utf-8"))
    messages = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        if (
            isinstance(func, ast.Attribute)
            and func.attr in LOG_CALLS
            and isinstance(func.value, ast.Name)
            and func.value.id in LOGGERS
        ):
            text = _static_text(node.args[0])
            if text:
                messages.append(text)
    return messages


def test_dolor_py_is_readable():
    # Guard the two tests below: a path or parse failure would make them vacuously pass.
    assert DOLOR_PY.is_file(), DOLOR_PY
    assert _logged_messages()


def test_every_stop_failure_message_carries_the_marker():
    # `dolor.py` reports a stop that left the server up on three paths - the teardown
    # stop, a scheduled restart's stop, and the teardown's record of that restart. The
    # job sets `stop_failed` off one marker, so every one of them has to contain it, or
    # that path never flips the flag and `_classify_failed_run` swallows the failure.
    candidates = [m for m in _logged_messages() if "still running" in m]

    assert len(candidates) >= 3, candidates
    for message in candidates:
        assert STOP_FAILED_MESSAGE in message, message


def test_a_forced_stop_message_carries_its_marker():
    # The force-kill marker is deliberately not required of every message: the teardown's
    # "had to be force killed during a scheduled restart earlier in the run" does not
    # carry it, and does not need to. It is only reached when the run already logged the
    # restart's own force-kill message, which does carry it, so the flag is set by then.
    matching = [m for m in _logged_messages() if FORCED_STOP_MESSAGE in m]

    assert len(matching) >= 2, matching
