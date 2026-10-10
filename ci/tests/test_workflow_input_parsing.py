"""
Test for Runner._parse_workflow_inputs.

Covers the parser used by the `--workflow-input` CLI flag, which accepts
comma-separated `name=value` pairs and writes them to workflow_inputs.json
for jobs to read via `Info.get_workflow_input_value`.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.runner import Runner


def test_parse_workflow_inputs():
    # Exercises whitespace stripping, empty value, value containing `=`,
    # skipping of malformed entries (no `=`), and rejection of entries
    # with an empty name (e.g. `=25.4`) in a single call.
    inputs = Runner._parse_workflow_inputs(
        " ref = main , type=patch , dry-run= , expr=a=b=c , bogus , =25.4 "
    )
    assert inputs == {
        "ref": "main",
        "type": "patch",
        "dry-run": "",
        "expr": "a=b=c",
    }
