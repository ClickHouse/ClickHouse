"""
Guard for the CIDB-staging-overload sender snapshot across binary swaps in
`ci/jobs/functional_tests.py`.

The overload heuristic in `FTResultsProcessor` is safe only because it is keyed
off the CONCRETE `system.<table>_sender` `Distributed` tables that
`start_log_exports` created before any test ran - a set a test cannot forge.

The bugfix-validation loop restarts the server for every master-HEAD build
type, and `CH.start()` wipes the server data directory: after the first swap
those CI-owned tables no longer exist and log export is not set up again. If
the pre-swap snapshot were still passed to the processor for the later build
types, a test could create `system.query_log_sender` itself and have its
shipping errors read as CI log-export noise. So the snapshot must be cleared on
the restart path, which makes the classifier abstain (fail closed) and keeps
the run on the `Server died` path.

See the `clickhouse-gh[bot]` review on ClickHouse/ClickHouse#106176.
"""

import ast
import os
from pathlib import Path

_SOURCE = Path(os.path.dirname(__file__)).parent / "jobs" / "functional_tests.py"


def _lines():
    return _SOURCE.read_text(encoding="utf-8").splitlines()


def _index_of(lines, needle, start=0):
    for i in range(start, len(lines)):
        if needle in lines[i]:
            return i
    return -1


def test_sender_snapshot_is_cleared_before_the_binary_swap_restart():
    lines = _lines()

    clean_logs = _index_of(lines, "CH.clean_logs()")
    assert clean_logs != -1, "the bugfix-validation binary swap no longer calls `CH.clean_logs()`"

    reset = _index_of(lines, 'log_export_state["senders"] = {}', clean_logs)
    assert reset != -1, (
        "the pre-swap `system.<table>_sender` snapshot is not cleared on the "
        "binary-swap path - the overload heuristic would stay keyed off names "
        "that no longer belong to CI"
    )

    restart = _index_of(lines, "if not (CH.start() and CH.wait_ready()):", clean_logs)
    assert restart != -1, "the binary-swap restart guard is gone"
    assert reset < restart, (
        "the snapshot must be cleared before the server is restarted with the "
        "swapped binary"
    )


def test_later_build_type_processor_uses_the_cleared_snapshot():
    lines = _lines()

    reset = _index_of(lines, 'log_export_state["senders"] = {}')
    processor = _index_of(lines, "ft_res_processor_bt = FTResultsProcessor(", reset)
    assert processor != -1, (
        "the per-build-type `FTResultsProcessor` is no longer constructed after "
        "the snapshot reset - re-check that it cannot see a stale sender set"
    )
    assert 'log_export_state["senders"]' in "".join(lines[processor : processor + 6])


def test_every_processor_gets_a_verified_sender_set():
    """No `FTResultsProcessor` may be handed the raw pre-suite snapshot: the
    names must go through `CH.verify_log_export_senders`, which drops any
    sender table that was dropped or rebound while the tests ran."""
    lines = _lines()

    i = 0
    constructions = 0
    while True:
        i = _index_of(lines, "FTResultsProcessor(", i)
        if i == -1:
            break
        block = "".join(lines[i : i + 8])
        if "log_export_senders" in block:
            constructions += 1
            assert "CH.verify_log_export_senders(" in block, (
                f"`FTResultsProcessor` at line {i + 1} is given a sender set "
                "that was not re-verified after the suite"
            )
        i += 1

    assert constructions == 2, (
        "expected exactly the main-path and per-build-type processors to take "
        f"a sender set, found {constructions}"
    )


def test_snapshot_is_only_populated_from_the_pre_suite_capture():
    """The only place that fills the set is the `START` stage capture right
    after `start_log_exports` succeeded. Any other writer would reintroduce a
    forgeable path."""
    tree = ast.parse(_SOURCE.read_text(encoding="utf-8"))

    populating = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.value, ast.Name)
                and target.value.id == "log_export_state"
            ):
                populating.append(ast.unparse(node.value))

    assert populating, "`log_export_state` is no longer assigned anywhere"
    assert set(populating) == {"CH.get_log_export_senders()", "{}"}, populating
