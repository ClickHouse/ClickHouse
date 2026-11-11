"""Differential fuzzing of data lake tables written by foreign tools and read by ClickHouse.

See README.md in this directory. Entry points:

* `make_history(writer, seed, num_ops, gates)`: a seeded random history (JSON-serializable),
* `run_history(ctx, history, deadline)`: execute and check it, returns a `RunResult`,
* `shrink(ctx, history, failure, deadline)`: minimize a failing history,
* `failure_report(result, report_dir, shrunk)`: the human-readable report with a replay command.
"""

from .budget import Budget, load_budget
from .context import FuzzContext
from .known_issues import KNOWN_ISSUES, Gates
from .oracle import FuzzFailure, Oracle
from .operations import REGISTRY, OpSpec, dumps_history, register
from .runner import RunResult, failure_report, make_history, run_history, shrink
from .writers import WRITERS, Capabilities, Source, Writer

__all__ = [
    "Budget",
    "Capabilities",
    "FuzzContext",
    "FuzzFailure",
    "Gates",
    "KNOWN_ISSUES",
    "OpSpec",
    "Oracle",
    "REGISTRY",
    "RunResult",
    "Source",
    "WRITERS",
    "Writer",
    "dumps_history",
    "failure_report",
    "load_budget",
    "make_history",
    "register",
    "run_history",
    "shrink",
]
