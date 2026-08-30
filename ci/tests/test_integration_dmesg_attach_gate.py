"""
Tests for the gate in `ci.jobs.integration_test_job` that decides whether the kernel's
memory-kill record survives a run: the `print_oom_lines` summary in the job log, and
`./ci/tmp/dmesg.log` in the job's files.

On a cgroups v1 runner the memcg OOM counter is charged to the cgroup of the task that was
killed, so a test container killed by its own limit is invisible to every counter-based
verdict in the job. For that shape the dump is the only record that anything was killed at
all, which is why the gate keys off the run's outcome rather than off a verdict. That makes
its two failure modes opposite, and both silent.

Too narrow and a red job loses the only evidence of a kill. Too wide and a run the job
reports OK, or downgrades to best-effort SKIPPED, uploads a kernel log nobody asked for.
The widest term reads the whole result list, which on a graceful xdist session-timeout
carries the synthetic `Timeout` row. That row is not a failure of its own: `main` strips it
again for targeted, flaky and bugfix-validation checks, all three of which treat a
session-timeout as an expected risk, and the run then reports OK or best-effort SKIPPED.

The gate lives inside `main`, which cannot be called without starting containers, so the
real `if` statement is sliced out by AST span and executed against namespaces built from the
real assignment sites. The reported status is derived the same way, from the module's own
strip statement and `is_empty_best_effort_skip`, so "the job does not call this shape a
failure" is measured here rather than assumed.
"""

import ast
import contextlib
import io
import os
import sys
import textwrap
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.integration_test_job as job
from ci.praktika.result import Result

JOB_SOURCE = Path(job.__file__).read_text()

# The name of the synthetic row appended on a graceful session-timeout. Kept as one literal
# here so a rename in production breaks a test rather than silently desynchronizing the two
# sites that must agree on it.
TIMEOUT_ROW_NAME = "Timeout"

ATTACHED_PATH = "./ci/tmp/dmesg.log"

# Verbatim shape of a pytest failure raised by a client whose container went away: the docker
# context `_is_infrastructure_error` requires for a FAIL, plus one of its error phrases.
DOCKER_RESET_INFO = (
    "test setup failure\n"
    "E   subprocess.CalledProcessError: Command '['docker', 'exec', "
    "'roottests3plainrewritable-gw1-node1-1', 'bash', '-c', 'clickhouse client "
    '--query "OPTIMIZE TABLE test FINAL"\']\' returned non-zero exit status 1.\n'
    "E   ConnectionResetError: [Errno 104] Connection reset by peer\n"
)

# Verbatim `journalctl -k` text of a container killed by its own memory limit, which is the shape
# the counter-based verdicts cannot attribute. Kept verbatim because the marker the printer selects
# is the `oom-kill:` constraint line, not the `Memory cgroup out of memory:` line below it.
CONTAINER_KILL_DMESG = (
    b"[Thu Aug 27 20:57:57 2026] python3 invoked oom-killer: "
    b"gfp_mask=0x100cca(GFP_HIGHUSER_MOVABLE), order=0, oom_score_adj=0\n"
    b"[Thu Aug 27 20:57:57 2026] oom-kill:constraint=CONSTRAINT_MEMCG,nodemask=(null),"
    b"cpuset=docker-34ce20e4b6d5.scope,mems_allowed=0-5,"
    b"oom_memcg=/system.slice/docker-34ce20e4b6d5.scope,"
    b"task_memcg=/system.slice/docker-34ce20e4b6d5.scope,task=python3,pid=1361510,uid=0\n"
    b"[Thu Aug 27 20:57:57 2026] Memory cgroup out of memory: Killed process 1361510 "
    b"(python3) total-vm:407004kB, anon-rss:387072kB, file-rss:0kB, shmem-rss:0kB\n"
)
KILL_SUMMARY = "Kernel memory kills in dmesg"
# The one line in the fixture the printer's marker selects, taken from the fixture rather than
# restated, so a reword of either cannot leave the assertions checking a line nobody prints.
KILL_LINE = next(
    line for line in CONTAINER_KILL_DMESG.decode().splitlines() if "oom-kill:" in line
)


def _single(nodes, what):
    assert len(nodes) == 1, f"expected one {what}, got {[n.lineno for n in nodes]}"
    return nodes[0]


def _statement(node):
    """Dedented source text of one statement of `main`, ready to `exec`.

    Sliced by node span rather than by indentation: the gate is a multi-line `if (...)`, and an
    indent-based scan stops at the closing `):`, which would hand back a truncated block. The
    `compile` keeps such a slice from reaching the arms as a silent pass.
    """
    lines = JOB_SOURCE.splitlines()
    block = textwrap.dedent("\n".join(lines[node.lineno - 1 : node.end_lineno]))
    compile(block, "<statement>", "exec")
    return block


MAIN = _single(
    [
        node
        for node in ast.walk(ast.parse(JOB_SOURCE))
        if isinstance(node, ast.FunctionDef) and node.name == "main"
    ],
    "`main`",
)


def _calls(node, name):
    return [
        inner
        for inner in ast.walk(node)
        if isinstance(inner, ast.Call)
        and isinstance(inner.func, (ast.Name, ast.Attribute))
        and ast.unparse(inner.func).endswith(name)
    ]


# Both halves of the record, so the innermost selection below cannot land on the inner
# `if dmesg_dumped:`, which guards the file alone.
_ATTACHES = [
    node
    for node in ast.walk(MAIN)
    if isinstance(node, ast.If)
    and any(
        ast.unparse(call) == f"attached_files.append({ATTACHED_PATH!r})"
        for call in _calls(node, "append")
    )
    and _calls(node, "print_oom_lines")
]
# The innermost of the enclosing conditionals, selected structurally so that adding another
# wrapper around the gate cannot silently hand the arms a wider condition than production uses.
GATE = _single(
    [
        node
        for node in _ATTACHES
        if not any(other is not node and other in ast.walk(node) for other in _ATTACHES)
    ],
    "dmesg attach gate",
)
GATE_SOURCE = _statement(GATE)

STRIP = _single(
    [
        node
        for node in ast.walk(MAIN)
        if isinstance(node, ast.If)
        and len(node.body) == 1
        and isinstance(node.body[0], ast.Assign)
        and ast.unparse(node.body[0].value)
        == f"[r for r in test_results if r.name != {TIMEOUT_ROW_NAME!r}]"
    ],
    "synthetic-row strip",
)
STRIP_SOURCE = _statement(STRIP)


def _timeout_row():
    """As `run_pytest_and_collect_results` builds it on a graceful xdist session-timeout."""
    return Result(
        name=TIMEOUT_ROW_NAME,
        status=Result.Status.FAIL,
        info="ERROR: session-timeout occurred during test execution",
    )


def _passed(name="test_foo/test.py::test_bar"):
    return Result(name=name, status=Result.Status.OK)


def _failed(name="test_foo/test.py::test_bar"):
    return Result(name=name, status=Result.Status.FAIL)


def _infra_row(name="test_s3_plain_rewritable/test.py::test"):
    """A pytest failure the real relabeler turns into SKIPPED + INFRA, as a lost container does."""
    row = Result(name=name, status=Result.Status.FAIL, info=DOCKER_RESET_INFO)
    assert job._mark_infrastructure_errors([row]) == 1, "fixture did not reach the relabeler"
    assert row.is_ok() and row.has_label(Result.Label.INFRA), row.status
    return row


def _run_gate(
    rows,
    *,
    attach_dmesg=False,
    has_error=False,
    flaky=False,
    targeted=False,
    bugfix_labelled=False,
    timed_out=False,
    dumped=True,
    gate_source=None,
):
    """`(kept_the_dump, printed)` from running the real gate statement over one namespace.

    Every name comes from the assignment site production reads it from; the dump is a
    container-scoped kill, which no verdict in the job attributes.
    """
    namespace = {
        "attach_dmesg": attach_dmesg,
        "has_error": has_error,
        "test_results": list(rows),
        "attached_files": [],
        "dmesg_dumped": dumped,
        "dmesg_cleared": True,
        "dmesg": CONTAINER_KILL_DMESG,
        "print_oom_lines": job.print_oom_lines,
        "is_flaky_check": flaky,
        "is_targeted_check": targeted,
        "timed_out": timed_out,
        "is_bugfix_validation_labelled": bugfix_labelled,
        "Result": Result,
    }
    log = io.StringIO()
    with contextlib.redirect_stdout(log):
        exec(gate_source or GATE_SOURCE, namespace)  # noqa: S102
    return namespace["attached_files"] == [ATTACHED_PATH], log.getvalue()


def _kept_the_record(kept, printed):
    """Both halves of the record: the attached dump, and the kernel line the summary announces.

    The header alone is a weak oracle. It is printed whenever the marker matched anything, so it
    survives a printer that announces the kills and then emits none of them.
    """
    return kept and KILL_SUMMARY in printed and f"  {KILL_LINE}" in printed


def _reported_status(rows, *, flaky=False, targeted=False, bugfix=False, timed_out=False):
    """The status the job reports for that row set, via the module's own strip and predicates."""
    namespace = {
        "is_targeted_check": targeted,
        "is_flaky_check": flaky,
        "is_bugfix_validation": bugfix,
        "test_results": list(rows),
    }
    exec(STRIP_SOURCE, namespace)  # noqa: S102
    remaining = namespace["test_results"]
    if job.is_empty_best_effort_skip(flaky, targeted, bool(remaining), timed_out):
        return Result.Status.SKIPPED
    return Result.create_from(name="Tests", results=remaining, status="").status


def _gate_without_the_timeout_exclusion():
    """The gate as it reads without the synthetic row excluded, to arm a mutation control."""
    mutant = ast.parse(GATE_SOURCE)
    generators = [
        node
        for node in ast.walk(mutant)
        if isinstance(node, ast.GeneratorExp) and "is_ok" in ast.unparse(node)
    ]
    comprehension = _single(generators, "`is_ok` comprehension").generators[0]
    assert len(comprehension.ifs) == 1, (
        "the gate no longer excludes the synthetic row, so the arms below prove nothing: "
        + ast.unparse(mutant)
    )
    comprehension.ifs = []
    return ast.unparse(ast.fix_missing_locations(mutant))


def test_a_failing_run_keeps_the_kernel_record():
    """Every shape the job reports red, including the two that never set `has_error`."""
    assert _kept_the_record(*_run_gate([_passed(), _failed()]))
    # The oracle above must not be satisfiable by the summary line alone, or a printer that
    # announces the kills and emits none of them would pass every arm in this module.
    assert not _kept_the_record(True, KILL_SUMMARY + ":\n")

    # A verdict already attributed the kill.
    assert _kept_the_record(*_run_gate([_passed()], attach_dmesg=True))
    assert _kept_the_record(*_run_gate([_passed()], has_error=True))

    # A flaky or targeted check whose pytest produced no rows and did not time out: reported
    # ERROR by `empty_harness_failure`, which leaves `has_error` False.
    assert _kept_the_record(*_run_gate([], flaky=True))
    assert _reported_status([], flaky=True) == Result.Status.ERROR
    assert _kept_the_record(*_run_gate([], targeted=True))

    # A bugfix-validation run aborted by an infrastructure error: the only failing row was
    # relabeled SKIPPED, which is a successful status, so no row is non-OK either.
    infra = [_passed(), _infra_row()]
    assert _kept_the_record(*_run_gate(infra, bugfix_labelled=True))
    assert _reported_status(infra, bugfix=True) == Result.Status.OK

    # A session-timeout alongside a genuine failure keeps the record on the failure's account.
    assert _kept_the_record(
        *_run_gate([_failed(), _timeout_row()], flaky=True, timed_out=True)
    )
    assert (
        _reported_status([_failed(), _timeout_row()], flaky=True, timed_out=True)
        == Result.Status.FAIL
    )


def test_a_run_the_job_does_not_call_a_failure_keeps_nothing():
    """A green run, and the two shapes a session-timeout downgrades, upload no kernel log."""
    kept, printed = _run_gate([_passed(), _passed()])
    assert not kept and printed == "", printed
    assert _reported_status([_passed(), _passed()]) == Result.Status.OK

    # A flaky or targeted check that hit its soft time budget after some tests passed: the
    # synthetic row is stripped again and the job reports OK.
    passed_then_timed_out = [_passed(), _timeout_row()]
    for kind in ("flaky", "targeted"):
        kept, printed = _run_gate(
            passed_then_timed_out, timed_out=True, **{kind: True}
        )
        assert not kept and printed == "", printed
        assert (
            _reported_status(passed_then_timed_out, timed_out=True, **{kind: True})
            == Result.Status.OK
        )

    # The same downgrade with nothing but the synthetic row: best-effort SKIPPED.
    assert not _run_gate([_timeout_row()], flaky=True, timed_out=True)[0]
    assert (
        _reported_status([_timeout_row()], flaky=True, timed_out=True)
        == Result.Status.SKIPPED
    )

    # A run whose infrastructure relabel the bugfix-validation term does not cover stays green,
    # so the row being SKIPPED is not on its own a reason to keep the dump.
    assert not _run_gate([_passed(), _infra_row()], bugfix_labelled=False)[0]


def test_the_synthetic_timeout_row_is_what_the_non_failing_shapes_turn_on():
    """Mutation control: without the exclusion, every shape above flips to keeping the dump."""
    mutant = _gate_without_the_timeout_exclusion()
    flipped = [
        ([_timeout_row()], dict(flaky=True, timed_out=True)),
        ([_passed(), _timeout_row()], dict(flaky=True, timed_out=True)),
        ([_passed(), _timeout_row()], dict(targeted=True, timed_out=True)),
    ]
    for rows, kwargs in flipped:
        assert not _run_gate(rows, **kwargs)[0]
        assert _run_gate(rows, gate_source=mutant, **kwargs)[0]

    # And the mutation must leave the failing shapes alone, or it would prove nothing about
    # which term the arms above depend on.
    assert _run_gate([_passed(), _failed()], gate_source=mutant)[0]
    assert not _run_gate([_passed(), _passed()], gate_source=mutant)[0]


def test_a_run_that_produced_no_dump_attaches_no_path():
    """`dmesg_dumped` is the only witness that the file exists; an empty buffer is not one."""
    for rows, kwargs in (
        ([_passed(), _failed()], {}),
        ([], dict(flaky=True)),
        ([_passed(), _infra_row()], dict(bugfix_labelled=True)),
    ):
        assert _kept_the_record(*_run_gate(rows, **kwargs))
        kept, printed = _run_gate(rows, dumped=False, **kwargs)
        assert not kept
        # The lines still go to the job log, which is kept whatever happens to the files.
        assert KILL_SUMMARY in printed and f"  {KILL_LINE}" in printed, printed


def test_the_synthetic_row_name_has_one_spelling():
    """The row's construction, its strip and the gate's exclusion must agree on the name."""
    constructions = [
        node
        for node in ast.walk(ast.parse(JOB_SOURCE))
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "Result"
        and any(
            keyword.arg == "name"
            and isinstance(keyword.value, ast.Constant)
            and keyword.value.value == TIMEOUT_ROW_NAME
            for keyword in node.keywords
        )
    ]
    assert len(constructions) == 1, [node.lineno for node in constructions]

    exclusions = [
        ast.unparse(node)
        for node in ast.walk(GATE)
        if isinstance(node, ast.Compare) and ast.unparse(node).startswith("r.name !=")
    ]
    assert exclusions == [f"r.name != {TIMEOUT_ROW_NAME!r}"], exclusions
