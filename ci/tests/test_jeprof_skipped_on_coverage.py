"""Tests for the LLVM-coverage skip in `ClickHouseProc._get_jemalloc_profiles`.

`jeprof` symbolizes the whole ClickHouse binary, and on an instrumented
LLVM-coverage binary one render pass measured up to 2572 s against the
functional-test job's single 9000 s wall clock, so the two passes per server PID
could consume the remaining budget and get the job SIGKILLed during teardown,
discarding every result. The render is skipped there; the archiving is not, so
the raw `.heap` profiles still ship and can be rendered offline.
"""

import ast
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts import clickhouse_proc as module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.utils import Shell

FLAG = "is_llvm_coverage"
NPIDS = 3
TREE = ast.parse(Path(module.__file__).read_text())
COLLECTOR = next(
    n
    for n in ast.walk(TREE)
    if isinstance(n, ast.FunctionDef) and n.name == "_get_jemalloc_profiles"
)
GUARDS = [
    s
    for s in COLLECTOR.body
    if isinstance(s, ast.If) and ast.unparse(s.test) == f"self.{FLAG}"
]


def _checks(node, needle):
    """`Shell.check` calls under `node` whose command mentions `needle`."""
    return {
        ast.unparse(c)
        for c in ast.walk(node)
        if isinstance(c, ast.Call)
        and ast.unparse(c.func) == "Shell.check"
        and needle in ast.unparse(c)
    }


def _checks_at_top_level(fn, needle):
    """As `_checks`, but only calls that are direct statements of `fn`'s body."""
    return {
        c
        for s in fn.body
        for c in _checks(s, needle)
        if not isinstance(s, (ast.If, ast.For))
    }


def _run(monkeypatch, tmp_path, is_llvm_coverage):
    """Drive the collector on fake heap profiles, recording issued commands.

    `Shell.check` is wrapped rather than replaced, so the code under test keeps
    running its own predicates and the archive really appears on disk.
    """
    (tmp_path / "jemalloc_profiles").mkdir(parents=True)
    for pid in range(600, 600 + NPIDS):
        for n in (1, 2):
            f = (
                tmp_path
                / "jemalloc_profiles"
                / f"clickhouse.jemalloc.{pid}.{n}.m{n}.heap"
            )
            f.write_text("x")
    monkeypatch.setattr(module, "temp_dir", str(tmp_path))
    monkeypatch.setattr(module, "p_temp_dir", tmp_path)
    issued, real = [], Shell.check
    monkeypatch.setattr(
        Shell, "check", lambda c, *a, **kw: (issued.append(c), real(c, *a, **kw))[1]
    )
    proc = ClickHouseProc.__new__(ClickHouseProc)
    setattr(proc, FLAG, is_llvm_coverage)
    return issued, proc._get_jemalloc_profiles()


def test_the_guard_encloses_the_render_loop():
    """The flag is accepted, stored, read off `self`, and the guard is a direct
    statement of the function body enclosing every `jeprof` call. Asserted
    structurally: a guard merely present somewhere (nested inside the per-PID
    loop, say) still renders once per PID."""
    init = next(
        n
        for n in ast.walk(TREE)
        if isinstance(n, ast.FunctionDef) and n.name == "__init__"
    )
    assert FLAG in [a.arg for a in init.args.args]
    assert any(ast.unparse(s) == f"self.{FLAG} = {FLAG}" for s in init.body)
    assert COLLECTOR.decorator_list == [] and COLLECTOR.args.args[0].arg == "self"
    assert len(GUARDS) == 1, f"expected one top-level `if self.{FLAG}:` guard"
    renders = _checks(COLLECTOR, "jeprof")
    assert renders and renders == _checks(GUARDS[0], "jeprof")


def test_the_archive_is_unconditional():
    """Skipping the render is lossless only because the raw `.heap` files are
    archived anyway, so the archiving must not sit under ANY branch. Phrased
    over the function body rather than over the guard, so this arm reddens only
    for a misplaced archive and not for a misplaced guard."""
    archive = _checks(COLLECTOR, "jemalloc.tar.zst")
    assert len(archive) == 1, "expected exactly one archiving call"
    assert archive == _checks_at_top_level(COLLECTOR, "jemalloc.tar.zst")


def test_the_flag_is_wired_from_the_job():
    """A flag the job never sets leaves the render unconditional in production."""
    job = Path(module.__file__).parent.parent / "functional_tests.py"
    built = [
        c
        for c in ast.walk(ast.parse(job.read_text()))
        if isinstance(c, ast.Call) and ast.unparse(c.func) == "ClickHouseProc"
    ]
    assert len(built) == 1
    assert {k.arg: ast.unparse(k.value) for k in built[0].keywords}.get(FLAG) == FLAG


@pytest.mark.parametrize("coverage,renders", [(True, 0), (False, 2 * NPIDS)])
def test_only_a_coverage_build_skips_the_render(
    monkeypatch, tmp_path, coverage, renders
):
    """The `False` case is the positive control: without it these tests cannot
    tell "skips on coverage" from "skips always"."""
    issued, returned = _run(monkeypatch, tmp_path, is_llvm_coverage=coverage)
    assert len([c for c in issued if "jeprof " in c]) == renders
    assert len([c for c in issued if "jemalloc.tar.zst" in c]) == 1
    assert returned == [f"{tmp_path}/jemalloc.tar.zst"]
    assert (tmp_path / "jemalloc.tar.zst").exists()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
