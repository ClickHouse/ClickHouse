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
import tarfile
import types
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs import functional_tests as job_module
from ci.jobs.scripts import clickhouse_proc as module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.utils import Shell

FLAG = "is_llvm_coverage"
PER_TEST_FLAG = "is_per_test_coverage"
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

    Returns the basenames of the profiles written, so an assertion over the
    archive cannot drift from the fixture by re-deriving them.
    """
    (tmp_path / "jemalloc_profiles").mkdir(parents=True)
    written = set()
    for pid in range(600, 600 + NPIDS):
        for n in (1, 2):
            f = (
                tmp_path
                / "jemalloc_profiles"
                / f"clickhouse.jemalloc.{pid}.{n}.m{n}.heap"
            )
            f.write_text("x")
            written.add(f.name)
    monkeypatch.setattr(module, "temp_dir", str(tmp_path))
    monkeypatch.setattr(module, "p_temp_dir", tmp_path)
    issued, real = [], Shell.check
    monkeypatch.setattr(
        Shell, "check", lambda c, *a, **kw: (issued.append(c), real(c, *a, **kw))[1]
    )
    proc = ClickHouseProc.__new__(ClickHouseProc)
    setattr(proc, FLAG, is_llvm_coverage)
    return issued, proc._get_jemalloc_profiles(), written


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
    issued, returned, written = _run(monkeypatch, tmp_path, is_llvm_coverage=coverage)
    assert len([c for c in issued if "jeprof " in c]) == renders
    assert len([c for c in issued if "jemalloc.tar.zst" in c]) == 1
    assert returned == [f"{tmp_path}/jemalloc.tar.zst"]
    archive = tmp_path / "jemalloc.tar.zst"
    assert archive.exists()
    # Losslessness is the whole contract of skipping the render, so assert the
    # archive's contents and not merely that it appeared. Despite the name the
    # production command is `tar -czf`, i.e. gzip, which `tarfile` reads.
    # Compare basename SETS: a count is also satisfied by copies of one profile.
    with tarfile.open(archive) as tar:
        shipped = {Path(n).name for n in tar.getnames() if n.endswith(".heap")}
    assert shipped == written, f"profiles missing from the archive: {written - shipped}"


def _derive_flags(parameter):
    """Run `main`'s real option-parsing loop on a job parameter string.

    The loop is located structurally rather than by line number, and the
    module-level option constants it reads are taken from the module itself, so
    they cannot drift from a copy.
    """
    job_tree = ast.parse(Path(job_module.__file__).read_text())
    main = next(
        n for n in job_tree.body if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    loops = [
        s
        for s in main.body
        if isinstance(s, ast.For)
        and isinstance(s.target, ast.Name)
        and s.target.id == "to"
    ]
    assert (
        len(loops) == 1
    ), f"expected one `for to in ...` loop in main(), got {len(loops)}"
    loop = loops[0]
    # Seed exactly the names the loop reads; the `is_*` accumulators are read
    # off the AST too, so a new flag cannot silently become an undefined name.
    flags = sorted(
        {
            n.id
            for n in ast.walk(loop)
            if isinstance(n, ast.Name)
            and isinstance(n.ctx, ast.Store)
            and n.id.startswith("is_")
        }
    )
    assert FLAG in flags and PER_TEST_FLAG in flags
    ns = {
        "OPTIONS_TO_INSTALL_ARGUMENTS": job_module.OPTIONS_TO_INSTALL_ARGUMENTS,
        "OPTIONS_TO_TEST_RUNNER_ARGUMENTS": job_module.OPTIONS_TO_TEST_RUNNER_ARGUMENTS,
        "SELECTED_TESTS_OPTION": job_module.SELECTED_TESTS_OPTION,
        "config_installs_args": "",
        "runner_options": "",
        "batch_num": 0,
        "total_batches": 1,
        "args": types.SimpleNamespace(test=[], options=parameter),
        "test_options": [to.strip() for to in parameter.split(",")],
        **{f: False for f in flags},
    }
    exec(compile(ast.Module(body=[loop], type_ignores=[]), "<loop>", "exec"), ns)
    return ns[FLAG], ns[PER_TEST_FLAG]


@pytest.mark.parametrize(
    "parameter,coverage,per_test",
    [
        ("amd_llvm_coverage, ParallelReplicas, s3 storage, parallel", True, False),
        (
            "amd_llvm_coverage, old analyzer, s3 storage, DBReplicated, WasmEdge, sequential, 1/2",
            True,
            False,
        ),
        ("amd_llvm_coverage_per_test, per_test_coverage, 1/8", True, True),
        ("amd_asan_ubsan, db disk, distributed plan, sequential, 1/3", False, False),
        ("amd_binary_excluded_from_llvm", False, False),
    ],
)
def test_the_flag_is_derived_from_the_job_parameter(parameter, coverage, per_test):
    """The ctor wiring above says the flag reaches the collector; this says the
    right jobs compute it in the first place. Without it a change to the
    `amd_`-prefix/`"coverage" in to` selector leaves every arm green while every
    coverage job renders `jeprof` again. The two `False` rows are the positive
    controls: they distinguish "detects coverage" from "returns True always"."""
    assert _derive_flags(parameter) == (coverage, per_test)


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
