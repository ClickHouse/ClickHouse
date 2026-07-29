"""Build-configuration guard for the jemalloc safety-check lane.

`ENABLE_JEMALLOC_SAFETY_CHECKS` must define **two** jemalloc macros, and both are
load-bearing for the `WeeklyJemallocSafety` lane:

* `JEMALLOC_OPT_SAFETY_CHECKS` arms `config_opt_safety_checks`
  (`contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h:188`, asserted
  below), which gates `arena_ptr_array_flush_impl`'s sized-deallocation detector.
* `JEMALLOC_OPT_SIZE_CHECKS` arms `config_opt_size_checks` (same file, `:207`,
  asserted below), which is the **sole** gate on `maybe_check_alloc_ctx`
  (`jemalloc_internal_inlines_c.h:420-421`) - the check whose failure text is
  literally `"Internal heap corruption detected: mismatch in slab bit"`, i.e.
  exactly the union-view confusion this lane exists to catch.

The AST fuzzer job's runtime preflight
(`assert_jemalloc_safety_checks_armed` in `ci/jobs/ast_fuzzer_job.py`) can only
verify the first one: `config_opt_size_checks` has no mallctl, so it appears in
neither `contrib/jemalloc/src/ctl.c` nor `src/stats.c` and cannot be read out of the
built binary at all. Exposing one would mean patching `contrib/jemalloc`, which is a
submodule.

So the size gate is asserted here instead. Losing either macro leaves the lane green
while removing detection, so this file pins every place such a loss can happen:

* the effective build, via `assert_jemalloc_safety_macros_armed`
  (`ci/jobs/build_clickhouse.py`, run right after cmake configuration in the
  `amd_jemalloc_safety` build). It compiles a probe with a real jemalloc translation
  unit's own flags and `_Static_assert`s both `config_opt_*` booleans, so the compiler -
  not a parser - answers whether the gates are armed; and it scans every other compile
  line for the macros, which must reach no other translation unit. Both halves' cases are
  driven below, together with the layer's wiring: that this build type requests the option
  and that the build job really invokes the check;
* every *other* build, via `assert_jemalloc_safety_macros_absent` (same file, same point
  in the job). The option defaults to OFF, so those builds must carry neither macro -
  flipping that default would arm both gates in every x86-64 jemalloc build, release
  included. Emptiness is deliberately not a failure there: jemalloc is disabled outright
  under every sanitizer but UBSan, so those builds have no jemalloc entries at all;
* the platform headers the option can reach (x86-64 only, since the option refuses
  every other arch), where a bare `#undef` would silently cancel the `-D`;
* the compiled `jemalloc_preamble.h`, the sole place each `-D` is converted into the
  boolean the detector sites read, whose initializers are *evaluated* rather than
  searched for the macro's name - narrowing a condition to `JEMALLOC_DEBUG` alone,
  turning its `||` into `&&`, inverting it or swapping its arms would each disarm a
  gate with every other layer still green;
* the `CI Tests` cache digest, for the three files those assertions depend on (the
  platform headers, the compiled preamble, and the jemalloc cmake file - whose
  `ARCH_AMD64` guard decides *which* platform headers have to be checked) - so a commit
  changing any of them re-runs this file instead of being cache-skipped.

The option's own cmake text - that it defaults to OFF and passes both macros to
`_jemalloc` PRIVATE - is deliberately **not** guarded by a text model of
`contrib/jemalloc-cmake/CMakeLists.txt`: two attempts at one were removed for
false-failing on behaviour-identical spellings while passing on inactive text. What is
guarded instead is what the compiler computes for the build that requests the option,
what the flags say for the builds that do not, and - as a Python-level fact about the
`ci/` dicts rather than about cmake - that no other build type passes it.
"""

import argparse
import collections
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `ci/defs/defs.py` does `from praktika import ...` rather than
# `from ci.praktika import ...`, so the `ci/` directory itself must be on the path
# for `import praktika` to resolve to `ci/praktika`. CI configures this via the
# praktika runner (`PYTHONPATH=./ci:.`); we replicate it here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import ci.jobs.build_clickhouse as build_job
from ci.defs.defs import BuildTypes, ToolSet
from ci.defs.job_configs import JobConfigs
from ci.jobs.build_clickhouse import (
    BUILD_TYPE_TO_CMAKE,
    assert_jemalloc_macros_stay_private,
    assert_jemalloc_safety_macros_absent,
    assert_jemalloc_safety_macros_armed,
    jemalloc_probe_flags,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
JEMALLOC_CMAKE_REL = "./contrib/jemalloc-cmake/CMakeLists.txt"

# The header that actually gets compiled: `target_include_directories(_jemalloc SYSTEM
# PUBLIC include)` (CMakeLists.txt:177) precedes `PRIVATE "${LIBRARY_DIR}/include"`
# (:178), and the submodule's `jemalloc_preamble.h.in` is never `configure_file`d, so
# this ClickHouse-owned copy shadows it.
JEMALLOC_PREAMBLE_REL = (
    "./contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h"
)
JEMALLOC_PREAMBLE = REPO_ROOT / JEMALLOC_PREAMBLE_REL[2:]

# The image the `CI Tests` job runs in (`JobConfigs.ci_tests.run_in_docker`). This
# module's oracle cases compile, so that image has to carry the compiler; the assertion
# below is what keeps the package from being dropped again.
JOB_IMAGE_WITH_COMPILER_REL = "./ci/docker/integration/runner/Dockerfile"

OPTION_NAME = "ENABLE_JEMALLOC_SAFETY_CHECKS"
REQUIRED_MACROS = ("JEMALLOC_OPT_SAFETY_CHECKS", "JEMALLOC_OPT_SIZE_CHECKS")

# The option is x86-64 only (`ARCH_AMD64` guard in the cmake file), and cmake
# `configure_file`s exactly one `<prefix>/jemalloc/internal/jemalloc_internal_defs.h.in`
# chosen by OS x ARCH, so these are the headers a `#undef` could reach in practice.
REACHABLE_DEFS_HEADERS_GLOB = (
    "contrib/jemalloc-cmake/include_*_x86_64*/jemalloc/internal/"
    "jemalloc_internal_defs.h.in"
)
DEFS_HEADERS_DIGEST_ENTRY = (
    "./contrib/jemalloc-cmake/include_*/jemalloc/internal/jemalloc_internal_defs.h.in"
)


def _reachable_defs_headers() -> list[Path]:
    headers = sorted(REPO_ROOT.glob(REACHABLE_DEFS_HEADERS_GLOB))
    # A rename of the include directories must not silently make the header
    # assertions vacuous.
    assert headers, (
        f"no jemalloc platform headers matched {REACHABLE_DEFS_HEADERS_GLOB!r}; "
        "the include directory layout changed - re-derive the set the "
        f"{OPTION_NAME} option can reach"
    )
    return headers


# --- the effective build --------------------------------------------------------------
#
# What decides whether a gate is armed is the compiler, not the cmake text that produced
# the flags nor the flags read as text: a `-U` reaching the `_jemalloc` compile line from
# any other command cancels the `-D` (and `contrib/jemalloc-cmake/CMakeLists.txt:255`
# already passes a macro that way, via `target_compile_options`), the `-U` can arrive
# wrapped (`-Wp,-U`, `-Xpreprocessor -U`), and an `#undef` can arrive through one of the
# headers `jemalloc_preamble.h:4-54` includes before the `config_opt_*` booleans. So the
# build job compiles a probe with a real jemalloc entry's own flags and lets clang answer.
# `CMakeLists.txt:50` exports `compile_commands.json` unconditionally, so a configured
# tree always supplies those flags.
#
# The cmake text itself is deliberately not scanned: a regex model of it false-fails on
# behaviour-identical spellings (bare vs `-D` definitions, reflowed calls, `set(... 0)`)
# and passes on inactive text (`if (FALSE)`, `#[[ ]]`). A cmake-level fact worth asserting
# belongs either here - answered by the compiler - or as a Python-level fact about the
# `ci/` dicts, as `test_only_this_build_type_requests_the_option` below is.

JE = "/ClickHouse/contrib/jemalloc/src/arena.c"
JE2 = "/ClickHouse/contrib/jemalloc/src/jemalloc.c"
OTHER = "/ClickHouse/src/Interpreters/Context.cpp"
# A `.cc` sibling: the leak probe's language selection has to follow the entry, and the
# reduction has to drop this operand as surely as it drops jemalloc's `.c` one.
OTHER_CC = "/ClickHouse/contrib/orc/c++/src/Adaptor.cc"
# A C entry outside jemalloc, so the `c` arm of that selection is exercised too.
OTHER_C = "/ClickHouse/contrib/zlib-ng/adler32.c"
SAFETY, SIZE = (f"-D{macro}" for macro in REQUIRED_MACROS)
BOTH = f"{SAFETY} {SIZE}"
SPLIT_SAFETY, SPLIT_SIZE = (f"-D {macro}" for macro in REQUIRED_MACROS)

_CONFIG_FLAG_BLOCK_RE = re.compile(
    r"static const bool\s+(config_opt_(?:safety|size)_checks)\s*=.*?;", re.S
)


def _probe_include_root(tmp_path: Path) -> Path:
    """A minimal include tree whose `jemalloc_preamble.h` is the real one's two gates.

    The build job's probe includes `jemalloc/internal/jemalloc_preamble.h` and
    `_Static_assert`s both `config_opt_*` booleans. Compiling the real header needs a
    configured tree (its include chain reaches `configure_file`d and generated headers),
    which the `CI Tests` job does not have, so these cases stand up a header carrying the
    two initializers *copied out of the real file* - not hand-written, so a reshaped
    initializer cannot leave them exercising a stale shape.
    """
    text = re.sub(
        r"/\*.*?\*/", "", JEMALLOC_PREAMBLE.read_text(encoding="utf-8"), flags=re.S
    )
    blocks = {
        match.group(1): match.group(0) for match in _CONFIG_FLAG_BLOCK_RE.finditer(text)
    }
    assert set(blocks) == {"config_opt_safety_checks", "config_opt_size_checks"}, (
        f"{JEMALLOC_PREAMBLE_REL}: expected both `config_opt_*` initializers to extract; "
        f"got {sorted(blocks)}. Re-derive this fixture against the current header."
    )
    root = tmp_path / "include"
    (root / "jemalloc" / "internal").mkdir(parents=True, exist_ok=True)
    (root / "jemalloc" / "internal" / "jemalloc_preamble.h").write_text(
        # The real preamble gets `bool` from the headers it includes first; the
        # initializers are copied without them, so declare it here.
        "#include <stdbool.h>\n"
        + "\n".join(blocks[flag] for flag in sorted(blocks))
        + "\n",
        encoding="utf-8",
    )
    return root


def _probe_verdict(tmp_path: Path, flags: str, others=()) -> str | None:
    """The build job's verdict for a jemalloc TU compiled with `flags`, `None` if armed.

    The whole `compile_commands.json` path is exercised, so the flag reduction
    (`-o`/`-c`/depfile/operands dropped), the probe and the leak sweep's call site are all
    under test. `others` adds non-jemalloc `(file, flags)` entries for the leak direction.
    """
    root = _probe_include_root(tmp_path)
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": JE,
                    "command": (
                        f"{ToolSet.COMPILER_C} -I{root} {flags} -o out.o -c {JE}"
                    ),
                }
            ]
            + [
                {
                    "directory": str(tmp_path),
                    "file": other,
                    "command": f"{ToolSet.COMPILER_C} {other_flags} -c {other}",
                }
                for other, other_flags in others
            ]
        ),
        encoding="utf-8",
    )
    try:
        assert_jemalloc_safety_macros_armed(str(path))
    except AssertionError as error:
        return str(error)
    return None


def _running_in_ci():
    """Whether this pytest process is a CI job rather than a contributor's local run.

    Every generated workflow runs jobs as `praktika run ... --ci` (see the `CI Tests`
    step in `.github/workflows/pull_request.yml`), which leaves `LOCAL_RUN` false;
    `Runner.generate_local_run_environment` sets it true for a local `praktika run`.
    The environment is a file under `Settings.TEMP_DIR`, which is inside the directory
    the job's docker invocation mounts, so it is readable in the container.

    Fails **closed**: any problem reading the signal counts as CI, so this guard cannot
    evaporate merely because the signal could not be found. A tree carrying no
    environment file at all also reports false, which is the same conservative
    direction - relied on deliberately rather than by luck.
    """
    try:
        from ci.praktika.info import Info

        return not Info().is_local_run
    except Exception:
        return True


def _require_compiler_or_skip():
    """`compiler_probe`'s body, factored out so its own behaviour can be tested."""
    if shutil.which(ToolSet.COMPILER_C):
        return
    if _running_in_ci():
        pytest.fail(
            f"{ToolSet.COMPILER_C} is not on PATH in this CI job's image, so every "
            "compiler-oracle case in this module would skip - and praktika counts a "
            "skip as success (`Result.is_ok`, ci/praktika/result.py). This guard would "
            "then report success without having measured anything, which is the very "
            "failure it exists to catch, one layer up. Run this module in a job whose "
            f"image carries {ToolSet.COMPILER_C} (see JOB_IMAGE_WITH_COMPILER_REL)."
        )
    pytest.skip(f"{ToolSet.COMPILER_C} is not on PATH (local run)")


@pytest.fixture
def compiler_probe():
    """Skip when the compiler is absent locally; **fail** when it is absent in CI.

    These cases are this module's compiler oracle - what lets it answer questions by
    compiling rather than by parsing. A skip is silent and counts as success, so
    absence in CI is an error, not a skip.
    """
    _require_compiler_or_skip()


SAFETY_MACRO, SIZE_MACRO = REQUIRED_MACROS

# The probe carries two layers of assertion per gate, and which one fires is itself the
# answer to a distinct question, so the cases below say which they expect. MACRO_MISSING
# is the `#ifdef` layer: *our* macro is not on the compile line. BOOLEAN_MISSING is the
# `_Static_assert` layer: the preamble did not turn it into the boolean the detector sites
# read. `JEMALLOC_DEBUG` is exactly the shape that separates them - it satisfies both
# initializers on its own (`jemalloc_preamble.h:191`, `:208`), so the boolean is true
# while our macro is absent.
MACRO_MISSING = "{macro} is not defined on this compile line"
BOOLEAN_MISSING = "{macro} is not in effect"
BOTH_LAYERS = (MACRO_MISSING, BOOLEAN_MISSING)
MACRO_LAYER_ONLY = (MACRO_MISSING,)


@pytest.mark.parametrize(
    "label, flags, unarmed",
    [
        # Both `-D`s present: the probe compiles, so both gates are armed.
        ("both macros", BOTH, ()),
        # The forwarded `-U` spellings, which an argument scan of the compile line reads
        # as still defined while the `"mismatch in slab bit"` detector is gone - and
        # `config_opt_size_checks` has no mallctl, so nothing downstream can notice.
        (
            "-Wp,-U of the size macro",
            f"{BOTH} -Wp,-UJEMALLOC_OPT_SIZE_CHECKS",
            ((SIZE_MACRO, BOTH_LAYERS),),
        ),
        (
            "-Xpreprocessor -U of the size macro",
            f"{BOTH} -Xpreprocessor -U -Xpreprocessor JEMALLOC_OPT_SIZE_CHECKS",
            ((SIZE_MACRO, BOTH_LAYERS),),
        ),
        # A plain later `-U`: the last mention wins, as the preprocessor does.
        (
            "-D then a later -U",
            f"{BOTH} -UJEMALLOC_OPT_SIZE_CHECKS",
            ((SIZE_MACRO, BOTH_LAYERS),),
        ),
        # ... and the reverse order really is defined, so ordering is answered rather
        # than any `-U` being rejected on sight.
        ("-U then a later -D", f"{SAFETY} -UJEMALLOC_OPT_SIZE_CHECKS {SIZE}", ()),
        # The split spelling, this repo's own idiom (`cmake/target.cmake:3` is
        # `add_definitions(-D OS_LINUX)`), in both directions.
        ("split -D of both macros", f"{SPLIT_SAFETY} {SPLIT_SIZE}", ()),
        (
            "joined -D then a split -U",
            f"{BOTH} -U JEMALLOC_OPT_SIZE_CHECKS",
            ((SIZE_MACRO, BOTH_LAYERS),),
        ),
        # `-DMACRO=1` is a definition too, which is what `#ifdef` tests.
        ("value form of the size macro", f"{SAFETY} {SIZE}=1", ()),
        # Token-exact, as `#ifdef` is: a suffixed spelling defines nothing.
        (
            "suffixed lookalike",
            f"{SAFETY} {SIZE}_DISABLED",
            ((SIZE_MACRO, BOTH_LAYERS),),
        ),
        ("the option never passed at all", "", ((SAFETY_MACRO, BOTH_LAYERS),)),
        ("only the safety macro", SAFETY, ((SIZE_MACRO, BOTH_LAYERS),)),
        # `JEMALLOC_DEBUG` alone satisfies both `config_opt_*` initializers, so the
        # `_Static_assert` pair holds and only the `#ifdef` pair can see that neither
        # macro this build type promises is present. Without that pair the checker would
        # report our two macros as arming both gates while naming macros the compile line
        # does not carry - and `JEMALLOC_DEBUG` additionally arms jemalloc's internal
        # `assert`s, so the lane would not be the `amd_debug` build plus one option.
        (
            "only JEMALLOC_DEBUG",
            "-DJEMALLOC_DEBUG",
            ((SAFETY_MACRO, MACRO_LAYER_ONLY), (SIZE_MACRO, MACRO_LAYER_ONLY)),
        ),
        # The other side of that: `JEMALLOC_DEBUG` is not rejected on sight. Adding it on
        # top of both option macros is legitimate, and the invariant is that our macros
        # are present, not that a third is absent.
        ("JEMALLOC_DEBUG on top of both macros", f"{BOTH} -DJEMALLOC_DEBUG", ()),
        # And it does not cover for a missing macro: the size gate's boolean is armed
        # twice over here, while the safety macro is still absent.
        (
            "the size macro plus JEMALLOC_DEBUG",
            f"{SIZE} -DJEMALLOC_DEBUG",
            ((SAFETY_MACRO, MACRO_LAYER_ONLY),),
        ),
    ],
)
def test_the_probe_answers_whether_each_gate_is_armed(
    compiler_probe, tmp_path, label, flags, unarmed
):
    """The compiler's answer decides, and names both which gate and which layer.

    Every rejected shape leaves the lane fuzzing green with a detector gone.
    """
    verdict = _probe_verdict(tmp_path, flags)
    if not unarmed:
        assert verdict is None, f"{label}: expected both gates armed, got:\n{verdict}"
        return
    assert verdict is not None, (
        f"{label}: expected {[macro for macro, _ in unarmed]} to be reported unarmed, "
        "but the probe compiled"
    )
    for macro, layers in unarmed:
        for layer in layers:
            assert layer.format(macro=macro) in verdict, (
                f"{label}: the failure must report {layer.format(macro=macro)!r}; "
                f"got:\n{verdict}"
            )
        # The layer that must NOT fire is as much of the answer as the one that must:
        # a `JEMALLOC_DEBUG` build reaching here with the boolean reported unarmed would
        # mean the preamble no longer accepts it, and these cases would stop exercising
        # the `#ifdef` pair at all.
        for silent in set(BOTH_LAYERS) - set(layers):
            assert silent.format(macro=macro) not in verdict, (
                f"{label}: {silent.format(macro=macro)!r} must not be reported - only "
                f"{[layer.format(macro=macro) for layer in layers]}; got:\n{verdict}"
            )


@pytest.mark.parametrize(
    "label, command, must_go, must_stay",
    [
        # What cmake's Makefile generator emits, and the shape that matters: both `-MT`
        # and `-MF` take their operand as the next argv element, so dropping the flag
        # alone leaves the operand behind as a positional input file.
        (
            "cmake's Makefile generator shape",
            f"clang-21 {BOTH} -MD -MT arena.o -MF dep/arena.c.o.d -o arena.o -c {JE}",
            ("arena.o", "dep/arena.c.o.d", JE),
            (SAFETY, SIZE),
        ),
        (
            "-MQ and -MJ, the other operand-taking spellings",
            f"clang-21 {BOTH} -MQ arena.o -MJ compile.json -o arena.o -c {JE}",
            ("arena.o", "compile.json", JE),
            (SAFETY, SIZE),
        ),
        # `-MD`, `-MMD` and `-MP` take no operand, so the token after each must survive.
        # Consuming it on a `-M` prefix instead of on the flag's own grammar silently
        # eats a real flag - here the two `-D`s the whole check is about, which would
        # report an armed build as unarmed.
        (
            "the operandless spellings do not swallow the next flag",
            f"clang-21 -MD {SAFETY} -MP {SIZE} -MMD -o arena.o -c {JE}",
            ("arena.o", JE),
            (SAFETY, SIZE),
        ),
        # Every jemalloc TU is `.c`, so reading only that extension was enough for the
        # arming half - but the leak half probes this tree's other 13344 sources, and all
        # 130 of its prefilter candidates are `.cc`/`.cpp`. A left-in C++ operand costs
        # 2.8s instead of 0.2s per probe, and for a source cmake has not generated yet
        # (both guards run in the CMAKE stage, before BUILD) it fails the probe with a
        # `no such file or directory` that carries no `#error` marker - i.e. the
        # inconclusive branch, on a perfectly clean compile line.
        (
            "every compiled-source extension is dropped, not just jemalloc's .c",
            f"clang-21 {BOTH} -o x.o -c {OTHER_CC}",
            (OTHER_CC,),
            (SAFETY, SIZE),
        ),
        (
            "the remaining source extensions this tree really uses",
            "clang-21 -DKEEP -c a.cpp b.cxx c.c++ d.s e.S f.asm g.m h.mm i.C",
            (
                "a.cpp",
                "b.cxx",
                "c.c++",
                "d.s",
                "e.S",
                "f.asm",
                "g.m",
                "h.mm",
                "i.C",
            ),
            ("-DKEEP",),
        ),
        # ... and a flag operand that merely *looks* like a source must survive, or the
        # reduction silently drops a real flag. Measured over a configured tree: 0 of
        # 17284 entries carries a token ending in a source extension that is not its own
        # source file, so this is the property that keeps the extension list safe to widen.
        (
            "a path-shaped flag operand is not mistaken for a source",
            f"clang-21 {BOTH} -include /p/prefix.h -I/p/dir.c++ -o x.o -c {OTHER_CC}",
            (OTHER_CC,),
            (SAFETY, SIZE, "-include", "/p/prefix.h", "-I/p/dir.c++"),
        ),
    ],
)
def test_the_flag_reduction_leaves_no_stray_operand(label, command, must_go, must_stay):
    """A depfile operand kept as a positional input turns a PASS into a misleading FAIL.

    `-o` is consumed as a pair, and the operand-taking depfile flags must be too: clang
    reads a leftover path as an input file and reports `no such file or directory`, which
    the checker then presents as "the gates are not armed" - pointing at entirely the
    wrong cause. The reverse mistake costs the same in the other direction (a flag dropped
    that should have been kept changes how the probe compiles), so both are pinned here.
    """
    flags = jemalloc_probe_flags(command)
    for stray in must_go:
        assert stray not in flags, f"{label}: {stray!r} must not survive the reduction"
    for kept in must_stay:
        assert kept in flags, f"{label}: {kept!r} must survive the reduction"


@pytest.mark.parametrize(
    "label, macro_flags, unarmed",
    [
        # Armed plus depfile flags: a stray operand would make this fail with a `no such
        # file` message while both gates really are armed.
        ("both macros", BOTH, ()),
        # ... and the pairing must not be implemented by swallowing probe errors: a
        # genuinely unarmed line with the same depfile flags must still be reported.
        ("neither macro", "", ((SAFETY_MACRO, BOTH_LAYERS),)),
    ],
)
def test_depfile_flags_do_not_change_the_probe_verdict(
    compiler_probe, tmp_path, label, macro_flags, unarmed
):
    """cmake's Makefile generator emits `-MD -MT <target> -MF <path>` on every line."""
    flags = f"{macro_flags} -MD -MT arena.o -MF dep/arena.c.o.d".strip()
    verdict = _probe_verdict(tmp_path, flags)
    if not unarmed:
        assert verdict is None, f"{label}: expected both gates armed, got:\n{verdict}"
        return
    assert (
        verdict is not None
    ), f"{label}: expected the probe to report the gates unarmed"
    assert "no such file or directory" not in verdict, (
        f"{label}: the failure must be about the macros, not a stray depfile operand "
        f"reaching clang as an input file; got:\n{verdict}"
    )
    for macro, layers in unarmed:
        for layer in layers:
            assert layer.format(macro=macro) in verdict, (
                f"{label}: the failure must report {layer.format(macro=macro)!r}; "
                f"got:\n{verdict}"
            )


def test_every_distinct_flag_set_is_probed(compiler_probe, tmp_path):
    """One jemalloc TU of several carrying different flags must not go unprobed.

    The flags of interest are per-target, so probing one entry per *distinct* flag set is
    what makes the compile affordable; keying on the first entry alone would leave a
    per-file divergence silently unchecked.
    """
    root = _probe_include_root(tmp_path)
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": source,
                    "command": f"{ToolSet.COMPILER_C} -I{root} {flags} -o out.o -c {source}",
                }
                for source, flags in ((JE, BOTH), (JE2, SAFETY))
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError) as raised:
        assert_jemalloc_safety_macros_armed(str(path))
    assert "JEMALLOC_OPT_SIZE_CHECKS is not in effect" in str(raised.value)
    assert JE2 in str(
        raised.value
    ), f"the failure must name the diverging translation unit; got:\n{raised.value}"


def test_the_probe_reports_both_gates_when_neither_is_armed(compiler_probe, tmp_path):
    """With no `-D` at all both `_Static_assert`s must be reported, not just the first."""
    verdict = _probe_verdict(tmp_path, "-ferror-limit=0")
    assert verdict is not None
    for macro in REQUIRED_MACROS:
        assert (
            f"{macro} is not in effect" in verdict
        ), f"the failure must name {macro}; got:\n{verdict}"


def test_an_undef_arriving_through_an_include_disarms_a_gate(compiler_probe, tmp_path):
    """The class the flags cannot show: `jemalloc_preamble.h` includes before it decides.

    Its `config_opt_*` booleans sit at `:188` / `:207`, after the includes at `:4-54`, so
    an `#undef` reaching the preamble through any of them cancels the `-D` with the
    compile line unchanged. This is why the layer is a compile and not a bigger parser.
    """
    undef = tmp_path / "undef.h"
    undef.write_text("#undef JEMALLOC_OPT_SIZE_CHECKS\n", encoding="utf-8")
    verdict = _probe_verdict(tmp_path, f"{BOTH} -include {undef}")
    assert (
        verdict is not None and "JEMALLOC_OPT_SIZE_CHECKS is not in effect" in verdict
    ), f"an `#undef` arriving through an include must disarm the size gate; got:\n{verdict}"


def test_a_probe_that_cannot_run_fails_closed(tmp_path):
    """An inconclusive probe must never pass: no compiler, no verdict."""
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": JE,
                    "command": f"/nonexistent/clang {BOTH} -c {JE}",
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="could not be run at all"):
        assert_jemalloc_safety_macros_armed(str(path))


def test_no_jemalloc_translation_unit_fails_closed(tmp_path):
    """A rename of jemalloc's source layout must not pass vacuously."""
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [{"directory": str(tmp_path), "file": OTHER, "command": "clang-21 -c x.c"}]
        ),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="translation unit"):
        assert_jemalloc_safety_macros_armed(str(path))


def test_missing_compile_commands_fails_closed(tmp_path):
    """No exported compile commands means the question cannot be answered."""
    with pytest.raises(AssertionError, match="is missing"):
        assert_jemalloc_safety_macros_armed(str(tmp_path / "compile_commands.json"))


# --- the macros must stay PRIVATE to jemalloc -----------------------------------------
#
# The other direction, and - since r15 - the other half that asks the compiler. It used to
# be a flag scan, on the grounds that a probe per entry over ~17k is not affordable; the
# scan was then caught missing a real definition four times running, each time in a
# strictly narrower spelling of the previous one (`-D X` split, `-Wp,`/`-Xpreprocessor`,
# `-Xclang`). The defect was never the missing spelling: a scanner is only ever as complete
# as the list of forms someone thought of, while `-Xclang -D`, an `@response` file and an
# `-include`d header all define just as plainly. So the verdict moved onto the compiler and
# the affordability argument moved onto `_may_define_either`, which admits only the entries
# that could possibly carry a definition - 130 of a configured tree's 17217 non-jemalloc
# entries, ~27s of probing against the ~40 minutes of compiling that follows.
#
# The cases below therefore assert the verdict the COMPILER gives, computed in the test by
# running it, rather than a hardcoded expectation. Hardcoding is what let the scan's answer
# and clang's answer drift apart in the first place.


def _macros_stay_private(entries, directory="/b") -> bool:
    """Whether the build job's leak sweep accepts these `(file, flags)` pairs."""
    try:
        assert_jemalloc_macros_stay_private(
            [
                {
                    "directory": directory,
                    "file": f,
                    "command": f"{ToolSet.COMPILER_C} -c {flags} {f}",
                }
                for f, flags in entries
            ]
        )
    except AssertionError:
        return False
    return True


def _compiler_defines(tmp_path, flags, macro) -> bool:
    """Whether the real compiler leaves `macro` defined for `flags`.

    The oracle for every leak case below: the expectation is measured, not written down, so
    a case cannot silently encode the scanner's opinion instead of the compiler's.
    """
    source = tmp_path / "oracle.c"
    source.write_text(
        f"#ifdef {macro}\nint defined_ok;\n#else\n#error not defined\n#endif\n",
        encoding="utf-8",
    )
    return (
        subprocess.run(
            [ToolSet.COMPILER_C, "-fsyntax-only", *shlex.split(flags), str(source)],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            check=False,
        ).returncode
        == 0
    )


# Every spelling that reaches the leak half, each paired with the macro it decides. The
# expectation is not written here: `test_the_leak_sweep_agrees_with_the_compiler` runs the
# compiler on each and requires the sweep to say the same, so a spelling cannot be listed
# as "a leak" while clang disagrees, nor pass because the scanner happens to miss it.
_LEAK_SPELLINGS = [
    ("clean", "", REQUIRED_MACROS[0]),
    ("joined -D", SAFETY, REQUIRED_MACROS[0]),
    ("split -D", SPLIT_SAFETY, REQUIRED_MACROS[0]),
    ("-Wp,-D", f"-Wp,-D{REQUIRED_MACROS[0]}", REQUIRED_MACROS[0]),
    ("-Wp,-D,MACRO", f"-Wp,-D,{REQUIRED_MACROS[1]}", REQUIRED_MACROS[1]),
    (
        "-Xpreprocessor -D -Xpreprocessor MACRO",
        f"-Xpreprocessor -D -Xpreprocessor {REQUIRED_MACROS[0]}",
        REQUIRED_MACROS[0],
    ),
    # The r15 spelling: `clang-21` honours it, and the flag scan reported it as *absent*,
    # so a macro that really did reach a non-jemalloc TU was accepted.
    (
        "-Xclang -D -Xclang MACRO",
        f"-Xclang -D -Xclang {REQUIRED_MACROS[0]}",
        REQUIRED_MACROS[0],
    ),
    ("-Xclang -DMACRO", f"-Xclang -D{REQUIRED_MACROS[0]}", REQUIRED_MACROS[0]),
    # Split across the two forwarding mechanisms, which feed one argv.
    (
        "-Wp,-D plus -Xpreprocessor operand",
        f"-Wp,-D -Xpreprocessor {REQUIRED_MACROS[1]}",
        REQUIRED_MACROS[1],
    ),
    # A forwarded flag beats a plain one whichever was written first, because the driver
    # emits every plain `-D`/`-U` before any forwarded one - so both of these are decided
    # by the forwarded member, the opposite of a left-to-right reading.
    (
        "plain -D cancelled by a forwarded -U",
        f"{SAFETY} -Wp,-U{REQUIRED_MACROS[0]}",
        REQUIRED_MACROS[0],
    ),
    (
        "forwarded -D not cancelled by a later plain -U",
        f"-Wp,-D{REQUIRED_MACROS[0]} -U{REQUIRED_MACROS[0]}",
        REQUIRED_MACROS[0],
    ),
    (
        "forwarded -D cancelled by a later forwarded -U",
        f"-Wp,-D{REQUIRED_MACROS[0]} -Wp,-U{REQUIRED_MACROS[0]}",
        REQUIRED_MACROS[0],
    ),
    ("joined -D cancelled by a joined -U", f"{SAFETY} -U{REQUIRED_MACROS[0]}", REQUIRED_MACROS[0]),
    # Must-not-fire controls: tokens that carry the macro's name but define nothing.
    (
        "the name inside a -Wl, operand",
        f"-Wl,-rpath,/opt/{REQUIRED_MACROS[0]}/lib",
        REQUIRED_MACROS[0],
    ),
    (
        "a suffixed lookalike",
        f"{SIZE}_DISABLED",
        REQUIRED_MACROS[1],
    ),
    ("unrelated forwarded flags", "-Wp,-MD -Xpreprocessor -v", REQUIRED_MACROS[0]),
]


@pytest.mark.parametrize("label, flags, macro", _LEAK_SPELLINGS)
def test_the_leak_sweep_agrees_with_the_compiler(
    compiler_probe, tmp_path, label, flags, macro
):
    """The sweep's verdict must be the compiler's, for every spelling.

    This is the assertion the four rounds of missed spellings were failing: each fix taught
    the scanner one more form, and the next form was found in the same afternoon. Here the
    expectation is *measured* - the compiler is run on the same flags - so a spelling the
    sweep does not understand fails this test instead of shipping.
    """
    defined = _compiler_defines(tmp_path, flags, macro)
    accepted = _macros_stay_private([(JE, BOTH), (OTHER, flags)], directory=str(tmp_path))
    assert accepted is not defined, (
        f"{label}: the compiler leaves {macro} "
        f"{'defined' if defined else 'undefined'} for {flags!r}, so the leak sweep must "
        f"{'reject' if defined else 'accept'} the entry; it "
        f"{'accepted' if accepted else 'rejected'} it"
    )


def test_a_jemalloc_entry_carrying_both_macros_is_not_a_leak(compiler_probe, tmp_path):
    """The sweep skips jemalloc's own sources; that is the whole point of the macros."""
    assert _macros_stay_private(
        [(JE, BOTH), (JE2, BOTH), (OTHER, "")], directory=str(tmp_path)
    ), "jemalloc's own translation units are where the macros belong"


@pytest.mark.parametrize(
    "file, std",
    [(OTHER_CC, "-std=c++23"), (OTHER_C, "-std=c11")],
)
def test_the_leak_probe_follows_the_entrys_language(compiler_probe, tmp_path, file, std):
    """A C++ entry must be probed as C++ and a C entry as C.

    `-std=c++23` under `-x c` is rejected outright (`invalid argument '-std=c++23' not
    allowed with 'C'`), and that failure carries no `#error` marker - so probing a `.cc`
    entry as C raises *inconclusive* on a perfectly clean compile line, i.e. reds an
    ordinary build. Every real candidate in a configured tree is `.cc`/`.cpp` and carries
    `-std=c++23`, so this is the common case, not an exotic one.

    ⚠️ The must-accept arm has to be a line the prefilter actually **admits**, or the probe
    never runs and the case is vacuous whatever language it would have chosen. An empty
    pre-included header is the cheapest such line: it makes the entry a candidate without
    defining anything.
    """
    header = tmp_path / "empty-prefix.h"
    header.write_text("/* defines nothing */\n", encoding="utf-8")
    candidate = f"{std} -include {header}"
    assert build_job._may_define_either(
        f"{ToolSet.COMPILER_C} -c {candidate} {file}"
    ), "the must-accept arm must be probed, or this case cannot see the language at all"
    assert _macros_stay_private([(JE, BOTH), (file, candidate)], directory=str(tmp_path)), (
        f"{file} compiled with {candidate!r} carries neither macro, so it must be accepted "
        "- if it is not, the probe is using the wrong language for this entry"
    )
    assert not _macros_stay_private(
        [(JE, BOTH), (file, f"{candidate} {SAFETY}")], directory=str(tmp_path)
    ), f"{file} really does carry {REQUIRED_MACROS[0]}, so it must be reported"


def test_a_leak_arriving_through_a_response_file_is_caught(compiler_probe, tmp_path):
    """A `-D` inside an `@response` file defines without its name appearing on the line.

    One of the two indirect routes `_may_define_either` admits, and the reason a
    macro-name-only prefilter would be incomplete.
    """
    response = tmp_path / "flags.rsp"
    response.write_text(f"-D{REQUIRED_MACROS[0]}\n", encoding="utf-8")
    assert not _macros_stay_private(
        [(JE, BOTH), (OTHER, f"@{response}")], directory=str(tmp_path)
    ), "a -D expanded out of a response file still reaches the translation unit"


def test_a_leak_arriving_through_a_pre_included_header_is_caught(
    compiler_probe, tmp_path
):
    """The other indirect route: `-include` a header that `#define`s the macro."""
    header = tmp_path / "prefix.h"
    header.write_text(f"#define {REQUIRED_MACROS[1]} 1\n", encoding="utf-8")
    assert not _macros_stay_private(
        [(JE, BOTH), (OTHER, f"-include {header}")], directory=str(tmp_path)
    ), "a macro defined by a pre-included header still reaches the translation unit"


@pytest.mark.parametrize(
    "label, flags",
    [
        ("the macro's own name, in any spelling", f"-Xclang -D -Xclang {REQUIRED_MACROS[0]}"),
        ("a response file", "@flags.rsp"),
        ("a pre-included header", "-include prefix.h"),
        ("-imacros, the other pre-inclusion flag", "-imacros prefix.h"),
    ],
)
def test_the_prefilter_admits_every_route_a_definition_can_take(label, flags):
    """`_may_define_either` must admit every command on which a definition is possible.

    The load-bearing half of the seam: entries it rejects are accepted **without a probe**,
    so a route it does not know about passes silently - which is precisely the failure mode
    the compile probe was introduced to end. A future exotic spelling has to fail here.
    """
    assert build_job._may_define_either(
        f"{ToolSet.COMPILER_C} {flags} -c {OTHER}"
    ), f"{label}: {flags!r} can carry a definition, so it must be probed"


@pytest.mark.parametrize(
    "label, flags",
    [
        ("an ordinary compile line", "-O2 -std=c++23 -DSOME_OTHER_MACRO"),
        # Deliberately rejected: a forwarding token is only interesting when a macro name
        # is also present, which the first clause already covers. Listing these would
        # re-create the spelling enumeration the seam exists to delete.
        ("a forwarding token with no macro name", "-Wp,-MD -Xpreprocessor -v -Xclang -O2"),
        ("a similarly-named but different macro", "-DJEMALLOC_OPT_OTHER_CHECKS"),
    ],
)
def test_the_prefilter_rejects_lines_that_cannot_define(label, flags):
    """... and it must reject the rest, or the affordability argument is gone.

    A prefilter that admits everything is a per-entry compile over ~17k entries.
    """
    assert not build_job._may_define_either(
        f"{ToolSet.COMPILER_C} {flags} -c {OTHER}"
    ), f"{label}: {flags!r} cannot define either macro, so probing it is wasted"


@pytest.mark.parametrize(
    "label, flags",
    [
        ("the name inside a -I path", f"-I/opt/{REQUIRED_MACROS[0]}/include"),
        ("the name inside a -Wl, operand", f"-Wl,-rpath,/opt/{REQUIRED_MACROS[0]}/lib"),
        ("a suffixed lookalike", f"-D{REQUIRED_MACROS[0]}_DISABLED"),
    ],
)
def test_the_prefilter_over_admits_rather_than_narrowing_unsoundly(label, flags):
    """A line merely *mentioning* a macro name is probed, and that is deliberate.

    The obvious narrowing - require the name to appear as a whole identifier - is
    **unsound**, because the spelling that matters most has no left word boundary:
    in `-DJEMALLOC_OPT_SAFETY_CHECKS` the character before the name is `D`. A
    word-boundary prefilter therefore rejects the plainest real definition there is, which
    is far worse than probing a path that happens to contain the name.

    So the prefilter is deliberately one-sided: it may admit a line that defines nothing,
    and the compiler - never this helper - decides. The cost is nil in practice (measured
    over a configured tree: 0 of 17217 non-jemalloc entries mention either macro at all),
    and the corresponding must-accept verdicts are asserted against the real compiler in
    `test_the_leak_sweep_agrees_with_the_compiler`.
    """
    assert build_job._may_define_either(f"{ToolSet.COMPILER_C} {flags} -c {OTHER}"), (
        f"{label}: narrowing the prefilter to exclude {flags!r} would also exclude the "
        "joined -D spelling, which really defines"
    )


def test_the_prefilter_admits_nothing_in_a_real_tree_it_need_not(compiler_probe):
    """Every candidate the prefilter admits must be one the tree really could define on.

    Asserted as the two counts the affordability argument rests on: clause 1 (the macro's
    own name) admits nothing in a clean tree, and the indirect clauses admit only the
    entries that genuinely pre-include or expand flags.
    """
    clean = f"{ToolSet.COMPILER_C} -O2 -DFOO=1 -I/x -o a.o -c {OTHER}"
    assert not build_job._may_define_either(clean)
    for token in ("@x.rsp", "-include x.h", "-imacros x.h", f"-D{REQUIRED_MACROS[0]}"):
        assert build_job._may_define_either(
            f"{ToolSet.COMPILER_C} -O2 {token} -o a.o -c {OTHER}"
        ), f"{token!r} must be admitted"


def test_an_inconclusive_leak_probe_raises_without_claiming_a_leak(tmp_path):
    """A probe that cannot run must not pass - and must not be reported as a leak either.

    The distinction the fail-closed rule turns on: a missing compiler, a not-yet-generated
    source or an unrelated diagnostic all exit nonzero without either `#error` marker, and
    calling that a leak would blame the macros for someone else's broken compile line.
    """
    with pytest.raises(AssertionError) as raised:
        assert_jemalloc_macros_stay_private(
            [
                {
                    "directory": str(tmp_path),
                    "file": OTHER,
                    "command": f"/nonexistent/clang -include x.h -c {OTHER}",
                }
            ]
        )
    message = str(raised.value)
    assert "inconclusive" in message, f"the failure must say so; got:\n{message}"
    assert (
        "reaches" not in message
    ), f"an inconclusive probe must not be reported as a leak; got:\n{message}"


def test_an_unrelated_compile_error_is_inconclusive_not_a_leak(compiler_probe, tmp_path):
    """The same rule for a real compiler failing for a reason of its own.

    A source cmake has not generated yet is exactly this shape, and both guards run in the
    CMAKE stage, before BUILD - so on an ordinary build this is the branch a left-in source
    operand would take, on 9 of a configured tree's entries.
    """
    with pytest.raises(AssertionError) as raised:
        assert_jemalloc_macros_stay_private(
            [
                {
                    "directory": str(tmp_path),
                    "file": OTHER,
                    "command": (
                        f"{ToolSet.COMPILER_C} -include "
                        f"{tmp_path / 'absent.h'} -c {OTHER}"
                    ),
                }
            ]
        )
    message = str(raised.value)
    assert "inconclusive" in message and "reaches" not in message, (
        "a missing pre-included header is inconclusive, not a leak; got:\n" f"{message}"
    )


def test_the_armed_check_also_runs_the_leak_sweep(compiler_probe, tmp_path):
    """The two halves must both run: the cases above drive the sweep directly.

    Without this, dropping the sweep's call site leaves it dead code while every leak case
    still passes and the macros reach every dependent translation unit.
    """
    verdict = _probe_verdict(tmp_path, BOTH, others=[(OTHER, BOTH)])
    assert verdict is not None and "non-jemalloc translation units" in verdict, (
        "a leak alongside an armed jemalloc TU must be reported; got:\n" f"{verdict}"
    )


# The negative direction, decided by the compiler. Each row is a way to define a macro on a
# compile line; the checker must CATCH every one, and must ACCEPT a line that defines
# neither. The two controls are what make the table meaningful - without the `-D` row a
# blanket-catch would pass it, and without the clean row a blanket-catch would too.
#
# `-Xclang -D -Xclang <M>` is the row this table exists for: it is the spelling that a
# parse of the compile line reported as "absent" while clang defined it, so the checker
# printed "neither macro is defined" on a line where one was. Adding `-Xclang` to a
# spelling list would have been the fourth pattern on a chain that had already eaten five
# rounds, so the parse is gone instead and the compiler answers.
_DEFINING_FORMS = [
    ("-D<M> (control)", f"-D{REQUIRED_MACROS[0]}"),
    ("-D <M> split", f"-D {REQUIRED_MACROS[0]}"),
    ("-D<M>=1 valued", f"-D{REQUIRED_MACROS[0]}=1"),
    ("-Wp,-D<M>", f"-Wp,-D{REQUIRED_MACROS[0]}"),
    ("-Wp,-D,<M>", f"-Wp,-D,{REQUIRED_MACROS[0]}"),
    (
        "-Xpreprocessor -D -Xpreprocessor <M>",
        f"-Xpreprocessor -D -Xpreprocessor {REQUIRED_MACROS[0]}",
    ),
    ("-Xclang -D -Xclang <M>", f"-Xclang -D -Xclang {REQUIRED_MACROS[0]}"),
    ("-Xclang -D<M>", f"-Xclang -D{REQUIRED_MACROS[0]}"),
    # The second macro, so neither is the only one the checker can see.
    ("-D<SIZE>", f"-D{REQUIRED_MACROS[1]}"),
]

_NON_DEFINING_FORMS = [
    ("clean (control)", ""),
    ("an unrelated -D", "-DUNRELATED=1"),
    # Cancelled: the compiler leaves it undefined, so this build does not carry it.
    ("-D<M> then -U<M>", f"-D{REQUIRED_MACROS[0]} -U{REQUIRED_MACROS[0]}"),
    # A suffixed lookalike, which a substring test would misread as our macro.
    ("a suffixed lookalike", f"-D{REQUIRED_MACROS[1]}_DISABLED"),
    # The name as a linker operand rather than a definition.
    (
        "the name inside a -Wl, operand",
        f"-Wl,-rpath,/opt/{REQUIRED_MACROS[0]}/lib",
    ),
]


def _absent_verdict(tmp_path, flags):
    """`assert_jemalloc_safety_macros_absent`'s verdict for one jemalloc entry."""
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": JE,
                    "command": f"{ToolSet.COMPILER_C} {flags} -o out.o -c {JE}",
                }
            ]
        ),
        encoding="utf-8",
    )
    try:
        assert_jemalloc_safety_macros_absent(str(path))
    except AssertionError as error:
        return str(error)
    return None


@pytest.mark.parametrize("label, flags", _DEFINING_FORMS)
def test_the_absent_check_catches_every_defining_spelling(
    compiler_probe, tmp_path, label, flags
):
    """Every way of defining a macro must be caught, whatever the spelling.

    Asserted against what the **compiler** does with the same flags, so a row cannot
    quietly stop being a definition and leave the case asserting nothing.
    """
    probe = tmp_path / "defines.c"
    probe.write_text(
        "".join(
            f"#ifdef {macro}\nint defined_{index};\n#endif\n"
            for index, macro in enumerate(REQUIRED_MACROS)
        )
        + "int probe_ok;\n",
        encoding="utf-8",
    )
    compiled = subprocess.run(
        [ToolSet.COMPILER_C, "-fsyntax-only", "-dM", "-E", *shlex.split(flags), str(probe)],
        capture_output=True,
        text=True,
        check=False,
    )
    defined = any(f"define {macro}" in compiled.stdout for macro in REQUIRED_MACROS)
    assert defined, (
        f"{label}: the compiler does not actually define either macro with {flags!r}, so "
        "this row asserts nothing; re-derive it"
    )

    verdict = _absent_verdict(tmp_path, flags)
    assert verdict is not None, (
        f"{label}: clang defines the macro with {flags!r}, but the absent check accepted "
        "the line - so a build that did not request the option would be reported as "
        "carrying neither macro while one is armed"
    )
    assert "did not request" in verdict, (
        f"{label}: the failure must say this build did not request the option; got "
        f"{verdict}"
    )


@pytest.mark.parametrize("label, flags", _NON_DEFINING_FORMS)
def test_the_absent_check_accepts_a_line_defining_neither(
    compiler_probe, tmp_path, label, flags
):
    """The other direction: a clean line must pass, or the check catches everything.

    Without these the catching cases above would be satisfied by a checker that simply
    always fails.
    """
    assert _absent_verdict(tmp_path, flags) is None, (
        f"{label}: {flags!r} defines neither macro, so the absent check must accept it"
    )


def test_the_absent_check_rejects_an_inconclusive_probe(compiler_probe, tmp_path):
    """A probe that fails for an unrelated reason is not an answer of "absent".

    Reading it as one would let a flipped default through on any build whose compile
    lines changed shape - the same fail-closed reasoning as the missing-file case.
    """
    verdict = _absent_verdict(tmp_path, "-I/definitely/missing --nonexistent-flag")
    assert verdict is not None and "inconclusive" in verdict, (
        "a probe failing without reporting either macro must raise as inconclusive; got "
        f"{verdict}"
    )


def test_the_absent_check_probes_a_cxx_entry_as_cxx(compiler_probe, tmp_path):
    """The probe's language must follow the entry, or a C++ entry reads as inconclusive.

    Today `_jemalloc`'s sources are all `.c` (`jemalloc_cpp.cpp` exists in the submodule
    but is not in the cmake `SRCS`), so this clause is a guard against that changing rather
    than a description of the tree. It is pinned because getting it wrong is silent in the
    dangerous direction: a `.cpp` entry compiled as C fails for reasons of its own, and a
    probe that fails without naming a macro is inconclusive - which this check raises on,
    so the whole build's guard would red with a misleading cause.

    Driven with C++-only flags, so a C compile cannot pass: `-std=c++20` is rejected by
    the C frontend.
    """
    path = tmp_path / "compile_commands.json"
    cxx = "/ClickHouse/contrib/jemalloc/src/jemalloc_cpp.cpp"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": cxx,
                    "command": f"{ToolSet.COMPILER_C} -std=c++20 -o out.o -c {cxx}",
                }
            ]
        ),
        encoding="utf-8",
    )
    # Accepted, not inconclusive: the entry defines neither macro, and it must be compiled
    # as C++ for that to be answerable at all.
    assert_jemalloc_safety_macros_absent(str(path))

    # And the same entry really does carry a definition when one is on its line, so the
    # case above is not passing merely because the probe never ran.
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": cxx,
                    "command": f"{ToolSet.COMPILER_C} -std=c++20 {SAFETY} -o out.o "
                    f"-c {cxx}",
                }
            ]
        ),
        encoding="utf-8",
    )
    with pytest.raises(AssertionError, match="did not request"):
        assert_jemalloc_safety_macros_absent(str(path))


def test_the_absent_check_probes_one_entry_per_distinct_flag_set(
    compiler_probe, tmp_path, capsys
):
    """Cost is one compile per distinct flag set, not per entry.

    A non-safety build has ~67 jemalloc entries sharing one flag set, so deduping is what
    makes asking the compiler affordable here. The count is printed, so a future per-file
    divergence is visible rather than silently unprobed.
    """
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": f"/ClickHouse/contrib/jemalloc/src/unit{index}.c",
                    "command": f"{ToolSet.COMPILER_C} -DSHARED=1 -o out{index}.o "
                    f"-c /ClickHouse/contrib/jemalloc/src/unit{index}.c",
                }
                for index in range(5)
            ]
        ),
        encoding="utf-8",
    )
    assert_jemalloc_safety_macros_absent(str(path))
    printed = capsys.readouterr().out
    assert "5 jemalloc translation units" in printed, printed
    assert "1 distinct flag set(s) probed" in printed, (
        "five entries sharing one flag set must cost one probe, and the count must be "
        f"reported; got {printed!r}"
    )


# --- the layer's own wiring -----------------------------------------------------------
#
# The cases above drive `assert_jemalloc_safety_macros_armed` directly, so they stay
# green if the build job stops calling it or if the build type stops requesting the
# option - in either case the whole layer disappears while every assertion passes.


def test_only_this_build_type_requests_the_option():
    """No other build type may pass the option.

    The probe answers what the compiler computes for the build that requests it, so the
    default-off contract is only guarded here: copying the flag into another build type is
    the plausible accident, and it would arm both gates in a build that never asked.
    """
    requesting = sorted(
        build_type
        for build_type, command in BUILD_TYPE_TO_CMAKE.items()
        if f"-D{OPTION_NAME}" in command
    )
    assert requesting == [BuildTypes.AMD_JEMALLOC_SAFETY], (
        f"only {BuildTypes.AMD_JEMALLOC_SAFETY} may pass -D{OPTION_NAME}; "
        f"{requesting} do"
    )


def test_the_lane_is_amd_debug_plus_only_the_safety_option():
    """The build type must request the option, and differ from `amd_debug` by nothing else.

    Both halves matter: without the option there is no `-D` to assert, and any other
    difference would make the lane something other than the `amd_debug` fuzz session the
    two confirmed SIGSEGVs came from - which is what the PR description claims it is.
    """
    debug = collections.Counter(BUILD_TYPE_TO_CMAKE[BuildTypes.AMD_DEBUG].split())
    lane = collections.Counter(
        BUILD_TYPE_TO_CMAKE[BuildTypes.AMD_JEMALLOC_SAFETY].split()
    )
    assert lane - debug == collections.Counter([f"-D{OPTION_NAME}=1"]), (
        f"the {BuildTypes.AMD_JEMALLOC_SAFETY} cmake command must be the "
        f"{BuildTypes.AMD_DEBUG} one plus exactly `-D{OPTION_NAME}=1`; it adds "
        f"{sorted(lane - debug)}"
    )
    assert not debug - lane, (
        f"the {BuildTypes.AMD_JEMALLOC_SAFETY} cmake command drops "
        f"{sorted(debug - lane)} relative to {BuildTypes.AMD_DEBUG}, so the lane no "
        "longer fuzzes the same build the SIGSEGVs came from"
    )


# --- and every other build must be WITHOUT them ---------------------------------------
#
# The option defaults to OFF. Flipping that default, or widening the `-D`s to a target
# other builds link, would arm both gates in every x86-64 jemalloc build, release included
# - and neither shipped layer above can see it: the probe runs only for the build type
# that requests the option, and `test_only_this_build_type_requests_the_option` reads the
# `ci/` cmake commands rather than a configured tree. So the ordinary build job, which has
# a configured tree by construction, asserts the negative.


def _macros_absent(tmp_path, entries) -> str | None:
    """The absent checker's verdict for these `(file, flags)` pairs, `None` if accepted."""
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {
                    "directory": str(tmp_path),
                    "file": file,
                    "command": f"clang-21 {flags} -o out.o -c {file}",
                }
                for file, flags in entries
            ]
        ),
        encoding="utf-8",
    )
    try:
        assert_jemalloc_safety_macros_absent(str(path))
    except AssertionError as error:
        return str(error)
    return None


@pytest.mark.parametrize(
    "label, entries, carried_macro",
    [
        ("a jemalloc TU with neither macro", [(JE, ""), (OTHER, "")], None),
        # The two the option passes, each on its own: one is enough to arm one gate.
        ("the safety macro on a jemalloc TU", [(JE, SAFETY)], REQUIRED_MACROS[0]),
        ("the size macro on a jemalloc TU", [(JE, SIZE)], REQUIRED_MACROS[1]),
        ("both macros on a jemalloc TU", [(JE, BOTH)], REQUIRED_MACROS[0]),
        # One spelling each from the matrix the scan is responsible for; the full matrix
        # is `test_the_leak_sweep_sees_every_spelling`'s.
        ("the split spelling", [(JE, SPLIT_SIZE)], REQUIRED_MACROS[1]),
        (
            "the forwarded spelling",
            [(JE, "-Wp,-DJEMALLOC_OPT_SIZE_CHECKS")],
            REQUIRED_MACROS[1],
        ),
        # A cancelled definition is not a definition, exactly as in the leak direction.
        (
            "a definition cancelled by a later -U",
            [(JE, f"{SAFETY} -U{REQUIRED_MACROS[0]}")],
            None,
        ),
        # The macros on a *non*-jemalloc TU are the positive check's leak sweep to report;
        # this check reads jemalloc's own lines, so it must not fire on them - otherwise a
        # leak would be reported twice, from the build that has nothing to do with it.
        ("the macros only on a non-jemalloc TU", [(JE, ""), (OTHER, BOTH)], None),
        # The opposite of the positive check's emptiness rule, and the plausible
        # accident: `contrib/jemalloc-cmake/CMakeLists.txt:1-11` disables jemalloc outright
        # whenever `SANITIZE` is set to anything but `undefined`, so `amd_asan_ubsan`,
        # `amd_tsan` and `amd_msan` have no jemalloc translation units at all. Copying the
        # fail-closed guard here would red every one of those builds.
        ("no jemalloc translation units at all", [(OTHER, "")], None),
        ("no entries at all", [], None),
    ],
)
def test_a_build_that_did_not_request_the_option_carries_neither_macro(
    tmp_path, label, entries, carried_macro
):
    verdict = _macros_absent(tmp_path, entries)
    if carried_macro is None:
        assert verdict is None, f"{label}: expected to be accepted, got:\n{verdict}"
        return
    assert verdict is not None, f"{label}: expected {carried_macro} to be reported"
    assert (
        carried_macro in verdict
    ), f"{label}: the failure must name {carried_macro}; got:\n{verdict}"
    assert (
        JE in verdict
    ), f"{label}: the failure must name the translation unit carrying it; got:\n{verdict}"


def test_a_build_without_exported_compile_commands_fails_closed(tmp_path):
    """A missing `compile_commands.json` is inconclusive, not an answer of "absent".

    All three of this guard's premises say the file is there: `CMakeLists.txt:50` sets
    `CMAKE_EXPORT_COMPILE_COMMANDS` unconditionally, the build job calls this only inside
    `if res:` after a successful cmake configure, and a configured tree therefore always
    has it. So its absence means the question could not be asked - and reading that as
    "neither macro is defined" would let a flipped default through on any build whose
    configure step changed shape.

    An *empty* jemalloc set is the opposite case and still passes; the two tolerated
    shapes are pinned in `test_a_build_that_did_not_request_the_option_carries_neither_macro`.
    """
    with pytest.raises(AssertionError) as raised:
        assert_jemalloc_safety_macros_absent(str(tmp_path / "nothing.json"))
    message = str(raised.value)
    assert str(tmp_path / "nothing.json") in message, (
        f"the failure must name the path it looked for; got:\n{message}"
    )
    for macro in REQUIRED_MACROS:
        assert f"-D{macro} is on" not in message, (
            f"a missing file must not be reported as {macro} being found; got:\n{message}"
        )


def test_both_checkers_fail_closed_on_a_missing_file(tmp_path):
    """The two paths handle the same state, so they must not drift apart again.

    They disagreed until r15: the positive check raised and this one returned `None`. An
    asymmetry between two paths deciding the same question is the smell that one of them is
    wrong, and the fail-open side was - so the symmetry itself is pinned, with the messages
    required to stay distinguishable so a failure still says which direction was being
    checked.
    """
    missing = str(tmp_path / "compile_commands.json")
    messages = []
    for checker in (
        assert_jemalloc_safety_macros_armed,
        assert_jemalloc_safety_macros_absent,
    ):
        with pytest.raises(AssertionError) as raised:
            checker(missing)
        messages.append(str(raised.value))
    assert all(missing in message for message in messages)
    assert messages[0] != messages[1], (
        "the two directions must be distinguishable in the failure text; both said:\n"
        f"{messages[0]}"
    )


class _StopBuild(Exception):
    """Sentinel raised right after the build job's jemalloc assertion decision point."""


@pytest.fixture
def build_job_run(monkeypatch):
    """Run the build job's `main()` through the cmake stage; report both checkers' calls.

    Returns `{"armed": [...], "absent": [...]}` of the paths each checker was handed, so
    the two directions can be asserted against each other: exactly one of them must run
    per build, and which one is the whole point of the wiring.

    Everything with an external effect is stubbed: no cmake is configured, no compiler
    cache is set up, and the run stops at the first shell command after the decision
    point. Mirrors the `guard` fixture of `test_ast_fuzzer_jemalloc_preflight.py`.
    """
    calls = {"armed": [], "absent": []}

    class _Ok:
        def is_ok(self):
            return True

        def set_info(self, *_args):
            pass

    class _Info:
        pr_number = 1
        is_local_run = True

        def add_workflow_warning(self, *_args):
            pass

    class _Version:
        def write(self):
            pass

    def _shell_check(command, *_args, **_kwargs):
        # `ninja --version` is the first command after the decision point.
        if isinstance(command, str) and command.startswith("ninja --version"):
            raise _StopBuild()
        return True

    def _from_commands_run(name=None, command=None, command_args=None, **_kwargs):
        # The assertion is passed as a callable with its argument list, so call it the
        # way praktika would; every other step is a shell command and is skipped.
        if callable(command):
            command(*(command_args or []))
        return _Ok()

    monkeypatch.setattr(build_job, "Info", _Info)
    monkeypatch.setattr(build_job, "setup_build_caches_env", lambda _info: None)
    monkeypatch.setattr(
        build_job.CHVersion,
        "get_current_version",
        staticmethod(lambda **_k: _Version()),
    )
    monkeypatch.setattr(build_job.Shell, "check", staticmethod(_shell_check))
    monkeypatch.setattr(
        build_job.Result, "from_commands_run", staticmethod(_from_commands_run)
    )
    monkeypatch.setattr(
        build_job,
        "assert_jemalloc_safety_macros_armed",
        lambda path: calls["armed"].append(path),
    )
    monkeypatch.setattr(
        build_job,
        "assert_jemalloc_safety_macros_absent",
        lambda path: calls["absent"].append(path),
    )

    def _run(build_type):
        for recorded in calls.values():
            recorded.clear()
        monkeypatch.setattr(
            build_job,
            "parse_args",
            lambda: argparse.Namespace(
                build_type=build_type, param=build_job.JobStages.CMAKE
            ),
        )
        try:
            build_job.main()
        except _StopBuild:
            pass
        return {kind: list(recorded) for kind, recorded in calls.items()}

    return _run


def test_the_build_job_asserts_the_macros_for_this_build_type(build_job_run):
    """The lane's build must actually run the compile-line check after cmake.

    Without the call site the whole layer is dead code and every case above still
    passes, while the lane rebuilds and fuzzes green with a detector gone.
    """
    calls = build_job_run(BuildTypes.AMD_JEMALLOC_SAFETY)
    assert len(calls["armed"]) == 1, (
        f"the build job must assert the jemalloc safety macros exactly once for "
        f"{BuildTypes.AMD_JEMALLOC_SAFETY}; it called the checker "
        f"{len(calls['armed'])} times"
    )
    assert calls["armed"][0].endswith("compile_commands.json"), (
        "the checker must be pointed at the generated compile commands; got "
        f"{calls['armed'][0]}"
    )
    # The build that promises the macros must not also be asserted to be without them.
    assert calls["absent"] == [], (
        f"{BuildTypes.AMD_JEMALLOC_SAFETY} requests the option, so the absent check must "
        f"not run for it; it was called with {calls['absent']}"
    )


@pytest.mark.parametrize(
    "build_type", [BuildTypes.AMD_DEBUG, BuildTypes.AMD_TSAN, BuildTypes.AMD_RELEASE]
)
def test_the_build_job_does_not_assert_the_macros_elsewhere(build_job_run, build_type):
    """Only this build type promises the macros, so no other build may be failed by it.

    And every other build must be checked for their *absence* instead: the option defaults
    to OFF, and nothing else can see a flipped default - the probe runs for one build type
    only, and `test_only_this_build_type_requests_the_option` reads the `ci/` cmake
    commands rather than a configured tree.
    """
    calls = build_job_run(build_type)
    assert calls["armed"] == [], (
        f"{build_type} does not request the option, so the armed check must not run for "
        f"it; it was called with {calls['armed']}"
    )
    assert len(calls["absent"]) == 1, (
        f"{build_type} must be checked for the macros' absence exactly once; the checker "
        f"ran {len(calls['absent'])} times"
    )
    assert calls["absent"][0].endswith("compile_commands.json"), (
        "the absent check must be pointed at the generated compile commands; got "
        f"{calls['absent'][0]}"
    )


# `JEMALLOC_DEBUG` is not one of the macros the option passes, but the preamble's two
# initializers each accept it as an alternative (`:191` `#elif defined(JEMALLOC_DEBUG)`,
# `:208` `|| defined(JEMALLOC_DEBUG)`), so a platform header defining it would arm both
# gates on its own. It belongs in this search for the same reason the two `-D` macros do.
DEBUG_MACRO = "JEMALLOC_DEBUG"
_PLATFORM_HEADER_MACROS = REQUIRED_MACROS + (DEBUG_MACRO,)
_PLATFORM_DIRECTIVE_RE_TEMPLATE = r"^\s*#\s*(?:undef|define)\s+{macro}\b.*$"


def _active_platform_directives(text: str, macro: str) -> list[str]:
    """Active `#undef`/`#define` lines for `macro` in a platform header's text.

    Both directions matter: a bare `#undef` cancels the `-D` the option passes, and a
    `#define` arms the gate in a build that never asked for it. Indented spellings
    (`#  define`) count, the commented-out placeholder form does not, and `\\b` keeps a
    merely prefixed identifier (`JEMALLOC_OPT_SIZE_CHECKS_DISABLED`) out.
    """
    return re.findall(_PLATFORM_DIRECTIVE_RE_TEMPLATE.format(macro=macro), text, re.M)


def test_reachable_platform_headers_do_not_change_the_macro_state():
    """A directive in the configured platform header decides the macro state.

    `jemalloc_internal_defs.h` is included before the `config_opt_*` definitions are
    read (`jemalloc_preamble.h:188` / `:207`, whose initializers are evaluated by
    `test_compiled_preamble_maps_each_macro_to_its_config_flag`, test `defined(...)`),
    so an active `#undef JEMALLOC_OPT_SIZE_CHECKS` there would disarm the gate while
    the cmake option, the build and the runtime preflight all stay green - and an active
    `#define` is the same hazard mirrored: it arms the gate in a default build, which
    breaks the option's default-off contract with every layer still green. Every header
    the option can reach must therefore leave all three macros' state untouched.
    """
    for header in _reachable_defs_headers():
        text = header.read_text(encoding="utf-8")
        for macro in _PLATFORM_HEADER_MACROS:
            active = _active_platform_directives(text, macro)
            assert not active, (
                f"{header.relative_to(REPO_ROOT)}: a `#undef`/`#define` of {macro} is "
                f"active ({active}); it must stay commented out "
                f"(`/* #undef {macro} */`). A bare `#undef` silently cancels the "
                f"`-D{macro}` that {OPTION_NAME} passes, and a `#define` arms the gate "
                "in a build that never asked for it, because jemalloc tests "
                f"`defined({macro})` after including this header "
                f"({JEMALLOC_PREAMBLE_REL[2:]}:188 and :207)."
            )


@pytest.mark.parametrize(
    "label, prologue, detected_expected",
    [
        ("#define of the size macro", "#define JEMALLOC_OPT_SIZE_CHECKS\n", True),
        ("#define of the safety macro", "#define JEMALLOC_OPT_SAFETY_CHECKS\n", True),
        # Arms both gates on its own, so it is a third enabling macro.
        ("#define JEMALLOC_DEBUG", "#define JEMALLOC_DEBUG\n", True),
        ("indented #define", "#  define JEMALLOC_OPT_SIZE_CHECKS\n", True),
        ("bare #undef", "#undef JEMALLOC_OPT_SIZE_CHECKS\n", True),
        # The spelling every reachable header already uses for its placeholder.
        (
            "commented-out placeholder",
            "/* #undef JEMALLOC_OPT_SIZE_CHECKS */\n",
            False,
        ),
        # A different identifier that merely has a macro's name as a prefix.
        (
            "suffixed identifier",
            "#define JEMALLOC_OPT_SIZE_CHECKS_DISABLED\n",
            False,
        ),
    ],
)
def test_platform_header_search_detects_both_directions(
    label, prologue, detected_expected
):
    """The predicate above, driven over each real header with a prologue injected.

    Pins that the search the assertion relies on fires on every way a header can change
    a macro's state, and stays quiet on the inert forms the headers really contain.
    """
    for header in _reachable_defs_headers():
        text = prologue + header.read_text(encoding="utf-8")
        detected = any(
            _active_platform_directives(text, macro)
            for macro in _PLATFORM_HEADER_MACROS
        )
        assert detected is detected_expected, (
            f"{label}: expected the platform-header search to "
            f"{'detect' if detected_expected else 'ignore'} this directive in "
            f"{header.relative_to(REPO_ROOT)}"
        )


_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.S)


def _preprocessor_expr_to_python(expr: str) -> str:
    """`defined(X) || defined(Y)` -> a Python expression over `D('X')`.

    Only the operators these conditions actually use are translated. Anything else
    (arithmetic comparison, a macro used as a value) survives into `eval` and raises
    there, which is the intended fail-closed behaviour: the guard must not silently
    approximate a condition it does not understand.
    """
    expr = re.sub(r"defined\s*\(\s*([A-Za-z_]\w*)\s*\)", r"D('\1')", expr)
    expr = re.sub(r"defined\s+([A-Za-z_]\w*)", r"D('\1')", expr)
    expr = expr.replace("&&", " and ").replace("||", " or ")
    expr = re.sub(r"!(?=\s*[D(])", " not ", expr)
    return expr


_PRIOR_STATE_DIRECTIVE_RE_TEMPLATE = r"^[ \t]*#[ \t]*(?:{kinds})[ \t]+{macro}\b.*$"


def _config_flag_value(text: str, flag: str, defined_macros: set, macro: str) -> bool:
    """Value of `static const bool <flag> = #if ... ;` under `defined_macros`.

    Walks the initializer's `#if`/`#ifdef`/`#ifndef`/`#elif`/`#else` arms and returns
    the `true`/`false` literal of the first arm whose condition holds, so the assertion
    is about what the compiler computes rather than about which identifiers appear. A
    condition can name the right macro and still not be armed by it - `&&` instead of
    `||`, an inverted test, swapped arms - and each of those disarms the detector while
    a text search for the macro stays satisfied.

    Backslash continuations are spliced first (the preprocessor's own rule) so a
    condition legitimately reflowed across physical lines is read whole. C block
    comments are stripped *before* the initializer is located: the safety block's own
    `#elif` arm contains a comment whose `;` would otherwise truncate the non-greedy
    match mid-comment.

    `macro` is the compile-time macro whose state the caller is modelling. Anything in
    this header that already changed that macro's state before the initializer is read
    makes `defined_macros` a fiction, so such a directive fails the guard closed rather
    than being modelled: an earlier `#undef` cancels the `-D` the lane passes exactly as
    a platform header's bare `#undef` would, and a `#define` arms the flag independently
    of the lane - both are changes this guard must not silently bless, and for the size
    gate nothing else can notice (`config_opt_size_checks` has no mallctl).

    `JEMALLOC_DEBUG` is scanned for as well, but for `#define` only, because both
    initializers accept it as an *alternative* (`:191`, `:208`): defining it arms either
    flag on its own, so `defined_macros` would no longer decide the outcome and the
    caller's `disarmed is False` half - the PR's premise that no ClickHouse build arms
    these flags today - would be satisfied while the preprocessor computes true.
    Undefining it changes neither modelled outcome, since nothing in this repo defines it
    (no `JEMALLOC_DEBUG` in the jemalloc cmake file, and no active directive for it in
    any reachable platform header), so a preceding `#undef JEMALLOC_DEBUG` removes a
    macro that was never there and is treated as inert.

    Fails closed - an unknown directive, a nested conditional, a non-literal arm, or no
    arm selected raises rather than guessing.
    """
    text = re.sub(r"\\\n", " ", text)
    text = _BLOCK_COMMENT_RE.sub("", text)
    match = re.search(rf"static const bool\s+{flag}\s*=(.*?);", text, re.S)
    assert match, (
        f"{JEMALLOC_PREAMBLE_REL}: no `static const bool {flag} = ...;` initializer "
        "found. This header is the only place the compile-time macro becomes the "
        "boolean jemalloc's detector sites read - re-derive this assertion against "
        "whatever replaced it before deleting it."
    )
    # Block comments are already stripped, so the commented-out placeholder spelling
    # (`/* #undef JEMALLOC_OPT_SIZE_CHECKS */`) cannot reach this. `\b` on the
    # identifier so `JEMALLOC_OPT_SIZE_CHECKS_DISABLED` does not count. The other
    # *option* macro is deliberately not scanned for: it is inert for this flag. The
    # directive kinds differ per macro: the option macro's state is what `defined_macros`
    # models, so either direction breaks the model, while `JEMALLOC_DEBUG` is only an
    # alternative enabling condition - defining it arms the flag, undefining an absent
    # macro changes nothing.
    prior_state = [
        directive
        for scanned, kinds in ((macro, "undef|define"), (DEBUG_MACRO, "define"))
        for directive in re.findall(
            _PRIOR_STATE_DIRECTIVE_RE_TEMPLATE.format(
                kinds=kinds, macro=re.escape(scanned)
            ),
            text[: match.start()],
            re.M,
        )
    ]
    assert not prior_state, (
        f"{JEMALLOC_PREAMBLE_REL}: an active `#undef`/`#define` of `{macro}`, or a "
        f"`#define` of `{DEBUG_MACRO}`, precedes "
        f"the `{flag}` initializer, so its condition is no longer decided by "
        f"the lane's `-D{macro}`. An earlier `#undef` cancels that `-D` just as a bare "
        "`#undef` in a platform header would, and a `#define` of either macro arms the "
        f"flag in builds that never asked for it ({DEBUG_MACRO} is an alternative in "
        "both initializers, so it arms the flag on its own). Either way this guard's "
        f"model of the macro state is "
        f"wrong, and for `config_opt_size_checks` there is no mallctl, so the AST "
        "fuzzer job's runtime preflight cannot see it. Re-derive this guard against "
        "the new shape rather than letting it report a state the preprocessor does not "
        f"compute.\ndirectives found: {prior_state}"
    )
    initializer = match.group(1)

    def defined(name: str) -> bool:
        return name in defined_macros

    arms: list[tuple[str, str]] = []
    condition = None
    body: list[str] = []
    conditional_open = False
    for line in initializer.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            directive = stripped[1:].strip()
            if condition is not None:
                arms.append((condition, "\n".join(body)))
            body = []
            if directive.startswith(("ifdef", "ifndef", "if")) and conditional_open:
                raise AssertionError(
                    f"{JEMALLOC_PREAMBLE_REL}: the `{flag}` initializer nests a "
                    f"conditional ({stripped!r}). This evaluator tracks one arm at a "
                    "time, so a nested arm's value would be reported without its "
                    "enclosing condition - a nested `true` under an outer arm the "
                    "preprocessor never selects would be read as arming the gate. "
                    "Re-derive this guard against the nested shape rather than letting "
                    "it approximate it."
                )
            if directive.startswith("ifdef"):
                condition = _preprocessor_expr_to_python(
                    f"defined({directive.split(None, 1)[1].strip()})"
                )
                conditional_open = True
            elif directive.startswith("ifndef"):
                condition = _preprocessor_expr_to_python(
                    f"!defined({directive.split(None, 1)[1].strip()})"
                )
                conditional_open = True
            elif directive.startswith("elif"):
                condition = _preprocessor_expr_to_python(directive[len("elif") :])
            elif directive.startswith("if"):
                condition = _preprocessor_expr_to_python(directive[len("if") :])
                conditional_open = True
            elif directive.startswith("else"):
                condition = "True"
            elif directive.startswith("endif"):
                condition = None
                conditional_open = False
            else:
                raise AssertionError(
                    f"{JEMALLOC_PREAMBLE_REL}: unhandled preprocessor directive "
                    f"{stripped!r} inside the `{flag}` initializer; extend this guard "
                    "rather than letting it approximate the condition"
                )
        elif condition is not None:
            body.append(line)
    if condition is not None:
        arms.append((condition, "\n".join(body)))

    for expression, arm in arms:
        if eval(expression, {"__builtins__": {}}, {"D": defined}):  # noqa: S307
            value = arm.strip()
            assert value in ("true", "false"), (
                f"{JEMALLOC_PREAMBLE_REL}: the selected arm of `{flag}` is "
                f"{value!r}, not a `true`/`false` literal - the initializer changed "
                "shape, so re-derive this guard instead of trusting it"
            )
            return value == "true"
    raise AssertionError(
        f"{JEMALLOC_PREAMBLE_REL}: no arm of the `{flag}` initializer is selected with "
        f"{sorted(defined_macros)} defined; the initializer lost its `#else`, so the "
        "flag's value is no longer determined by this guard"
    )


@pytest.mark.parametrize(
    "macro, flag",
    [
        ("JEMALLOC_OPT_SAFETY_CHECKS", "config_opt_safety_checks"),
        ("JEMALLOC_OPT_SIZE_CHECKS", "config_opt_size_checks"),
    ],
)
def test_compiled_preamble_maps_each_macro_to_its_config_flag(macro, flag):
    """Defining each `-D` must still make the flag the detector sites test true.

    This is the last layer at which the option can be silently lost. The two above pin
    that the `-D` is in effect on the compile line and not cancelled by a platform
    header; this one pins that it is *consumed*, by evaluating the initializer rather
    than searching it for the macro's name. Narrow `config_opt_size_checks` to
    `#if defined(JEMALLOC_DEBUG)`, or turn its `||` into `&&`, or swap its arms, and the
    `"mismatch in slab bit"` check is disarmed while the compile line, the platform
    headers and the build all stay green - and for the size gate there is no runtime
    observable either (no mallctl), so nothing else can notice.

    The `set()` half is the PR's own premise: no ClickHouse build arms these flags
    today, since `JEMALLOC_DEBUG` is not defined either.
    """
    preamble = JEMALLOC_PREAMBLE.read_text(encoding="utf-8")
    armed = _config_flag_value(preamble, flag, {macro}, macro)
    disarmed = _config_flag_value(preamble, flag, set(), macro)
    context = (
        f"This header is the sole conversion of `-D{macro}` into the boolean the "
        "detector sites read, and it is the one that gets compiled because "
        "`contrib/jemalloc-cmake/CMakeLists.txt:177` puts the cmake include tree ahead "
        "of the submodule's (whose `jemalloc_preamble.h.in` is never configure_file'd). "
        f"If defining `{macro}` no longer yields `{flag}`, the lane rebuilds and fuzzes "
        "green with the detector gone - and `config_opt_size_checks` has no mallctl, so "
        "the AST fuzzer job's runtime preflight cannot see it."
    )
    assert armed is True, (
        f"{JEMALLOC_PREAMBLE_REL}: with only `{macro}` defined, `{flag}` evaluates to "
        f"{armed!r}; the lane's `-D{macro}` no longer arms the gate. {context}"
    )
    assert disarmed is False, (
        f"{JEMALLOC_PREAMBLE_REL}: with no macros defined, `{flag}` evaluates to "
        f"{disarmed!r} rather than false, so this guard can no longer tell an armed "
        f"build from an unarmed one. {context}"
    )


# --- the mapping assertion's own negative cases ---------------------------------------
#
# Driven through the same helper the assertion uses, over the real preamble with only the
# `config_opt_size_checks` initializer substituted, so the ways a condition can name the
# right macro while not being armed by it stay pinned without mutating the real file.
# The size gate is the one worth pinning: it has no mallctl, so nothing else can notice.

_REAL_SIZE_BLOCK = """static const bool config_opt_size_checks =
#if defined(JEMALLOC_OPT_SIZE_CHECKS) || defined(JEMALLOC_DEBUG)
    true
#else
    false
#endif
    ;"""

# `&&`: the macro is still named, but the gate now also needs JEMALLOC_DEBUG, which the
# lane does not set.
_SIZE_AND_INSTEAD_OF_OR = _REAL_SIZE_BLOCK.replace(
    "|| defined(JEMALLOC_DEBUG)", "&& defined(JEMALLOC_DEBUG)"
)
# Inverted test: named, and armed by exactly the builds that do not define it.
_SIZE_NEGATED = _REAL_SIZE_BLOCK.replace(
    "#if defined(JEMALLOC_OPT_SIZE_CHECKS) || defined(JEMALLOC_DEBUG)",
    "#if !defined(JEMALLOC_OPT_SIZE_CHECKS)",
)
# Arms swapped: the condition is untouched, the value is inverted.
_SIZE_ARMS_SWAPPED = _REAL_SIZE_BLOCK.replace(
    "    true\n#else\n    false", "    false\n#else\n    true"
)
# The macro dropped from the condition: the case a plain text search would catch.
_SIZE_MACRO_REMOVED = _REAL_SIZE_BLOCK.replace(
    "defined(JEMALLOC_OPT_SIZE_CHECKS) || ", ""
)
# Legitimate reflow across physical lines: must keep passing (continuations are spliced).
_SIZE_REFLOWED = _REAL_SIZE_BLOCK.replace(
    "#if defined(JEMALLOC_OPT_SIZE_CHECKS) || defined(JEMALLOC_DEBUG)",
    "#if defined(JEMALLOC_DEBUG) \\\n    || defined(JEMALLOC_OPT_SIZE_CHECKS)",
)
# The `#ifdef`/`#elif` shape the *safety* flag already uses: must keep passing, and
# doubles as proof the helper handles both of the real file's two spellings.
_SIZE_IFDEF_ELIF_FORM = (
    "static const bool config_opt_size_checks =\n"
    "#ifdef JEMALLOC_OPT_SIZE_CHECKS\n"
    "    true\n"
    "#elif defined(JEMALLOC_DEBUG)\n"
    "    true\n"
    "#else\n"
    "    false\n"
    "#endif\n"
    "    ;"
)


def _size_flag_armed_with(block: str) -> bool:
    """`config_opt_size_checks` under only its own macro, with `block` substituted in."""
    preamble = JEMALLOC_PREAMBLE.read_text(encoding="utf-8")
    assert _REAL_SIZE_BLOCK in preamble, (
        f"{JEMALLOC_PREAMBLE_REL}: the `config_opt_size_checks` initializer no longer "
        "matches the text these negative cases substitute; re-derive them against the "
        "current initializer so they keep exercising the real shape"
    )
    return _config_flag_value(
        preamble.replace(_REAL_SIZE_BLOCK, block),
        "config_opt_size_checks",
        {"JEMALLOC_OPT_SIZE_CHECKS"},
        "JEMALLOC_OPT_SIZE_CHECKS",
    )


@pytest.mark.parametrize(
    "label, block, armed_expected",
    [
        ("unmutated", _REAL_SIZE_BLOCK, True),
        ("reflowed across a continuation", _SIZE_REFLOWED, True),
        ("#ifdef/#elif form", _SIZE_IFDEF_ELIF_FORM, True),
        ("&& instead of ||", _SIZE_AND_INSTEAD_OF_OR, False),
        ("condition negated", _SIZE_NEGATED, False),
        ("true/false arms swapped", _SIZE_ARMS_SWAPPED, False),
        ("macro dropped from the condition", _SIZE_MACRO_REMOVED, False),
    ],
)
def test_size_flag_evaluation_detects_disarming_edits(label, block, armed_expected):
    assert _size_flag_armed_with(block) is armed_expected, (
        f"{label}: expected `config_opt_size_checks` to evaluate to "
        f"{armed_expected} with only JEMALLOC_OPT_SIZE_CHECKS defined. A False here "
        "for one of the mutated shapes is what makes "
        "test_compiled_preamble_maps_each_macro_to_its_config_flag fail on it; a True "
        "for one of the legitimate shapes is what keeps that test from failing "
        "spuriously."
    )


# --- the mapping assertion's own negative cases, part two: prior macro state ----------
#
# A directive earlier in the same header that changes the macro's state makes the
# modelled `defined_macros` a fiction: an active `#undef` cancels the lane's `-D` (the
# platform-header hazard, reappearing inside the compiled preamble), a `#define` arms the
# flag in builds that never asked for it. Both must fail closed. The commented-out
# placeholder spelling and a suffixed identifier must not fire.


def _size_flag_armed_with_prologue(prologue: str) -> bool:
    """`config_opt_size_checks` with `prologue` inserted above its initializer."""
    preamble = JEMALLOC_PREAMBLE.read_text(encoding="utf-8")
    assert _REAL_SIZE_BLOCK in preamble, (
        f"{JEMALLOC_PREAMBLE_REL}: the `config_opt_size_checks` initializer no longer "
        "matches the text these negative cases substitute; re-derive them against the "
        "current initializer"
    )
    return _config_flag_value(
        preamble.replace(_REAL_SIZE_BLOCK, prologue + _REAL_SIZE_BLOCK),
        "config_opt_size_checks",
        {"JEMALLOC_OPT_SIZE_CHECKS"},
        "JEMALLOC_OPT_SIZE_CHECKS",
    )


@pytest.mark.parametrize(
    "label, prologue",
    [
        ("active #undef", "#undef JEMALLOC_OPT_SIZE_CHECKS\n"),
        ("active #define", "#define JEMALLOC_OPT_SIZE_CHECKS\n"),
        ("indented #undef", "#  undef JEMALLOC_OPT_SIZE_CHECKS\n"),
        # `JEMALLOC_DEBUG` is an alternative in both initializers, so defining it arms
        # the flag on its own: without it in the fail-closed set the helper reports
        # `armed=True, disarmed=False` (both assertions of the mapping test satisfied)
        # while `clang -E` computes the flag true with no option macro defined at all.
        ("active #define JEMALLOC_DEBUG", "#define JEMALLOC_DEBUG\n"),
    ],
)
def test_prior_macro_state_directives_fail_closed(label, prologue):
    with pytest.raises(AssertionError, match="precedes the"):
        _size_flag_armed_with_prologue(prologue)


@pytest.mark.parametrize(
    "label, prologue",
    [
        # The spelling every reachable platform header uses for its placeholder.
        ("commented-out placeholder", "/* #undef JEMALLOC_OPT_SIZE_CHECKS */\n"),
        # A different identifier that merely has the macro's name as a prefix.
        ("suffixed identifier", "#undef JEMALLOC_OPT_SIZE_CHECKS_DISABLED\n"),
        # The other gate's macro: unrelated to this flag's state.
        ("the other gate's macro", "#undef JEMALLOC_OPT_SAFETY_CHECKS\n"),
        # The widening to `JEMALLOC_DEBUG` must not over-fire on the inert forms.
        ("commented-out JEMALLOC_DEBUG", "/* #undef JEMALLOC_DEBUG */\n"),
        ("suffixed JEMALLOC_DEBUG", "#undef JEMALLOC_DEBUG_SOMETHING\n"),
        # Nothing in this repo defines `JEMALLOC_DEBUG`, so undefining it removes a macro
        # that was never there: both modelled outcomes are unchanged. It is the spelling
        # all 12 platform headers use for their inert placeholders, so a legitimate
        # tidy-up adding it here must not be reported as a broken guard model. A
        # `#define` of it is the real hazard and still fails closed above.
        ("active #undef JEMALLOC_DEBUG", "#undef JEMALLOC_DEBUG\n"),
    ],
)
def test_prior_state_guard_does_not_fire_on_inert_directives(label, prologue):
    assert _size_flag_armed_with_prologue(prologue) is True, (
        f"{label}: this directive does not change `JEMALLOC_OPT_SIZE_CHECKS`' state, so "
        "the prior-state guard must not fire on it"
    )


# --- the mapping assertion's own negative cases, part three: nested conditionals ------
#
# The evaluator tracks one arm at a time, so a `#if` nested inside another one would have
# its value reported without the enclosing condition. Measured against `cc -E`: with the
# real size condition nested under an outer arm the preprocessor never selects, the helper
# reports armed while the compiler computes false in every case - satisfying both
# assertions of the mapping test while the gate is unconditionally disarmed. The shape
# must therefore fail closed rather than be modelled.

_SIZE_NESTED_UNDER_FALSE_OUTER = (
    "static const bool config_opt_size_checks =\n"
    "#if defined(UNRELATED_MACRO)\n"
    "#  if defined(JEMALLOC_OPT_SIZE_CHECKS) || defined(JEMALLOC_DEBUG)\n"
    "    true\n"
    "#  else\n"
    "    false\n"
    "#  endif\n"
    "#else\n"
    "    false\n"
    "#endif\n"
    "    ;"
)

_SIZE_NESTED_UNDER_IFDEF_OUTER = (
    "static const bool config_opt_size_checks =\n"
    "#ifdef JEMALLOC_OPT_SIZE_CHECKS\n"
    "#  if defined(JEMALLOC_DEBUG)\n"
    "    true\n"
    "#  else\n"
    "    false\n"
    "#  endif\n"
    "#else\n"
    "    false\n"
    "#endif\n"
    "    ;"
)


@pytest.mark.parametrize(
    "label, block",
    [
        ("nested under a false outer arm", _SIZE_NESTED_UNDER_FALSE_OUTER),
        ("nested under an armed #ifdef", _SIZE_NESTED_UNDER_IFDEF_OUTER),
    ],
)
def test_nested_initializer_conditionals_fail_closed(label, block):
    with pytest.raises(AssertionError, match="nests a"):
        _size_flag_armed_with(block)


def test_ci_tests_digest_covers_the_jemalloc_cmake_file():
    """A change to the cmake file must re-run the assertions it decides.

    The `if (ENABLE_JEMALLOC_SAFETY_CHECKS AND NOT ARCH_AMD64)` guard
    (`contrib/jemalloc-cmake/CMakeLists.txt:90-92`) is what makes
    `REACHABLE_DEFS_HEADERS_GLOB` x86-64 only, so a commit widening it to another
    architecture also changes which platform headers
    `test_reachable_platform_headers_do_not_change_the_macro_state` has to check - and
    e2k's bare `#undef JEMALLOC_OPT_SAFETY_CHECKS` (`include_linux_e2k/...:374`) is
    exactly what it would then expose. `JobConfigs.ci_tests` digests `./ci`, which does
    not cover that cmake file, so without an explicit entry such a commit is
    cache-skipped and the header assertion never re-runs.
    """
    digest = JobConfigs.ci_tests.digest_config
    assert JEMALLOC_CMAKE_REL in digest.include_paths, (
        f"add {JEMALLOC_CMAKE_REL!r} to JobConfigs.ci_tests digest include_paths; "
        f"got {digest.include_paths}"
    )
    # The real path-matching predicate, not just membership in the list.
    assert JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc-cmake/CMakeLists.txt"]
    ), "CI Tests is not invalidated by a change to the jemalloc cmake file"
    # Exact file, not the whole directory: unrelated jemalloc-cmake files must not
    # start re-running CI Tests.
    assert not JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc-cmake/README"]
    ), "the digest entry broadened to the whole contrib/jemalloc-cmake directory"


def test_ci_tests_digest_covers_the_reachable_platform_headers():
    """The header assertion above must not be cache-skipped either.

    A bare `#undef` in a platform header disarms a gate without touching `./ci` or the
    jemalloc cmake file, so those headers need their own digest entry.
    """
    digest = JobConfigs.ci_tests.digest_config
    assert DEFS_HEADERS_DIGEST_ENTRY in digest.include_paths, (
        f"add {DEFS_HEADERS_DIGEST_ENTRY!r} to JobConfigs.ci_tests digest "
        f"include_paths; got {digest.include_paths}"
    )
    for header in _reachable_defs_headers():
        rel = str(header.relative_to(REPO_ROOT))
        assert JobConfigs.ci_tests.is_affected_by([rel]), (
            f"CI Tests is not invalidated by a change to {rel}, so a bare `#undef` "
            "there would be cache-skipped"
        )
    # Spelled to the file, not the directory: the entry must not start re-running
    # CI Tests for every contrib change.
    assert not JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc-cmake/README"]
    ), "the platform-header digest entry broadened to the jemalloc-cmake directory"
    assert not JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc/src/ctl.c"]
    ), "the platform-header digest entry broadened outside jemalloc-cmake"


def test_ci_tests_digest_covers_the_compiled_preamble():
    """The mapping assertion above must not be cache-skipped either.

    Narrowing a `config_opt_*` condition touches neither `./ci`, nor the jemalloc cmake
    file, nor a platform header, so the compiled preamble needs its own digest entry -
    otherwise the commit that disarms a gate is exactly the commit on which this file
    does not run, while the safety-check build job (which digests all of `./contrib`)
    still rebuilds and fuzzes green.
    """
    digest = JobConfigs.ci_tests.digest_config
    assert JEMALLOC_PREAMBLE_REL in digest.include_paths, (
        f"add {JEMALLOC_PREAMBLE_REL!r} to JobConfigs.ci_tests digest include_paths; "
        f"got {digest.include_paths}"
    )
    assert JobConfigs.ci_tests.is_affected_by(
        [str(JEMALLOC_PREAMBLE.relative_to(REPO_ROOT))]
    ), "CI Tests is not invalidated by a change to the compiled jemalloc preamble"
    # Exact file, not the directory: the other tracked headers next to it must not
    # start re-running CI Tests.
    assert not JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc-cmake/include/jemalloc/jemalloc.h"]
    ), "the preamble digest entry broadened to the jemalloc-cmake include tree"
    # And it names the compiled header, not the submodule template it shadows.
    assert not JobConfigs.ci_tests.is_affected_by(
        ["contrib/jemalloc/include/jemalloc/internal/jemalloc_preamble.h.in"]
    ), "the preamble digest entry points at the submodule template, not the compiled header"


# The compiler oracle is only an oracle if it runs. A skip counts as success
# (`Result.is_ok`), so a compiler missing from the job's image would leave this module
# green while every compiling case silently evaporated - the same failure it exists to
# catch, one layer up. Two things keep that from happening: the fixture fails rather than
# skips in CI, and the job's image really installs a compiler. Both are asserted here,
# and neither assertion needs a compiler itself, so they are alive either way.
@pytest.mark.parametrize(
    "label, local_run, expect",
    [
        ("in CI, compiler absent", False, "fail"),
        ("local run, compiler absent", True, "skip"),
    ],
)
def test_a_missing_compiler_fails_in_ci_and_skips_locally(
    monkeypatch, label, local_run, expect
):
    """Absence must be loud in CI and quiet locally, and nowhere silent in CI.

    Driven with `monkeypatch`, so it neither needs nor cares whether a real compiler is
    installed - which is what lets it assert the clang-free behaviour on a host that has
    clang.
    """
    monkeypatch.setattr(shutil, "which", lambda _name: None)
    monkeypatch.setattr(
        sys.modules[__name__], "_running_in_ci", lambda: not local_run
    )
    with pytest.raises(BaseException) as raised:
        _require_compiler_or_skip()
    if expect == "fail":
        assert raised.type is pytest.fail.Exception, (
            f"{label}: expected a hard failure, got {raised.type.__name__}; a skip in CI "
            "is counted as success, so the oracle would evaporate silently"
        )
        message = str(raised.value)
        assert ToolSet.COMPILER_C in message and "skip" in message, (
            f"{label}: the failure must name the compiler and say why a skip is not "
            f"acceptable; got {message!r}"
        )
    else:
        assert raised.type is pytest.skip.Exception, (
            f"{label}: a contributor without {ToolSet.COMPILER_C} must get a skip, not a "
            f"failure; got {raised.type.__name__}"
        )
    # A present compiler is neither, whichever side we are on.
    monkeypatch.setattr(shutil, "which", lambda name: f"/usr/bin/{name}")
    _require_compiler_or_skip()


def test_the_ci_signal_fails_closed_when_it_cannot_be_read():
    """An unreadable signal must read as CI, or the guard evaporates on a broken tree.

    The dangerous direction is `local`: it turns absence back into a skip. So anything
    unexpected - praktika not importable, the environment file malformed - counts as CI.
    """
    assert _running_in_ci.__doc__, "keep the fail-closed reasoning documented"
    import builtins

    real_import = builtins.__import__

    def _boom(name, *args, **kwargs):
        if name.startswith("ci.praktika"):
            raise ImportError("simulated: praktika is not importable")
        return real_import(name, *args, **kwargs)

    builtins.__import__ = _boom
    try:
        assert _running_in_ci() is True, (
            "an unreadable CI signal must count as CI; reading it as a local run would "
            "turn a missing compiler back into a silent skip"
        )
    finally:
        builtins.__import__ = real_import


def _apt_installed_packages(text):
    """The packages `text`'s `apt-get install`/`satisfy` commands name as operands.

    Three things have to happen for "is this package installed" to be answerable, and
    each of them is a way to get the wrong answer:

    * `\\` continuations are spliced, or an operand on its own physical line is invisible;
    * comment lines are dropped, or a package named in a comment reads as installed;
    * the spliced `RUN` is then split on `&&`/`||`/`;`/`|` into single commands, and only
      the install commands' operands are collected. Without this last step every token in
      the whole `RUN` counts - including a `clang-<N> --version` sanity check in the same
      chain, which made an earlier version of this assertion unfalsifiable.

    Flags are dropped, so `--yes` and `-o Acquire::...` cannot pass as packages.
    """
    logical = []
    pending = ""
    for raw in text.splitlines():
        stripped = raw.strip()
        if not pending and stripped.startswith("#"):
            continue
        if stripped.endswith("\\"):
            pending += stripped[:-1] + " "
            continue
        logical.append(pending + stripped)
        pending = ""
    if pending:
        logical.append(pending)

    packages = set()
    for line in logical:
        command = []
        for token in shlex.split(line, posix=False) + ["&&"]:
            if token in ("&&", "||", ";", "|"):
                if "apt-get" in command and (
                    "install" in command or "satisfy" in command
                ):
                    packages.update(
                        argument
                        for argument in command
                        if not argument.startswith("-")
                        and argument
                        not in ("apt-get", "install", "satisfy", "env", "RUN")
                        and "=" not in argument
                    )
                command = []
                continue
            command.append(token)
    return packages


def test_apt_installed_packages_reads_operands_and_nothing_else():
    """The helper above is what makes the install assertion mean "installed".

    Each of its three jobs is asserted, because failing any one of them silently weakens
    the assertion it feeds - which is exactly how the first version of that assertion
    ended up unfalsifiable.
    """
    packages = _apt_installed_packages(
        "# commented-package in a comment\n"
        "RUN apt-get update \\\n"
        "    && env DEBIAN_FRONTEND=noninteractive apt-get install --yes \\\n"
        "        real-package \\\n"
        "    && other-package --version \\\n"
        "    && rm -rf /tmp/*\n"
        "ENV X=1\n"
    )
    assert "real-package" in packages, (
        "an operand on its own continuation line must be seen as installed; got "
        f"{sorted(packages)}"
    )
    assert "commented-package" not in packages, "a comment must not read as installed"
    assert "other-package" not in packages, (
        "a command chained after the install with && is not the install; its arguments "
        f"must not read as installed packages. Got {sorted(packages)}"
    )
    assert not any(package.startswith("-") for package in packages), (
        f"flags must not read as packages; got {sorted(packages)}"
    )
    # And a file with no install at all yields nothing rather than everything.
    assert _apt_installed_packages("RUN echo real-package\n") == set()


def test_the_job_image_installs_the_compiler_these_cases_need():
    """`CI Tests` runs in one image, and that image must carry the compiler.

    This is the pin for the other half of the fix: the fixture failing loudly is only an
    improvement if the compiler is actually there, or every CI run reds. It is a plain
    text assertion about a file this PR owns, so it holds with or without a compiler on
    the host.
    """
    dockerfile = REPO_ROOT / JOB_IMAGE_WITH_COMPILER_REL[2:]
    assert dockerfile.is_file(), f"{JOB_IMAGE_WITH_COMPILER_REL} is missing"
    text = dockerfile.read_text(encoding="utf-8")

    # The image really is the one this job runs in, so the assertion cannot drift onto
    # an image `CI Tests` stopped using.
    image = "clickhouse/integration-tests-runner"
    assert image in JobConfigs.ci_tests.run_in_docker, (
        f"CI Tests no longer runs in {image} "
        f"({JobConfigs.ci_tests.run_in_docker!r}); point "
        "JOB_IMAGE_WITH_COMPILER_REL at the image it does run in, and assert the "
        "compiler there"
    )

    # Asserted as an `apt-get install` **operand**, not merely as text present somewhere:
    # a mention in a comment, or in a `--version` sanity line, is not an installation, and
    # an assertion satisfied by inactive text is the failure mode two removed cmake-text
    # models in this PR's history were removed for. So find the install commands and
    # require the package among their operands.
    installed = _apt_installed_packages(text)
    assert installed, (
        f"{JOB_IMAGE_WITH_COMPILER_REL} installs no apt packages at all; re-derive this "
        "assertion against how that image now installs them"
    )
    assert "clang-${LLVM_VERSION}" in installed, (
        f"{JOB_IMAGE_WITH_COMPILER_REL} does not install clang-${{LLVM_VERSION}} as an "
        "apt-get operand, so every compiler-oracle case in this module would fail (and, "
        "before the fixture change, would have silently skipped). "
        f"{ToolSet.COMPILER_C} is what makes this module answer by compiling instead of "
        f"by parsing. Operands seen: {sorted(t for t in installed if 'clang' in t)}"
    )

    # The version installed has to be the one the checks invoke. `ToolSet.COMPILER_C` is
    # `clang-<N>`; the Dockerfile spells that `N` as LLVM_VERSION, inherited from
    # test-base, so pin the two together rather than trusting they agree.
    version = ToolSet.COMPILER_C.rsplit("-", 1)[-1]
    base = REPO_ROOT / "ci/docker/test-base/Dockerfile"
    assert f"LLVM_VERSION={version}" in base.read_text(encoding="utf-8"), (
        f"test-base does not set LLVM_VERSION={version}, so "
        f"{JOB_IMAGE_WITH_COMPILER_REL} installs a clang other than "
        f"{ToolSet.COMPILER_C}, which is the binary these checks invoke"
    )

    # The apt source has to be restored in this image: test-base adds llvm.list, and the
    # runner removes /etc/apt/sources.list.d/*.list before its own first `apt-get
    # update`, so inheriting it is not possible. Without this line the `apt-get install`
    # above resolves against Ubuntu's own archive and installs nothing named clang-21.
    # Asserted against the *executable* text: this file explains the restore in a comment
    # right above it, and a comment is not a restore. Same reason the install above is
    # read as an operand rather than as a substring.
    executable = "\n".join(
        line
        for line in text.splitlines()
        if not line.strip().startswith("#")
    )
    assert "sources.list.d/llvm.list" in executable, (
        f"{JOB_IMAGE_WITH_COMPILER_REL} installs clang-${{LLVM_VERSION}} without "
        "restoring the LLVM apt source. Line 9 of that file removes "
        "/etc/apt/sources.list.d/*.list, which includes the llvm.list test-base wrote, "
        f"so {ToolSet.COMPILER_C} is not installable from Ubuntu's archive alone"
    )
    assert "apt.llvm.org" in executable, (
        f"{JOB_IMAGE_WITH_COMPILER_REL} writes an llvm.list that does not point at "
        "apt.llvm.org, so clang-${LLVM_VERSION} still is not installable"
    )
