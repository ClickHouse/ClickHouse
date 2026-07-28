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

* the option's own contract in `contrib/jemalloc-cmake/CMakeLists.txt`, read as text: it
  defaults to OFF, it is restricted to x86-64, and it passes both macros to `_jemalloc`
  as PRIVATE. None of the layers below can see any of these - the compile-line check runs
  only for the build type that requests the option anyway - so flipping the default to
  `ON` would arm both gates in every x86-64 build, release included;
* the effective compiler invocation, via `assert_jemalloc_safety_macros_armed`
  (`ci/jobs/build_clickhouse.py`, run right after cmake configuration in the
  `amd_jemalloc_safety` build): over every `contrib/jemalloc/src/*.c` entry of the
  generated `compile_commands.json`, both macros must be effectively defined - the last
  `-D`/`-U` on the line winning, as the preprocessor does, in either the joined
  (`-DX`) or the split (`-D X`) spelling - and no other translation unit may carry
  them. Its cases are driven below, together with its wiring: that this build type
  requests the option and that the build job really invokes the check;
* the platform headers the option can reach (x86-64 only, since the option refuses
  every other arch), where a bare `#undef` would silently cancel the `-D`;
* the compiled `jemalloc_preamble.h`, the sole place each `-D` is converted into the
  boolean the detector sites read, whose initializers are *evaluated* rather than
  searched for the macro's name - narrowing a condition to `JEMALLOC_DEBUG` alone,
  turning its `||` into `&&`, inverting it or swapping its arms would each disarm a
  gate with every other layer still green;
* the `CI Tests` cache digest, for all three files this module reads (the jemalloc cmake
  file, the platform headers and the compiled preamble) - so a commit changing any of them
  re-runs this file instead of being cache-skipped. The cmake entry is doubly needed: its
  `ARCH_AMD64` guard also decides *which* platform headers the header assertion has to
  check.

Whether the option's `-D`s actually survive to the compiler is still decided by the
compile line rather than by the cmake text, which is why both layers exist: the cmake
assertions pin the option's contract, the compile-line one pins its effect.
"""

import argparse
import collections
import json
import os
import re
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
from ci.defs.defs import BuildTypes
from ci.defs.job_configs import JobConfigs
from ci.jobs.build_clickhouse import (
    BUILD_TYPE_TO_CMAKE,
    assert_jemalloc_safety_macros_armed,
    effective_macro_state,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
JEMALLOC_CMAKE_REL = "./contrib/jemalloc-cmake/CMakeLists.txt"
JEMALLOC_CMAKE = REPO_ROOT / JEMALLOC_CMAKE_REL[2:]

# The header that actually gets compiled: `target_include_directories(_jemalloc SYSTEM
# PUBLIC include)` (CMakeLists.txt:177) precedes `PRIVATE "${LIBRARY_DIR}/include"`
# (:178), and the submodule's `jemalloc_preamble.h.in` is never `configure_file`d, so
# this ClickHouse-owned copy shadows it.
JEMALLOC_PREAMBLE_REL = (
    "./contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h"
)
JEMALLOC_PREAMBLE = REPO_ROOT / JEMALLOC_PREAMBLE_REL[2:]

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


# --- the option's own CMake contract --------------------------------------------------
#
# Three clauses of `contrib/jemalloc-cmake/CMakeLists.txt` are load-bearing, and none of
# the layers below can see any of them: the option's default (OFF), its `ARCH_AMD64`
# restriction, and the two `-D`s it passes. They are asserted here as *text*, because
# `ci/jobs/ci_tests_job.py` runs this suite with neither cmake nor a build tree, so there
# is no configured cache to query. A semantic check of the same clauses' outcome does
# exist - `assert_jemalloc_safety_macros_armed`, driven below - but only for the one build
# type that requests the option (`ci/jobs/build_clickhouse.py:495` gates it on
# `BuildTypes.AMD_JEMALLOC_SAFETY`), so it can never see the default flipping for every
# other build. The digest entry these assertions ride on already exists for the arch
# guard's effect on `REACHABLE_DEFS_HEADERS_GLOB`.

_OPTION_CALL_RE = re.compile(
    rf'^[ \t]*option[ \t]*\([ \t]*{OPTION_NAME}[ \t]+"[^"]*"(?P<initial>.*)\)[ \t]*$',
    re.M,
)
_ARCH_GUARD_CONDITION_RE = re.compile(
    rf"^[ \t]*if[ \t]*\([ \t]*{OPTION_NAME}[ \t]+AND[ \t]+NOT[ \t]+ARCH_AMD64[ \t]*\)"
    r"[ \t]*$",
    re.M,
)
_ARCH_GUARD_SET_OFF_RE = re.compile(
    rf"^[ \t]*set[ \t]*\([ \t]*{OPTION_NAME}[ \t]+OFF[ \t]*\)[ \t]*$", re.M
)
_OPTION_GUARDED_IF_RE = re.compile(
    rf"^[ \t]*if[ \t]*\([ \t]*{OPTION_NAME}[ \t]*\)[ \t]*$", re.M
)
_JEMALLOC_DEFINITIONS_RE = re.compile(
    r"^[ \t]*target_compile_definitions[ \t]*\([ \t]*_jemalloc[ \t]+"
    r"(?P<keyword>\w+)(?P<flags>[^)]*)\)[ \t]*$",
    re.M,
)


def _cmake_block_after(text: str, opening: re.Match) -> str:
    """The body of the cmake `if` whose opening line `opening` matched."""
    end = text.find("\nendif", opening.end())
    return text[opening.end() : end if end != -1 else len(text)]


def _option_initial_value(text: str) -> str:
    """Whatever the `option()` call passes after its help string, `""` if nothing."""
    match = _OPTION_CALL_RE.search(text)
    assert match, (
        f'{JEMALLOC_CMAKE_REL}: no `option ({OPTION_NAME} "...")` call found. This '
        "call is where the option's default lives - re-derive this assertion against "
        "whatever shape replaced it rather than letting it pass vacuously."
    )
    return match.group("initial").strip()


def _option_defaults_to_off(text: str) -> bool:
    """Whether the `option()` call leaves the option's default at cmake's OFF."""
    return _option_initial_value(text) == ""


def _option_is_restricted_to_x86_64(text: str) -> bool:
    """Whether the `ARCH_AMD64` guard is present and still turns the option off."""
    guard = _ARCH_GUARD_CONDITION_RE.search(text)
    if not guard:
        return False
    return _ARCH_GUARD_SET_OFF_RE.search(_cmake_block_after(text, guard)) is not None


def _option_defines_macro(text: str, macro: str) -> bool:
    """Whether an `if (<option>)` block passes `-D<macro>` to `_jemalloc` PRIVATE.

    Every such block is searched, not just the first: nothing stops the file from
    guarding several things on this option, and keying on the first one would make this
    predicate report on whichever block happens to come first.
    """
    for opening in _OPTION_GUARDED_IF_RE.finditer(text):
        for call in _JEMALLOC_DEFINITIONS_RE.finditer(
            _cmake_block_after(text, opening)
        ):
            if call.group("keyword") != "PRIVATE":
                continue
            if f"-D{macro}" in call.group("flags").split():
                return True
    return False


def test_the_option_defaults_to_off():
    """The option must stay opt-in, i.e. its `option()` call must pass no initial value.

    An initial value of `ON` - or of `${ENABLE_LIBRARIES}`, the spelling `ENABLE_JEMALLOC`
    itself uses at `:11` - arms both jemalloc gates in every x86-64 build including
    release, which is the opposite of this diagnostic option's default-off contract, and
    it is a user-visible change to shipped binaries from a PR whose changelog category
    says otherwise. No other layer can see it: the compile-line oracle runs only for
    `BuildTypes.AMD_JEMALLOC_SAFETY`, which requests the option anyway.

    The sibling diagnostic option `ENABLE_JEMALLOC_UAF_SAN` (`:75`) is spelled the same
    way - name plus help string, no initial value.
    """
    initial = _option_initial_value(JEMALLOC_CMAKE.read_text(encoding="utf-8"))
    assert initial == "", (
        f"{JEMALLOC_CMAKE_REL}: the `option ({OPTION_NAME} ...)` call now passes an "
        f"initial value ({initial!r}), so the option no longer defaults to OFF; it arms "
        "both jemalloc safety gates in every x86-64 build, release included"
    )


def test_the_option_is_restricted_to_x86_64():
    """The `ARCH_AMD64` guard must stay intact.

    It is what keeps the option away from e2k, whose platform header carries a bare
    `#undef JEMALLOC_OPT_SAFETY_CHECKS`
    (`include_linux_e2k/jemalloc/internal/jemalloc_internal_defs.h.in:374`) that would
    silently cancel the option's own `-D`. It is also what makes
    `REACHABLE_DEFS_HEADERS_GLOB` x86-64 only, so widening it means re-deriving that glob
    and re-running `test_reachable_platform_headers_do_not_change_the_macro_state`
    against the newly reachable headers.
    """
    assert _option_is_restricted_to_x86_64(
        JEMALLOC_CMAKE.read_text(encoding="utf-8")
    ), (
        f"{JEMALLOC_CMAKE_REL}: the `if ({OPTION_NAME} AND NOT ARCH_AMD64)` guard that "
        f"sets the option OFF is gone or reshaped. Without it the option reaches e2k, "
        "where the platform header's bare `#undef JEMALLOC_OPT_SAFETY_CHECKS` cancels "
        f"it; and {REACHABLE_DEFS_HEADERS_GLOB!r} must then be re-derived, since it is "
        "x86-64 only precisely because of this guard."
    )


@pytest.mark.parametrize("macro", REQUIRED_MACROS)
def test_the_option_defines_both_macros(macro):
    """Both macros must be passed to `_jemalloc`, and PRIVATE.

    Without both `-D`s the option is a no-op and the lane rebuilds and fuzzes green as an
    ordinary `amd_debug` session; `JEMALLOC_OPT_SIZE_CHECKS` has no mallctl, so neither
    the runtime preflight nor anything else downstream can notice. `PRIVATE` matters too:
    the macros are jemalloc-internal, and the compile-line oracle's leak sweep - which
    rejects them reaching any non-jemalloc translation unit - exists for that reason.
    """
    assert _option_defines_macro(JEMALLOC_CMAKE.read_text(encoding="utf-8"), macro), (
        f"{JEMALLOC_CMAKE_REL}: the `if ({OPTION_NAME})` block no longer passes "
        f"`-D{macro}` to `_jemalloc` as PRIVATE, so the option is a no-op for that gate "
        "and the lane fuzzes green with the detector missing"
    )


# --- the CMake contract's own negative cases ------------------------------------------
#
# Each predicate above is driven over the real file text with one clause mutated, in the
# `_size_flag_armed_with` shape: the helper first asserts the line being replaced is still
# in the file, so a reshaped cmake file cannot leave these cases silently exercising text
# that no longer exists.

_REAL_OPTION_LINE = (
    f'option ({OPTION_NAME} "Enable jemalloc sized-deallocation and double-free safety '
    'checks (diagnostic lane; x86-64 only)")'
)
_REAL_ARCH_GUARD_CONDITION = f"if ({OPTION_NAME} AND NOT ARCH_AMD64)"
_REAL_ARCH_GUARD_BLOCK = (
    f"{_REAL_ARCH_GUARD_CONDITION}\n"
    f'    message (${{RECONFIGURE_MESSAGE_LEVEL}} "{OPTION_NAME} is supported on '
    f'x86-64 only. Use -D{OPTION_NAME}=0")\n'
    f"    set ({OPTION_NAME} OFF)\n"
    "endif ()"
)
_REAL_DEFINITIONS_LINE = (
    "    target_compile_definitions(_jemalloc PRIVATE "
    f"-D{REQUIRED_MACROS[0]} -D{REQUIRED_MACROS[1]})"
)


def _cmake_text_with(real: str, mutated: str) -> str:
    """The real cmake file's text with `real` replaced by `mutated`."""
    text = JEMALLOC_CMAKE.read_text(encoding="utf-8")
    assert real in text, (
        f"{JEMALLOC_CMAKE_REL}: the text these negative cases mutate is no longer "
        f"present:\n{real}\nre-derive them against the current file so they keep "
        "exercising the real shape"
    )
    return text.replace(real, mutated, 1)


@pytest.mark.parametrize(
    "label, mutated, accepted_expected",
    [
        ("unmutated", _REAL_OPTION_LINE, True),
        (
            "an explicit ON default",
            _REAL_OPTION_LINE[:-1] + " ON)",
            False,
        ),
        # The spelling `ENABLE_JEMALLOC` itself uses, and the likeliest way this default
        # would be flipped by a well-meaning commit.
        (
            "defaulted from ENABLE_LIBRARIES",
            _REAL_OPTION_LINE[:-1] + " ${ENABLE_LIBRARIES})",
            False,
        ),
    ],
)
def test_default_off_predicate_detects_an_added_initial_value(
    label, mutated, accepted_expected
):
    assert (
        _option_defaults_to_off(_cmake_text_with(_REAL_OPTION_LINE, mutated))
        is accepted_expected
    ), f"{label}: expected the default-off predicate to {'accept' if accepted_expected else 'reject'} this `option()` call"


@pytest.mark.parametrize(
    "label, mutated, accepted_expected",
    [
        ("unmutated", _REAL_ARCH_GUARD_BLOCK, True),
        # Rewording the diagnostic must not false-fail: only the condition and the
        # `set(... OFF)` are load-bearing.
        (
            "reworded message",
            _REAL_ARCH_GUARD_BLOCK.replace(
                "is supported on x86-64 only.", "needs x86-64."
            ),
            True,
        ),
        ("guard block deleted", "", False),
        (
            "condition narrowed to the option alone",
            _REAL_ARCH_GUARD_BLOCK.replace(
                _REAL_ARCH_GUARD_CONDITION, f"if ({OPTION_NAME})"
            ),
            False,
        ),
        # Guarding the wrong architecture leaves the option enabled exactly where the
        # e2k `#undef` would cancel it.
        (
            "wrong architecture guarded",
            _REAL_ARCH_GUARD_BLOCK.replace("NOT ARCH_AMD64", "NOT ARCH_AARCH64"),
            False,
        ),
    ],
)
def test_arch_guard_predicate_detects_a_weakened_guard(
    label, mutated, accepted_expected
):
    assert (
        _option_is_restricted_to_x86_64(
            _cmake_text_with(_REAL_ARCH_GUARD_BLOCK, mutated)
        )
        is accepted_expected
    ), f"{label}: expected the arch-guard predicate to {'accept' if accepted_expected else 'reject'} this guard"


@pytest.mark.parametrize(
    "label, mutated, accepted_expected",
    [
        ("unmutated", _REAL_DEFINITIONS_LINE, (True, True)),
        # The macros are jemalloc-internal; PUBLIC leaks them into every dependent
        # translation unit, which the compile-line oracle's leak sweep then rejects.
        (
            "PRIVATE widened to PUBLIC",
            _REAL_DEFINITIONS_LINE.replace("PRIVATE", "PUBLIC"),
            (False, False),
        ),
        (
            "the size macro dropped",
            _REAL_DEFINITIONS_LINE.replace(f" -D{REQUIRED_MACROS[1]}", ""),
            (True, False),
        ),
        (
            "the safety macro dropped",
            _REAL_DEFINITIONS_LINE.replace(f"-D{REQUIRED_MACROS[0]} ", ""),
            (False, True),
        ),
        ("the whole call deleted", "", (False, False)),
    ],
)
def test_definitions_predicate_detects_a_lost_macro(label, mutated, accepted_expected):
    text = _cmake_text_with(_REAL_DEFINITIONS_LINE, mutated)
    accepted = tuple(_option_defines_macro(text, macro) for macro in REQUIRED_MACROS)
    assert accepted == accepted_expected, (
        f"{label}: expected the definitions predicate to read "
        f"{dict(zip(REQUIRED_MACROS, accepted_expected))} for this "
        "`target_compile_definitions` call"
    )


# --- the effective compile line -------------------------------------------------------
#
# What decides whether a macro is defined is the compiler invocation, not the cmake text
# that produced it: a `-U` reaching the `_jemalloc` compile line from any other command
# cancels the `-D` (and `contrib/jemalloc-cmake/CMakeLists.txt:255` already passes a
# macro that way, via `target_compile_options`). `CMakeLists.txt:50` exports
# `compile_commands.json` unconditionally, so the build job reads the answer from there.
# The fixtures below are synthetic, so these cases need no configured build.

JE = "/ClickHouse/contrib/jemalloc/src/arena.c"
JE2 = "/ClickHouse/contrib/jemalloc/src/jemalloc.c"
OTHER = "/ClickHouse/src/Interpreters/Context.cpp"
SAFETY, SIZE = (f"-D{macro}" for macro in REQUIRED_MACROS)
BOTH = f"{SAFETY} {SIZE}"
# The same two `-D`s with an added `-U` of the size macro, before and after them. Both
# contain the `-D`, so only ordered evaluation can tell them apart.
CANCELLED = f"{BOTH} -UJEMALLOC_OPT_SIZE_CHECKS"
RESTORED = f"{SAFETY} -UJEMALLOC_OPT_SIZE_CHECKS {SIZE}"

# The split spellings, where the operand is the next argv element. clang applies them
# exactly like the joined ones, and `cmake/target.cmake:3` is `add_definitions(-D
# OS_LINUX)`, so every entry of a configured tree carries one. Reading only the joined
# form is wrong in both directions: a split `-U` reads as still defined (the failure
# the lane's whole value depends on), a split `-D` as never mentioned.
SPLIT_SAFETY, SPLIT_SIZE = (f"-D {macro}" for macro in REQUIRED_MACROS)
SPLIT_UNDEF_SIZE = "-U JEMALLOC_OPT_SIZE_CHECKS"
SPLIT_CANCELLED = f"{BOTH} {SPLIT_UNDEF_SIZE}"
SPLIT_RESTORED = f"{SAFETY} {SPLIT_UNDEF_SIZE} {SIZE}"


def _armed(tmp_path, entries) -> bool:
    """Whether the build job's assertion accepts these `(file, flags)` pairs."""
    path = tmp_path / "compile_commands.json"
    path.write_text(
        json.dumps(
            [
                {"directory": "/b", "file": f, "command": f"clang-21 -c {flags} {f}"}
                for f, flags in entries
            ]
        ),
        encoding="utf-8",
    )
    try:
        assert_jemalloc_safety_macros_armed(str(path))
    except AssertionError:
        return False
    return True


@pytest.mark.parametrize(
    "label, entries, armed_expected",
    [
        (
            "both macros on every jemalloc TU",
            [(JE, BOTH), (JE2, BOTH), (OTHER, "")],
            True,
        ),
        # The bypass this layer exists for: a later `-U` from any other cmake command
        # cancels the `-D`, since the preprocessor takes the last mention.
        ("-D then a later -U of the size macro", [(JE, CANCELLED)], False),
        # ... and the reverse order really is defined, so ordering is modelled rather
        # than any `-U` being rejected on sight.
        ("-U then a later -D", [(JE, RESTORED)], True),
        ("the option never passed at all", [(JE, "")], False),
        (
            "one jemalloc TU of several missing a macro",
            [(JE, BOTH), (JE2, SAFETY)],
            False,
        ),
        # A rename of jemalloc's source layout must not pass vacuously. The other TU
        # carries no macro, so only the empty-selection guard can reject this.
        ("no jemalloc TU at all", [(OTHER, "")], False),
        ("the macros leak into a non-jemalloc TU", [(JE, BOTH), (OTHER, BOTH)], False),
        # Token-exact, as `#ifdef` is: a suffixed spelling defines nothing.
        ("suffixed lookalike", [(JE, f"{SAFETY} {SIZE}_DISABLED")], False),
        # `-DMACRO=1` is a definition too, which is what `#ifdef` tests.
        ("value form of the size macro", [(JE, f"{SAFETY} {SIZE}=1")], True),
        # --- the split `-D NAME` / `-U NAME` spelling ---
        #
        # The blocker this set exists for: a split `-U` really does cancel the joined
        # `-D`, and reading only joined tokens would accept this line while the
        # `"mismatch in slab bit"` detector is gone - with no runtime observable, since
        # `config_opt_size_checks` has no mallctl.
        ("joined -D then a split -U", [(JE, SPLIT_CANCELLED)], False),
        (
            "split -D then a joined -U",
            [(JE, f"{SAFETY} {SPLIT_SIZE} -UJEMALLOC_OPT_SIZE_CHECKS")],
            False,
        ),
        # Ordering is still modelled rather than any `-U` being rejected on sight, in
        # the split spelling too.
        ("split -U then a joined -D", [(JE, SPLIT_RESTORED)], True),
        (
            "split -U then a split -D",
            [(JE, f"{SAFETY} {SPLIT_UNDEF_SIZE} {SPLIT_SIZE}")],
            True,
        ),
        ("split -D of both macros", [(JE, f"{SPLIT_SAFETY} {SPLIT_SIZE}")], True),
        ("split -D with a value", [(JE, f"{SAFETY} {SPLIT_SIZE}=1")], True),
        # Token-exact in the split spelling as well.
        (
            "split -D of a suffixed lookalike",
            [(JE, f"{SAFETY} {SPLIT_SIZE}_DISABLED")],
            False,
        ),
        # The leak direction must see split forms too, or the macro could reach every
        # other translation unit unnoticed.
        (
            "the macros leak into a non-jemalloc TU in the split spelling",
            [(JE, BOTH), (OTHER, SPLIT_SAFETY)],
            False,
        ),
        # `-D OS_LINUX` is what every real compile line starts with: consuming the pair
        # must not swallow the flag that follows it.
        (
            "a real split neighbour ahead of the joined macros",
            [(JE, f"-D OS_LINUX {BOTH}")],
            True,
        ),
    ],
)
def test_effective_compile_line_decides_the_macro_state(
    tmp_path, label, entries, armed_expected
):
    """The build job's assertion, driven over synthetic compile commands.

    Each rejected shape leaves the lane fuzzing green with a detector gone, and for
    `JEMALLOC_OPT_SIZE_CHECKS` nothing downstream can notice (no mallctl).
    """
    assert _armed(tmp_path, entries) is armed_expected, (
        f"{label}: expected the jemalloc safety macros to read "
        f"{'armed' if armed_expected else 'not armed'}"
    )


@pytest.mark.parametrize(
    "label, cancelled, restored",
    [
        ("joined -U", CANCELLED, RESTORED),
        # The same property in the split spelling, whose operand is the next argv
        # element: `-D X -U X` is undefined and `-U X -D X` defined, exactly as joined.
        ("split -U", SPLIT_CANCELLED, SPLIT_RESTORED),
    ],
)
def test_a_substring_test_cannot_replace_the_ordered_state(label, cancelled, restored):
    """The pair differs only in order, so the last mention must win.

    Both contain `-DJEMALLOC_OPT_SIZE_CHECKS`, so a naive `f"-D{macro}" in command`
    would read both as defined and miss the bypass entirely.
    """
    assert SIZE in cancelled and SIZE in restored, (
        f"{label}: both spellings must contain the -D, otherwise this pair no longer "
        "distinguishes ordered evaluation from a substring test"
    )
    assert effective_macro_state(cancelled, "JEMALLOC_OPT_SIZE_CHECKS") is False
    assert effective_macro_state(restored, "JEMALLOC_OPT_SIZE_CHECKS") is True


def test_missing_compile_commands_fails_closed(tmp_path):
    """No exported compile commands means the question cannot be answered."""
    with pytest.raises(AssertionError, match="is missing"):
        assert_jemalloc_safety_macros_armed(str(tmp_path / "compile_commands.json"))


# --- the compile-line layer's own wiring ----------------------------------------------
#
# The cases above drive `assert_jemalloc_safety_macros_armed` directly, so they stay
# green if the build job stops calling it or if the build type stops requesting the
# option - in either case the whole layer disappears while every assertion passes.


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


class _StopBuild(Exception):
    """Sentinel raised right after the build job's jemalloc assertion decision point."""


@pytest.fixture
def build_job_run(monkeypatch):
    """Run the build job's `main()` through the cmake stage; report the checker's calls.

    Everything with an external effect is stubbed: no cmake is configured, no compiler
    cache is set up, and the run stops at the first shell command after the decision
    point. Mirrors the `guard` fixture of `test_ast_fuzzer_jemalloc_preflight.py`.
    """
    calls = []

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
        lambda path: calls.append(path),
    )

    def _run(build_type):
        calls.clear()
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
        return list(calls)

    return _run


def test_the_build_job_asserts_the_macros_for_this_build_type(build_job_run):
    """The lane's build must actually run the compile-line check after cmake.

    Without the call site the whole layer is dead code and every case above still
    passes, while the lane rebuilds and fuzzes green with a detector gone.
    """
    calls = build_job_run(BuildTypes.AMD_JEMALLOC_SAFETY)
    assert len(calls) == 1, (
        f"the build job must assert the jemalloc safety macros exactly once for "
        f"{BuildTypes.AMD_JEMALLOC_SAFETY}; it called the checker {len(calls)} times"
    )
    assert calls[0].endswith(
        "compile_commands.json"
    ), f"the checker must be pointed at the generated compile commands; got {calls[0]}"


@pytest.mark.parametrize(
    "build_type", [BuildTypes.AMD_DEBUG, BuildTypes.AMD_TSAN, BuildTypes.AMD_RELEASE]
)
def test_the_build_job_does_not_assert_the_macros_elsewhere(build_job_run, build_type):
    """Only this build type promises the macros, so no other build may be failed by it."""
    assert build_job_run(build_type) == []


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
    """A change to the cmake file must re-run the assertions about it.

    Two sets of them. The option's own contract is read straight out of this file
    (`test_the_option_defaults_to_off` and the two beside it), so a commit editing any of
    those three clauses is precisely the commit on which they must run. And the
    `if (ENABLE_JEMALLOC_SAFETY_CHECKS AND NOT ARCH_AMD64)` guard
    (`contrib/jemalloc-cmake/CMakeLists.txt:90-92`) is what makes
    `REACHABLE_DEFS_HEADERS_GLOB` x86-64 only, so a commit widening it to another
    architecture also changes which platform headers
    `test_reachable_platform_headers_do_not_change_the_macro_state` has to check - and
    e2k's bare `#undef JEMALLOC_OPT_SAFETY_CHECKS` (`include_linux_e2k/...:374`) is
    exactly what it would then expose. `JobConfigs.ci_tests` digests `./ci`, which does
    not cover that cmake file, so without an explicit entry such a commit is
    cache-skipped and neither is checked.
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
