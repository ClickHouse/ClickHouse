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

So the size gate is asserted here instead, at the layer where the two macros are
actually set. Losing either one leaves the lane green while removing detection, so
this file pins every place such a loss can happen:

* the active `target_compile_definitions(_jemalloc PRIVATE ...)` invocation, read with
  CMake comments stripped so a commented-out macro name cannot satisfy the assertion;
* the platform headers the option can reach (x86-64 only, since the option refuses
  every other arch), where a bare `#undef` would silently cancel the `-D`;
* the compiled `jemalloc_preamble.h`, the sole place each `-D` is converted into the
  boolean the detector sites read, where narrowing a condition to `JEMALLOC_DEBUG`
  alone would disarm a gate with every other layer still green;
* the `CI Tests` cache digest, so a commit changing any of those layers actually
  re-runs this file instead of being cache-skipped.
"""

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

from ci.defs.job_configs import JobConfigs

REPO_ROOT = Path(__file__).resolve().parents[2]
JEMALLOC_CMAKE = REPO_ROOT / "contrib" / "jemalloc-cmake" / "CMakeLists.txt"
JEMALLOC_CMAKE_REL = "./contrib/jemalloc-cmake/CMakeLists.txt"

# The header that actually gets compiled: `target_include_directories(_jemalloc SYSTEM
# PUBLIC include)` (CMakeLists.txt:177) precedes `PRIVATE "${LIBRARY_DIR}/include"`
# (:178), and the submodule's `jemalloc_preamble.h.in` is never `configure_file`d, so
# this ClickHouse-owned copy shadows it.
JEMALLOC_PREAMBLE = (
    REPO_ROOT
    / "contrib"
    / "jemalloc-cmake"
    / "include"
    / "jemalloc"
    / "internal"
    / "jemalloc_preamble.h"
)
JEMALLOC_PREAMBLE_REL = (
    "./contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h"
)

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


_CMAKE_BRACKET_COMMENT_RE = re.compile(r"#\[(=*)\[.*?\]\1\]", re.S)


def _strip_cmake_comments(text: str) -> str:
    """Drop CMake comments: bracket comments first, then `#` to end of line.

    Bracket comments (`#[[ ... ]]`, `#[==[ ... ]==]`) must be removed first: their
    body can span lines and sit inside a command's parentheses, where a per-line `#`
    split consumes only the opener and leaves the body looking like a real argument.
    The `(=*)` backreference makes the equals form match its own closer.

    Line comments are then a per-line split, which is enough here: the definitions
    block contains no `#` inside a quoted string.
    """
    text = _CMAKE_BRACKET_COMMENT_RE.sub("", text)
    return "\n".join(line.split("#", 1)[0] for line in text.splitlines())


def _definitions_block(text: str) -> str:
    """The `if (ENABLE_JEMALLOC_SAFETY_CHECKS) ... endif ()` block that defines macros.

    Located by content (it must contain `target_compile_definitions`) rather than by
    line number, so reformatting the file does not break the guard. Comments are
    stripped first, so every consumer below sees only active code.
    """
    stripped = _strip_cmake_comments(text)
    blocks = re.findall(
        rf"^if \(\s*{OPTION_NAME}\s*\).*?^endif \(\)",
        stripped,
        re.M | re.S,
    )
    with_definitions = [b for b in blocks if "target_compile_definitions" in b]
    assert with_definitions, (
        f"no `if ({OPTION_NAME}) ... endif ()` block containing "
        f"`target_compile_definitions` found in {JEMALLOC_CMAKE_REL}"
    )
    assert len(with_definitions) == 1, (
        f"expected exactly one `{OPTION_NAME}` block defining compile definitions, "
        f"found {len(with_definitions)}"
    )
    return with_definitions[0]


def _private_definitions_arguments(text: str) -> str:
    """Argument text of the block's `target_compile_definitions(_jemalloc PRIVATE ...)`.

    `re.S` so the definitions may be reflowed across lines.
    """
    block = _definitions_block(text)
    match = re.search(
        r"target_compile_definitions\s*\(\s*_jemalloc\s+PRIVATE\s+(.*?)\)",
        block,
        re.S,
    )
    assert match, (
        f"{JEMALLOC_CMAKE_REL}: the `{OPTION_NAME}` block must define its macros with "
        "`target_compile_definitions(_jemalloc PRIVATE ...)`. The macros are "
        "jemalloc-internal: `PRIVATE` on `_jemalloc` keeps them out of every "
        "ClickHouse translation unit, so no other target is compiled against a "
        f"different `config_opt_*` view of jemalloc's headers than jemalloc itself.\n"
        f"block was:\n{block}"
    )
    return match.group(1)


def _missing_macros(arguments: str) -> list[str]:
    """Which `REQUIRED_MACROS` are absent from a definitions-argument text.

    Whitespace-split rather than substring-searched: `-DJEMALLOC_OPT_SIZE_CHECKS` is a
    substring of `-DJEMALLOC_OPT_SIZE_CHECKS_DISABLED`, but jemalloc tests the exact
    identifier (`#ifdef`), so a suffixed spelling defines nothing.
    """
    defined = set(arguments.split())
    return [macro for macro in REQUIRED_MACROS if f"-D{macro}" not in defined]


def test_option_defines_both_jemalloc_safety_macros():
    arguments = _private_definitions_arguments(
        JEMALLOC_CMAKE.read_text(encoding="utf-8")
    )
    missing = _missing_macros(arguments)
    assert not missing, (
        f"{JEMALLOC_CMAKE_REL}: {OPTION_NAME} must define both "
        f"{' and '.join(REQUIRED_MACROS)}; missing {missing}. "
        "JEMALLOC_OPT_SIZE_CHECKS is the sole gate on maybe_check_alloc_ctx (the "
        "'mismatch in slab bit' check) and, unlike the safety gate, has no mallctl, "
        "so the AST fuzzer job's runtime preflight cannot notice its absence. "
        "Commented-out macro names do not count: the invocation is read with CMake "
        "comments stripped (line and bracket comments alike). Each macro is expected "
        "in this file's uniform `-D<MACRO>` spelling and is matched as a complete "
        "whitespace-delimited argument, so a suffixed or renamed spelling such as "
        "`-DJEMALLOC_OPT_SIZE_CHECKS_DISABLED` does not count either.\n"
        f"arguments were:\n{arguments}"
    )


# --- the assertion's own negative cases -----------------------------------------------
#
# Driven against inline CMake text through the same helpers the real assertion uses, so
# the two ways a macro can appear present while defining nothing - a suffixed spelling,
# and a name that survives comment stripping - stay pinned without mutating the real file.

_INLINE_GOOD = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

# The same invocation reflowed across lines: legitimate CMake style, must keep passing.
_INLINE_GOOD_REFLOWED = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE\n"
    "        -DJEMALLOC_OPT_SAFETY_CHECKS\n"
    "        -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

# Renamed/suffixed: contains the required name as a substring but defines nothing.
_INLINE_SUFFIXED = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS_DISABLED)\n"
    "endif ()\n"
)

# Present only inside a trailing line comment.
_INLINE_LINE_COMMENT = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE\n"
    "        -DJEMALLOC_OPT_SAFETY_CHECKS\n"
    "        # -DJEMALLOC_OPT_SIZE_CHECKS\n"
    "    )\n"
    "endif ()\n"
)

# Present only inside a bracket comment *inside* the parentheses, both spellings.
_INLINE_BRACKET_COMMENT = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE\n"
    "        -DJEMALLOC_OPT_SAFETY_CHECKS\n"
    "        #[[\n"
    "        -DJEMALLOC_OPT_SIZE_CHECKS\n"
    "        ]]\n"
    "    )\n"
    "endif ()\n"
)

_INLINE_BRACKET_EQUALS_COMMENT = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE\n"
    "        -DJEMALLOC_OPT_SAFETY_CHECKS\n"
    "        #[==[\n"
    "        -DJEMALLOC_OPT_SIZE_CHECKS\n"
    "        ]==]\n"
    "    )\n"
    "endif ()\n"
)


@pytest.mark.parametrize(
    "label, text, size_macro_expected",
    [
        ("single-line invocation", _INLINE_GOOD, True),
        ("reflowed invocation", _INLINE_GOOD_REFLOWED, True),
        ("suffixed macro name", _INLINE_SUFFIXED, False),
        ("line comment", _INLINE_LINE_COMMENT, False),
        ("bracket comment", _INLINE_BRACKET_COMMENT, False),
        ("equals-form bracket comment", _INLINE_BRACKET_EQUALS_COMMENT, False),
    ],
)
def test_size_macro_detection(label, text, size_macro_expected):
    arguments = _private_definitions_arguments(text)
    tokens = set(arguments.split())
    assert ("-DJEMALLOC_OPT_SIZE_CHECKS" in tokens) is size_macro_expected, (
        f"{label}: expected -DJEMALLOC_OPT_SIZE_CHECKS "
        f"{'present' if size_macro_expected else 'absent'} in the parsed argument "
        f"tokens {sorted(tokens)}"
    )
    # The real assertion's verdict must agree with the token view.
    assert ("JEMALLOC_OPT_SIZE_CHECKS" in _missing_macros(arguments)) is (
        not size_macro_expected
    ), f"{label}: _missing_macros disagrees with the parsed argument tokens"


def test_definitions_are_scoped_to_the_jemalloc_target():
    """The macros are jemalloc-internal and must not leak into other targets.

    `PRIVATE` on `_jemalloc` keeps them out of every ClickHouse translation unit, so
    no other target can end up compiled against a different `config_opt_*` view of
    jemalloc's internal headers than jemalloc itself.
    """
    # Raises with the scoping rationale when the invocation is missing or not PRIVATE.
    _private_definitions_arguments(JEMALLOC_CMAKE.read_text(encoding="utf-8"))


def test_reachable_platform_headers_keep_the_macro_undefs_inert():
    """A bare `#undef` in the configured platform header cancels the `-D`.

    `jemalloc_internal_defs.h` is included before the `config_opt_*` definitions are
    read (`jemalloc_preamble.h:188` / `:207`, whose conditions are asserted by
    `test_compiled_preamble_maps_each_macro_to_its_config_flag`, test `defined(...)`),
    so an active `#undef JEMALLOC_OPT_SIZE_CHECKS` there would disarm the gate while
    the cmake option, the build and the runtime preflight all stay green. Every header
    the option can reach must therefore keep both `#undef`s commented out.
    """
    for header in _reachable_defs_headers():
        text = header.read_text(encoding="utf-8")
        for macro in REQUIRED_MACROS:
            active = re.findall(rf"^\s*#\s*undef\s+{macro}\b.*$", text, re.M)
            assert not active, (
                f"{header.relative_to(REPO_ROOT)}: `#undef {macro}` is active "
                f"({active}); it must stay commented out (`/* #undef {macro} */`). A "
                f"bare `#undef` silently cancels the `-D{macro}` that "
                f"{OPTION_NAME} passes, because jemalloc tests `defined({macro})` "
                f"after including this header ({JEMALLOC_PREAMBLE_REL[2:]}:188 and "
                ":207)."
            )


def _config_flag_condition(text: str, flag: str) -> str:
    """The `#if`/`#ifdef`/`#elif` directives inside `static const bool <flag> = ...;`.

    Backslash continuations are spliced first (the preprocessor's own rule), so a
    condition legitimately reflowed across physical lines is still read whole. Then only
    `#`-leading logical lines are kept, so a C block comment in an arm (the safety block
    has one in its `#elif`) cannot be mistaken for part of a condition, and so the
    assertion is about the condition rather than about whitespace or arm order.
    """
    text = re.sub(r"\\\n", " ", text)
    match = re.search(rf"static const bool\s+{flag}\s*=(.*?);", text, re.S)
    assert match, (
        f"{JEMALLOC_PREAMBLE_REL}: no `static const bool {flag} = ...;` initializer "
        "found. This header is the only place the compile-time macro becomes the "
        "boolean jemalloc's detector sites read - re-derive this assertion against "
        "whatever replaced it before deleting it."
    )
    return "\n".join(
        line.strip()
        for line in match.group(1).splitlines()
        if line.strip().startswith("#")
    )


@pytest.mark.parametrize(
    "macro, flag",
    [
        ("JEMALLOC_OPT_SAFETY_CHECKS", "config_opt_safety_checks"),
        ("JEMALLOC_OPT_SIZE_CHECKS", "config_opt_size_checks"),
    ],
)
def test_compiled_preamble_maps_each_macro_to_its_config_flag(macro, flag):
    """Each `-D` must still be read by the flag the detector sites test.

    This is the third and last layer at which the option can be silently lost. The two
    above pin that the `-D` is passed and not cancelled; this one pins that it is
    consumed. Narrow `config_opt_size_checks` to `#if defined(JEMALLOC_DEBUG)` and the
    `"mismatch in slab bit"` check is disarmed while the cmake invocation, the platform
    headers and the build all stay green - and for the size gate there is no runtime
    observable either (no mallctl), so nothing else can notice.
    """
    condition = _config_flag_condition(
        JEMALLOC_PREAMBLE.read_text(encoding="utf-8"), flag
    )
    # Whole identifier, not a substring: `JEMALLOC_OPT_SIZE_CHECKS_DISABLED` contains
    # `JEMALLOC_OPT_SIZE_CHECKS` but is a different macro, and jemalloc tests the exact
    # identifier.
    assert re.search(rf"\b{macro}\b", condition), (
        f"{JEMALLOC_PREAMBLE_REL}: `{flag}` no longer reads `{macro}`; its condition "
        f"is now:\n{condition}\n"
        f"This header is the sole conversion of `-D{macro}` into the boolean the "
        "detector sites read, and it is the one that gets compiled because "
        "`contrib/jemalloc-cmake/CMakeLists.txt:177` puts the cmake include tree ahead "
        "of the submodule's (whose `jemalloc_preamble.h.in` is never configure_file'd). "
        f"With `{macro}` dropped from the condition the gate falls back to "
        "`JEMALLOC_DEBUG`, which the safety-check lane does not set, so the lane "
        "rebuilds and fuzzes green with the detector gone - and `config_opt_size_checks` "
        "has no mallctl, so the AST fuzzer job's runtime preflight cannot see it."
    )


def test_ci_tests_digest_covers_the_jemalloc_cmake_file():
    """This guard must not be cache-skipped on the commits that change what it guards.

    `JobConfigs.ci_tests` digests `./ci`, which does not cover
    `contrib/jemalloc-cmake/CMakeLists.txt`, so without an explicit entry a commit
    that drops a macro would not re-run `CI Tests` and the assertion above would
    never execute.
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
