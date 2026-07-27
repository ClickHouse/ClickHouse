"""Build-configuration guard for the jemalloc safety-check lane.

`ENABLE_JEMALLOC_SAFETY_CHECKS` must define **two** jemalloc macros, and both are
load-bearing for the `WeeklyJemallocSafety` lane:

* `JEMALLOC_OPT_SAFETY_CHECKS` arms `config_opt_safety_checks`
  (`contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h:188`), which
  gates `arena_ptr_array_flush_impl`'s sized-deallocation detector.
* `JEMALLOC_OPT_SIZE_CHECKS` arms `config_opt_size_checks` (same file, `:207`),
  which is the **sole** gate on `maybe_check_alloc_ctx`
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
* the `CI Tests` cache digest, so a commit changing either layer actually re-runs this
  file instead of being cache-skipped.
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
    read (`jemalloc_preamble.h:188` / `:207` test `defined(...)`), so an active
    `#undef JEMALLOC_OPT_SIZE_CHECKS` there would disarm the gate while the cmake
    option, the build and the runtime preflight all stay green. Every header the
    option can reach must therefore keep both `#undef`s commented out.
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
                "after including this header "
                "(contrib/jemalloc-cmake/include/jemalloc/internal/jemalloc_preamble.h"
                ":188 and :207)."
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
