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
  boolean the detector sites read, whose initializers are *evaluated* rather than
  searched for the macro's name - narrowing a condition to `JEMALLOC_DEBUG` alone,
  turning its `||` into `&&`, inverting it or swapping its arms would each disarm a
  gate with every other layer still green;
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


# CMake command names are case-insensitive, so every construct recognised below is
# matched with an inline `(?i:...)` group covering the command name and nothing else.
# `contrib/` already uses the uppercase style in hundreds of places, and under it a
# case-sensitive scanner stops seeing the very constructs it exists to reject.
# The `(?i:...)` scope is deliberately narrow: CMake's `PRIVATE` keyword and the
# `_jemalloc` target name are case-*sensitive* (`private` is a hard CMake error, and
# `_JEMALLOC` is not a target of this project), so a blanket `re.I` would make this
# guard read an invocation CMake itself rejects.
_OPTION_BLOCK_OPEN_RE = re.compile(rf"^(?i:if)\s*\(\s*{OPTION_NAME}\s*\)\s*$")
# Every CMake construct that opens a scope CMake may never enter, or may enter zero
# times, is counted - not just `if`/`endif`. A `foreach` over an empty list runs its
# body no times, so a definition inside it is as conditional as one in a branch.
_CMAKE_BLOCK_OPEN_RE = re.compile(r"^(?i:if|foreach|while|function|macro|block)\s*\(")
_CMAKE_BLOCK_CLOSE_RE = re.compile(
    r"^(?i:end(?:if|foreach|while|function|macro|block))\s*\("
)
# `else`/`elseif` open no scope: they end the arm CMake runs when the option is on.
_CMAKE_ARM_RE = re.compile(r"^(?i:else|elseif)\s*\(")
_PRIVATE_DEFINITIONS_RE = re.compile(
    r"(?i:target_compile_definitions)\s*\(\s*_jemalloc\s+PRIVATE\s+(.*?)\)", re.S
)

# The commands this scanner knows how to reason about. Anything else inside the option
# block means the block's effective definitions are no longer decidable by reading it,
# so the guard fails closed instead of treating the unknown command as inert.
_MODELLED_BLOCK_COMMANDS = frozenset(
    {
        "target_compile_definitions",
        "if",
        "else",
        "elseif",
        "endif",
        "foreach",
        "endforeach",
        "while",
        "endwhile",
        "function",
        "endfunction",
        "macro",
        "endmacro",
        "block",
        "endblock",
        "message",
        "set",
    }
)
# A command invocation is a name immediately followed by `(`, so a continuation line of
# a reflowed invocation (a bare `-DMACRO`) is not mistaken for one.
_BLOCK_COMMAND_RE = re.compile(r"^\s*([A-Za-z_]\w*)\s*\(")


def _definitions_block_lines(text: str) -> list[tuple[int, str, bool]]:
    """`(depth, line, in_first_arm)` for the `ENABLE_JEMALLOC_SAFETY_CHECKS` block.

    Located by content (it must contain `target_compile_definitions`) rather than by
    line number, so reformatting the file does not break the guard. Comments are
    stripped first, so every consumer below sees only active code.

    Control flow is tracked by *depth* rather than matched with a regex: every block
    opener/closer pair is counted regardless of indentation, and command names are
    matched case-insensitively because CMake treats them so, so a nested construct
    cannot be mistaken for part of the block's own top level. A regex ending at the
    first column-0 `endif ()` swallows an indented inner `endif ()` and reports a block
    spanning the nested branch, which lets a `target_compile_definitions` CMake may
    never reach look like the active one.

    `in_first_arm` is the second half of the same question: a depth-1 `else ()` /
    `elseif (...)` belongs to the option block's *own* `if`, so it opens no depth, and
    the lines after it are what CMake runs when the option is **off**. It goes false at
    the first such arm; a nested (`depth > 1`) `else`/`elseif` leaves it alone, since
    the whole inner construct is already excluded by the depth test.
    """
    stripped = _strip_cmake_comments(text)
    blocks: list[list[tuple[int, str, bool]]] = []
    current: list[tuple[int, str, bool]] | None = None
    depth = 0
    in_first_arm = True
    for line in stripped.splitlines():
        if current is None:
            if _OPTION_BLOCK_OPEN_RE.match(line):
                current = []
                depth = 1
                in_first_arm = True
            continue
        token = line.lstrip()
        if _CMAKE_BLOCK_CLOSE_RE.match(token):
            depth -= 1
            if depth == 0:
                blocks.append(current)
                current = None
                continue
        elif _CMAKE_ARM_RE.match(token) and depth == 1:
            in_first_arm = False
            continue
        current.append((depth, line, in_first_arm))
        if _CMAKE_BLOCK_OPEN_RE.match(token):
            depth += 1
    # Unbalanced control flow means the depths below are meaningless, so fail closed
    # rather than reading an invocation out of a block whose extent is unknown.
    assert current is None, (
        f"{JEMALLOC_CMAKE_REL}: the `if ({OPTION_NAME})` block is never closed "
        f"({len(current)} lines collected, `if`/`endif` unbalanced), so this guard "
        "cannot tell which invocations are unconditional"
    )

    with_definitions = [
        b
        for b in blocks
        if any("target_compile_definitions" in line.lower() for _, line, _ in b)
    ]
    assert with_definitions, (
        f"no `if ({OPTION_NAME}) ... endif ()` block containing "
        f"`target_compile_definitions` found in {JEMALLOC_CMAKE_REL}"
    )
    assert len(with_definitions) == 1, (
        f"expected exactly one `{OPTION_NAME}` block defining compile definitions, "
        f"found {len(with_definitions)}"
    )
    block = with_definitions[0]
    # Fail closed on any command this scanner does not model. Enumerating the known
    # constructs and treating the rest as unconditional is how each new spelling turns
    # into a wrong green: `cmake_language(EVAL CODE "...")` hides the invocation from
    # every reader, and a `return ()` above it means CMake never reaches it, yet both
    # read as an unconditional definition.
    unmodelled = sorted(
        {
            match.group(1)
            for match in (_BLOCK_COMMAND_RE.match(line) for _, line, _ in block)
            if match and match.group(1).lower() not in _MODELLED_BLOCK_COMMANDS
        }
    )
    assert not unmodelled, (
        f"{JEMALLOC_CMAKE_REL}: the `{OPTION_NAME}` block invokes commands this guard "
        f"does not model ({unmodelled}). It models a fixed command vocabulary, so an "
        "unmodelled command means the block's effective definitions are no longer "
        "decidable by reading it - `cmake_language (EVAL CODE ...)` hides the "
        "invocation, `return ()` or `include ()` change which lines CMake reaches at "
        "all. Re-derive this guard against the new shape rather than letting it "
        "approximate the block."
    )
    return block


def _private_definitions_arguments(text: str) -> str:
    """Argument text of the block's `target_compile_definitions(_jemalloc PRIVATE ...)`.

    Only invocations CMake runs unconditionally when the option is on count: depth 1
    (the block's own top level) *and* inside the block's first arm. A nested one is
    conditional by definition - CMake may never enter that branch, or may run a loop
    body zero times - and one in an `else`/`elseif` arm runs precisely when the option
    is off, so neither can stand in for the definitions the option promises. Several
    unconditional invocations are unioned, so splitting the macros across two top-level
    calls stays legal.

    `re.S` so the definitions may be reflowed across lines; a reflowed invocation is
    unconditional when its opening line is, and its continuation lines carry no block
    keyword.
    """
    block_lines = _definitions_block_lines(text)
    top_level = "\n".join(
        line for depth, line, first_arm in block_lines if depth == 1 and first_arm
    )
    arguments = _PRIVATE_DEFINITIONS_RE.findall(top_level)
    block = "\n".join(line for _, line, _ in block_lines)
    assert arguments, (
        f"{JEMALLOC_CMAKE_REL}: the `{OPTION_NAME}` block must define its macros with "
        "`target_compile_definitions(_jemalloc PRIVATE ...)`. The macros are "
        "jemalloc-internal: `PRIVATE` on `_jemalloc` keeps them out of every "
        "ClickHouse translation unit, so no other target is compiled against a "
        f"different `config_opt_*` view of jemalloc's headers than jemalloc itself. "
        "The invocation must also sit at the block's own top level: a nested one does "
        "not count, because CMake may never enter that branch, and then the option "
        "would define nothing while this guard stayed green. Nor does one in an "
        "`else ()` / `elseif (...)` arm of that same block: that arm is what CMake "
        "runs when the option is *off*.\n"
        f"block was:\n{block}"
    )
    return "\n".join(arguments)


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


# --- the assertion's own negative cases, part two: nested control flow ----------------
#
# A `target_compile_definitions` inside a branch nested in the option's block is
# conditional by definition, so it must not stand in for the definitions the option
# promises. All four shapes below satisfied the previous non-balanced-regex helper (which
# read the *first* invocation of an over-wide block), while the invocation CMake actually
# always runs defines only the safety macro.

_INLINE_NESTED_SIZE_FIRST = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    if (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endif ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "endif ()\n"
)

_INLINE_NESTED_SIZE_SECOND = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    if (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endif ()\n"
    "endif ()\n"
)

# The inner `endif ()` at column 0 - the spelling the old regex terminated on - must be
# rejected exactly like the indented one.
_INLINE_NESTED_ENDIF_COLUMN_ZERO = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "if (SOME_CONDITION)\n"
    "target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "endif ()\n"
)

_INLINE_NESTED_DEAD_BRANCH = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    if (FALSE)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endif ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "endif ()\n"
)

# Both arms nested: there is no unconditional invocation at all, so the guard must not
# find one to read - it raises with the top-level requirement instead.
_INLINE_NESTED_IF_ELSE = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    if (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    else ()\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    endif ()\n"
    "endif ()\n"
)

# Control: the macros legitimately split across two unconditional invocations. Both are
# always run, so this must keep passing.
_INLINE_TWO_TOP_LEVEL_CALLS = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

# An `else ()` / `elseif (...)` arm of the option block's *own* `if` sits at depth 1 too,
# and it is what CMake runs when the option is *off* - so a definition there is the exact
# opposite of unconditional. `foreach` / `while` bodies are the same class from the other
# direction: CMake may run them zero times.

_INLINE_ELSE_ARM = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "else ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_ELSEIF_ARM = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "elseif (SOME_CONDITION)\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_FOREACH_BODY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    foreach (item IN LISTS MAYBE_EMPTY_LIST)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endforeach ()\n"
    "endif ()\n"
)

_INLINE_WHILE_BODY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    while (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endwhile ()\n"
    "endif ()\n"
)

# A *nested* `else ()` must not end the option block's own first arm: the whole inner
# construct is already excluded by depth, and the depth-1 invocation after it is still
# unconditional (it defines only the safety macro, so the size macro must read absent -
# but were the flag cleared by the inner `else`, that invocation would vanish too and the
# guard would raise instead of reporting a missing macro).
_INLINE_NESTED_INNER_ELSE = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    if (SOME_CONDITION)\n"
    "    else ()\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    endif ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "endif ()\n"
)

# The armed path defines nothing at all and both macros live solely in the `else ()` arm:
# the vacuous-lane shape this file exists to reject, and the runtime preflight cannot see
# the size gate at all.
_INLINE_ELSE_ARM_ONLY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    '    message (STATUS "safety checks requested")\n'
    "else ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)


@pytest.mark.parametrize(
    "label, text, size_macro_expected",
    [
        ("nested branch, size block first", _INLINE_NESTED_SIZE_FIRST, False),
        ("nested branch, size block second", _INLINE_NESTED_SIZE_SECOND, False),
        (
            "nested branch, inner endif at column 0",
            _INLINE_NESTED_ENDIF_COLUMN_ZERO,
            False,
        ),
        ("dead if (FALSE) branch", _INLINE_NESTED_DEAD_BRANCH, False),
        ("the option block's own else arm", _INLINE_ELSE_ARM, False),
        ("the option block's own elseif arm", _INLINE_ELSEIF_ARM, False),
        ("foreach body, may run zero times", _INLINE_FOREACH_BODY, False),
        ("while body, may run zero times", _INLINE_WHILE_BODY, False),
        (
            "nested inner else, unconditional call after",
            _INLINE_NESTED_INNER_ELSE,
            False,
        ),
        ("two unconditional invocations", _INLINE_TWO_TOP_LEVEL_CALLS, True),
    ],
)
def test_nested_definitions_do_not_satisfy_the_guard(label, text, size_macro_expected):
    """Only invocations at the option block's own top level count."""
    arguments = _private_definitions_arguments(text)
    assert ("JEMALLOC_OPT_SIZE_CHECKS" in _missing_macros(arguments)) is (
        not size_macro_expected
    ), (
        f"{label}: expected -DJEMALLOC_OPT_SIZE_CHECKS "
        f"{'present' if size_macro_expected else 'absent'}; parsed arguments were "
        f"{arguments!r}"
    )


def test_wholly_nested_definitions_leave_no_invocation_to_read():
    """Every invocation nested: the guard must report a missing top-level one."""
    with pytest.raises(AssertionError, match="own top level"):
        _private_definitions_arguments(_INLINE_NESTED_IF_ELSE)


def test_definitions_only_in_the_else_arm_leave_no_invocation_to_read():
    """The armed path defines nothing: the guard must not read the `else` arm instead.

    This is the vacuous-lane shape in full - `ENABLE_JEMALLOC_SAFETY_CHECKS` on defines
    neither macro, so the fuzzer runs with both detectors gone, and the AST fuzzer job's
    runtime preflight cannot notice the size gate's absence (no mallctl).
    """
    with pytest.raises(AssertionError, match="own top level"):
        _private_definitions_arguments(_INLINE_ELSE_ARM_ONLY)


def test_unbalanced_control_flow_fails_closed():
    """A block that is never closed makes every depth meaningless."""
    unbalanced = (
        "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
        "    if (SOME_CONDITION)\n"
        "        target_compile_definitions(_jemalloc PRIVATE"
        " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
        "endif ()\n"
    )
    with pytest.raises(AssertionError, match="never closed"):
        _private_definitions_arguments(unbalanced)


# --- the assertion's own negative cases, part three: command-name case ----------------
#
# CMake command names are case-insensitive, and `contrib/` already uses the uppercase
# style widely, so every construct the cases above pin must be recognised under it too.
# Otherwise each of those shapes reappears as a wrong green with only the keyword's case
# changed - including the full vacuous-lane shape, where the option defines neither macro.

_INLINE_UPPER_ELSE_ARM = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "ELSE ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_UPPER_ELSEIF_ARM = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "ELSEIF (SOME_CONDITION)\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_UPPER_NESTED_IF = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    IF (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    ENDIF ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "endif ()\n"
)

_INLINE_MIXED_CASE_FOREACH_BODY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    Foreach (item IN LISTS MAYBE_EMPTY_LIST)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    EndForeach ()\n"
    "endif ()\n"
)

_INLINE_UPPER_WHILE_BODY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc PRIVATE -DJEMALLOC_OPT_SAFETY_CHECKS)\n"
    "    WHILE (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "    ENDWHILE ()\n"
    "endif ()\n"
)

# An entirely uppercase but perfectly valid file: CMake runs this exactly like the real
# lowercase one, so the guard must read the definitions as present. This is the case that
# pins the block-selection substring test, which is easy to miss.
_INLINE_ALL_UPPERCASE_VALID = (
    "IF (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    TARGET_COMPILE_DEFINITIONS(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "ENDIF ()\n"
)

# The vacuous-lane shape spelled with an uppercase `ELSE`: the armed path defines nothing.
_INLINE_UPPER_ELSE_ARM_ONLY = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    '    message (STATUS "safety checks requested")\n'
    "ELSE ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_UPPER_INNER_IF_UNCLOSED = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    IF (SOME_CONDITION)\n"
    "        target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

# The two spellings CMake really is case-*sensitive* about. Both are invocations CMake
# rejects outright, so the guard must not read either as defining anything - which is
# what a blanket case-insensitive match on the whole invocation would do.
_INLINE_LOWERCASE_PRIVATE_KEYWORD = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_jemalloc private"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_UPPERCASE_TARGET_NAME = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    target_compile_definitions(_JEMALLOC PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)


@pytest.mark.parametrize(
    "label, text, size_macro_expected",
    [
        ("uppercase ELSE arm of the option's own if", _INLINE_UPPER_ELSE_ARM, False),
        ("uppercase ELSEIF arm", _INLINE_UPPER_ELSEIF_ARM, False),
        ("uppercase IF/ENDIF nested branch", _INLINE_UPPER_NESTED_IF, False),
        ("MixedCase Foreach body", _INLINE_MIXED_CASE_FOREACH_BODY, False),
        ("uppercase WHILE body", _INLINE_UPPER_WHILE_BODY, False),
        ("all-uppercase but valid file", _INLINE_ALL_UPPERCASE_VALID, True),
    ],
)
def test_command_names_are_recognised_regardless_of_case(
    label, text, size_macro_expected
):
    """CMake command names are case-insensitive, so this guard's recognition must be.

    Every shape here executes in CMake exactly like its lowercase twin above. Were the
    scanner case-sensitive, each would be read as an unconditional definition, which is
    the same wrong green as before with only the keyword's case changed.
    """
    arguments = _private_definitions_arguments(text)
    assert ("JEMALLOC_OPT_SIZE_CHECKS" in _missing_macros(arguments)) is (
        not size_macro_expected
    ), (
        f"{label}: expected -DJEMALLOC_OPT_SIZE_CHECKS "
        f"{'present' if size_macro_expected else 'absent'}; parsed arguments were "
        f"{arguments!r}"
    )


def test_uppercase_else_only_definitions_leave_no_invocation_to_read():
    """The vacuous-lane shape, spelled with an uppercase `ELSE ()`."""
    with pytest.raises(AssertionError, match="own top level"):
        _private_definitions_arguments(_INLINE_UPPER_ELSE_ARM_ONLY)


def test_uppercase_unbalanced_control_flow_fails_closed():
    """An uppercase inner `IF` that is never closed must fail closed too."""
    with pytest.raises(AssertionError, match="never closed"):
        _private_definitions_arguments(_INLINE_UPPER_INNER_IF_UNCLOSED)


@pytest.mark.parametrize(
    "label, text",
    [
        ("lowercase `private` keyword", _INLINE_LOWERCASE_PRIVATE_KEYWORD),
        ("uppercased target name", _INLINE_UPPERCASE_TARGET_NAME),
    ],
)
def test_case_sensitive_parts_of_the_invocation_stay_case_sensitive(label, text):
    """`PRIVATE` and the target name are case-sensitive in CMake; keep them so.

    CMake errors on both spellings (`private` is not a valid keyword there, `_JEMALLOC`
    is not a target of this project), so reading either as a definition would mean this
    guard blessing an invocation the build itself rejects.
    """
    with pytest.raises(AssertionError, match="own top level"):
        _private_definitions_arguments(text)


# --- the assertion's own negative cases, part four: unmodelled commands ---------------
#
# The scanner reasons about a fixed command vocabulary. Every previous round taught it one
# more spelling of "this definition is conditional", so the class is closed from the other
# end instead: a command it does not model makes the block undecidable and must fail
# closed, rather than being silently treated as inert.

_INLINE_CMAKE_LANGUAGE_EVAL = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    '    cmake_language(EVAL CODE "target_compile_definitions(_jemalloc PRIVATE'
    ' -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)")\n'
    "endif ()\n"
)

_INLINE_RETURN_BEFORE_DEFINITIONS = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    return ()\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

_INLINE_INCLUDE_BEFORE_DEFINITIONS = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    "    include (defs.cmake)\n"
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)

# Control: `message ()` is modelled, so a block using it must not fail closed.
_INLINE_MESSAGE_THEN_DEFINITIONS = (
    "if (ENABLE_JEMALLOC_SAFETY_CHECKS)\n"
    '    message (STATUS "safety checks armed")\n'
    "    target_compile_definitions(_jemalloc PRIVATE"
    " -DJEMALLOC_OPT_SAFETY_CHECKS -DJEMALLOC_OPT_SIZE_CHECKS)\n"
    "endif ()\n"
)


@pytest.mark.parametrize(
    "label, text",
    [
        ("cmake_language (EVAL CODE ...)", _INLINE_CMAKE_LANGUAGE_EVAL),
        ("return () before the invocation", _INLINE_RETURN_BEFORE_DEFINITIONS),
        ("include () before the invocation", _INLINE_INCLUDE_BEFORE_DEFINITIONS),
    ],
)
def test_unmodelled_commands_fail_closed(label, text):
    """A command outside the modelled vocabulary makes the block undecidable.

    In each of these the option does *not* actually define both macros unconditionally -
    the invocation is hidden inside a string, or CMake never reaches it - yet each reads
    as an unconditional definition to a scanner that only knows the constructs it was
    taught. Failing closed here is what stops the next spelling from being a new wrong
    green.
    """
    with pytest.raises(AssertionError, match="does not model"):
        _private_definitions_arguments(text)


@pytest.mark.parametrize(
    "label, text",
    [
        ("message () then the invocation", _INLINE_MESSAGE_THEN_DEFINITIONS),
        # A continuation line of a reflowed invocation is not a command: the regex
        # requires a name immediately followed by `(`.
        ("reflowed invocation", _INLINE_GOOD_REFLOWED),
        ("two unconditional invocations", _INLINE_TWO_TOP_LEVEL_CALLS),
    ],
)
def test_modelled_commands_do_not_fail_closed(label, text):
    """Blocks built only from modelled commands must keep reading normally."""
    arguments = _private_definitions_arguments(text)
    assert not _missing_macros(arguments), (
        f"{label}: expected both macros to read present; parsed arguments were "
        f"{arguments!r}"
    )


def test_definitions_are_scoped_to_the_jemalloc_target():
    """The macros are jemalloc-internal and must not leak into other targets.

    `PRIVATE` on `_jemalloc` keeps them out of every ClickHouse translation unit, so
    no other target can end up compiled against a different `config_opt_*` view of
    jemalloc's internal headers than jemalloc itself.
    """
    # Raises with the scoping rationale when the invocation is missing or not PRIVATE.
    _private_definitions_arguments(JEMALLOC_CMAKE.read_text(encoding="utf-8"))


# `JEMALLOC_DEBUG` is not one of the macros the option passes, but the preamble's two
# initializers each accept it as an alternative (`:188` `#elif defined(JEMALLOC_DEBUG)`,
# `:207` `|| defined(JEMALLOC_DEBUG)`), so a platform header defining it would arm both
# gates on its own. It belongs in this search for the same reason the two `-D` macros do.
_PLATFORM_HEADER_MACROS = REQUIRED_MACROS + ("JEMALLOC_DEBUG",)
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


_PRIOR_STATE_DIRECTIVE_RE_TEMPLATE = r"^[ \t]*#[ \t]*(?:undef|define)[ \t]+{macro}\b.*$"


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
    # identifier so `JEMALLOC_OPT_SIZE_CHECKS_DISABLED` does not count, mirroring
    # `_missing_macros`' reasoning.
    prior_state = re.findall(
        _PRIOR_STATE_DIRECTIVE_RE_TEMPLATE.format(macro=re.escape(macro)),
        text[: match.start()],
        re.M,
    )
    assert not prior_state, (
        f"{JEMALLOC_PREAMBLE_REL}: an active `#undef`/`#define` of `{macro}` precedes "
        f"the `{flag}` initializer, so its condition is no longer decided by "
        f"the lane's `-D{macro}`. An earlier `#undef` cancels that `-D` just as a bare "
        "`#undef` in a platform header would, and a `#define` arms the flag in builds "
        "that never asked for it. Either way this guard's model of the macro state is "
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

    This is the third and last layer at which the option can be silently lost. The two
    above pin that the `-D` is passed and not cancelled; this one pins that it is
    consumed, by *evaluating* the initializer rather than searching it for the macro's
    name. Narrow `config_opt_size_checks` to `#if defined(JEMALLOC_DEBUG)`, or turn its
    `||` into `&&`, or swap its arms, and the `"mismatch in slab bit"` check is disarmed
    while the cmake invocation, the platform headers and the build all stay green - and
    for the size gate there is no runtime observable either (no mallctl), so nothing
    else can notice.

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
# The macro dropped from the condition: the case the previous text-search guard caught.
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
