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


_OPTION_BLOCK_OPEN_RE = re.compile(rf"^if \(\s*{OPTION_NAME}\s*\)\s*$")
_CMAKE_IF_RE = re.compile(r"^if\s*\(")
_CMAKE_ENDIF_RE = re.compile(r"^endif\s*\(")
_PRIVATE_DEFINITIONS_RE = re.compile(
    r"target_compile_definitions\s*\(\s*_jemalloc\s+PRIVATE\s+(.*?)\)", re.S
)


def _definitions_block_lines(text: str) -> list[tuple[int, str]]:
    """`(depth, line)` for every line of the `ENABLE_JEMALLOC_SAFETY_CHECKS` block.

    Located by content (it must contain `target_compile_definitions`) rather than by
    line number, so reformatting the file does not break the guard. Comments are
    stripped first, so every consumer below sees only active code.

    Control flow is tracked by *depth* rather than matched with a regex: `if`/`endif`
    are counted regardless of indentation, so a nested branch cannot be mistaken for
    part of the block's own top level. A regex ending at the first column-0
    `endif ()` swallows an indented inner `endif ()` and reports a block spanning the
    nested branch, which lets a `target_compile_definitions` CMake may never reach look
    like the active one.
    """
    stripped = _strip_cmake_comments(text)
    blocks: list[list[tuple[int, str]]] = []
    current: list[tuple[int, str]] | None = None
    depth = 0
    for line in stripped.splitlines():
        if current is None:
            if _OPTION_BLOCK_OPEN_RE.match(line):
                current = []
                depth = 1
            continue
        token = line.lstrip()
        if _CMAKE_ENDIF_RE.match(token):
            depth -= 1
            if depth == 0:
                blocks.append(current)
                current = None
                continue
        current.append((depth, line))
        if _CMAKE_IF_RE.match(token):
            depth += 1
    # Unbalanced control flow means the depths below are meaningless, so fail closed
    # rather than reading an invocation out of a block whose extent is unknown.
    assert current is None, (
        f"{JEMALLOC_CMAKE_REL}: the `if ({OPTION_NAME})` block is never closed "
        f"({len(current)} lines collected, `if`/`endif` unbalanced), so this guard "
        "cannot tell which invocations are unconditional"
    )

    with_definitions = [
        b for b in blocks if any("target_compile_definitions" in line for _, line in b)
    ]
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

    Only invocations at the block's own top level (depth 1) count. A nested one is
    conditional by definition - CMake may never enter that branch - so it cannot stand
    in for the definitions the option promises. Several unconditional invocations are
    unioned, so splitting the macros across two top-level calls stays legal.

    `re.S` so the definitions may be reflowed across lines; a reflowed invocation is at
    depth 1 when its opening line is, and its continuation lines carry no `if`/`endif`.
    """
    block_lines = _definitions_block_lines(text)
    top_level = "\n".join(line for depth, line in block_lines if depth == 1)
    arguments = _PRIVATE_DEFINITIONS_RE.findall(top_level)
    block = "\n".join(line for _, line in block_lines)
    assert arguments, (
        f"{JEMALLOC_CMAKE_REL}: the `{OPTION_NAME}` block must define its macros with "
        "`target_compile_definitions(_jemalloc PRIVATE ...)`. The macros are "
        "jemalloc-internal: `PRIVATE` on `_jemalloc` keeps them out of every "
        "ClickHouse translation unit, so no other target is compiled against a "
        f"different `config_opt_*` view of jemalloc's headers than jemalloc itself. "
        "The invocation must also sit at the block's own top level: a nested one does "
        "not count, because CMake may never enter that branch, and then the option "
        "would define nothing while this guard stayed green.\n"
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
    read (`jemalloc_preamble.h:188` / `:207`, whose initializers are evaluated by
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

    Fails closed - an unknown directive, a non-literal arm, or no arm selected raises
    rather than guessing.
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
    for line in initializer.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            directive = stripped[1:].strip()
            if condition is not None:
                arms.append((condition, "\n".join(body)))
            body = []
            if directive.startswith("ifdef"):
                condition = _preprocessor_expr_to_python(
                    f"defined({directive.split(None, 1)[1].strip()})"
                )
            elif directive.startswith("ifndef"):
                condition = _preprocessor_expr_to_python(
                    f"!defined({directive.split(None, 1)[1].strip()})"
                )
            elif directive.startswith("elif"):
                condition = _preprocessor_expr_to_python(directive[len("elif") :])
            elif directive.startswith("if"):
                condition = _preprocessor_expr_to_python(directive[len("if") :])
            elif directive.startswith("else"):
                condition = "True"
            elif directive.startswith("endif"):
                condition = None
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
