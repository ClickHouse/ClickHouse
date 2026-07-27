"""Build-configuration guard for the jemalloc safety-check lane.

`ENABLE_JEMALLOC_SAFETY_CHECKS` must define **two** jemalloc macros, and both are
load-bearing for the `WeeklyJemallocSafety` lane:

* `JEMALLOC_OPT_SAFETY_CHECKS` arms `config_opt_safety_checks`
  (`contrib/jemalloc/include/jemalloc/internal/jemalloc_preamble.h.in:197`), which
  gates `arena_ptr_array_flush_impl`'s sized-deallocation detector.
* `JEMALLOC_OPT_SIZE_CHECKS` arms `config_opt_size_checks` (same file, `:216`),
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


def _strip_cmake_comments(text: str) -> str:
    """Drop CMake comments (unquoted `#` to end of line).

    Deliberately naive: the definitions block contains no `#` inside a quoted string,
    so a per-line split is enough and keeps the guard readable.
    """
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


def test_option_defines_both_jemalloc_safety_macros():
    arguments = _private_definitions_arguments(
        JEMALLOC_CMAKE.read_text(encoding="utf-8")
    )
    missing = [
        macro for macro in REQUIRED_MACROS if f"-D{macro}" not in arguments
    ]
    assert not missing, (
        f"{JEMALLOC_CMAKE_REL}: {OPTION_NAME} must define both "
        f"{' and '.join(REQUIRED_MACROS)}; missing {missing}. "
        "JEMALLOC_OPT_SIZE_CHECKS is the sole gate on maybe_check_alloc_ctx (the "
        "'mismatch in slab bit' check) and, unlike the safety gate, has no mallctl, "
        "so the AST fuzzer job's runtime preflight cannot notice its absence. "
        "Commented-out macro names do not count: the invocation is read with CMake "
        "comments stripped. Each macro is expected in this file's uniform "
        "`-D<MACRO>` spelling.\n"
        f"arguments were:\n{arguments}"
    )


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
    read (`jemalloc_preamble.h.in:197` / `:216` test `defined(...)`), so an active
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
                "(contrib/jemalloc/include/jemalloc/internal/jemalloc_preamble.h.in"
                ":197 and :216)."
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
