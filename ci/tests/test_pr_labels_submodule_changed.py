"""
Tests for the `submodule changed` label decision in the `pr_labels_and_category.py`
workflow hook.

The label used to be applied whenever any changed path contained `contrib/`, which
mislabelled PRs touching ClickHouse's own build glue (`contrib/<lib>-cmake/`,
`contrib/CMakeLists.txt`) and the in-repo `contrib/antlr4-grammars` trees, none of
which are submodules.

Keying purely off the paths registered in the head's `.gitmodules` fixes that but
loses submodule *removals*: a removed submodule is still in the PR's file list while
no longer being listed in `.gitmodules` at the head. These tests pin the whole
lifecycle - add, update, remove, rename - plus the false positives the change exists
to prevent.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
# `praktika` is imported as a top-level package by the hook modules, so `ci/` itself
# must be on the path. CI does this via the praktika runner; replicate it here.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import submodule_changed

# A registered submodule set, as `.gitmodules` lists it at the PR head.
HEAD_SUBMODULES = {"contrib/zstd", "contrib/simdjson", "contrib/silk"}


@pytest.mark.parametrize(
    "case,changed_files,submodule_paths,expected",
    [
        # --- genuine submodule changes ---
        (
            "bump leaves .gitmodules untouched",
            ["contrib/zstd"],
            HEAD_SUBMODULES,
            True,
        ),
        (
            "bump alongside unrelated files",
            ["src/Storages/StorageURL.cpp", "contrib/simdjson"],
            HEAD_SUBMODULES,
            True,
        ),
        (
            "add registers the new path at head",
            [".gitmodules", "contrib/newlib", "contrib/newlib-cmake/CMakeLists.txt"],
            HEAD_SUBMODULES | {"contrib/newlib"},
            True,
        ),
        (
            # The regression this test file exists for: the removed path is gone from
            # the head's `.gitmodules`, so a path-only check would drop the label.
            "remove drops the path from .gitmodules at head",
            [".gitmodules", "contrib/oldlib"],
            HEAD_SUBMODULES,
            True,
        ),
        (
            "rename moves the path",
            [".gitmodules", "contrib/oldname", "contrib/newname"],
            HEAD_SUBMODULES | {"contrib/newname"},
            True,
        ),
        (
            # A removal that forgets to clean `.gitmodules` still leaves the entry
            # registered, so the path intersection catches it.
            "removal that leaves a stale .gitmodules entry",
            ["contrib/silk"],
            HEAD_SUBMODULES,
            True,
        ),
        # --- false positives the change exists to prevent ---
        (
            "cmake glue under contrib/ is not a submodule",
            ["contrib/arrow-cmake/CMakeLists.txt"],
            HEAD_SUBMODULES,
            False,
        ),
        (
            "contrib/CMakeLists.txt is ours",
            ["contrib/CMakeLists.txt", "contrib/aws-sdk-cpp-sqs/CMakeLists.txt"],
            HEAD_SUBMODULES,
            False,
        ),
        (
            "in-repo antlr grammar trees are not submodules",
            [
                "contrib/antlr4-grammars/promql/PromQLLexer.g4",
                "contrib/antlr4-grammars-cmake/generated/antlr4_grammars/PromQLLexer.cpp",
            ],
            HEAD_SUBMODULES,
            False,
        ),
        (
            "ordinary source file",
            ["src/Storages/StorageURL.cpp"],
            HEAD_SUBMODULES,
            False,
        ),
        ("no changed files", [], HEAD_SUBMODULES, False),
    ],
)
def test_submodule_changed(case, changed_files, submodule_paths, expected):
    assert submodule_changed(changed_files, submodule_paths) is expected, case
