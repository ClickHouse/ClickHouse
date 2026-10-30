"""Read the Rust archive inventory shared with bazel/rust/BUILD.bazel."""

import ast
from pathlib import Path


_module = ast.parse((Path(__file__).resolve().parents[1] / "rules/rust_archives.bzl").read_text())
_inventory = next(
    statement.value
    for statement in _module.body
    if isinstance(statement, ast.Assign)
    and len(statement.targets) == 1
    and isinstance(statement.targets[0], ast.Name)
    and statement.targets[0].id == "RUST_ARCHIVES"
)
RUST_ARCHIVES = ast.literal_eval(_inventory)
ARCHIVE_TARGETS = {spec["archive"]: name for name, spec in RUST_ARCHIVES.items()}
PACKAGE_TARGETS = {spec["package"]: name for name, spec in RUST_ARCHIVES.items()}
