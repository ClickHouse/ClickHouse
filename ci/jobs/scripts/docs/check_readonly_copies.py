#!/usr/bin/env python3
"""
Guard for one-way-synced docs folders.

Some docs folders in this repo are *copies* whose canonical source lives in
another repository (e.g. ``docs/integrations/language-clients/python`` is owned
by ``clickhouse-connect``). Editing the copy here is almost always a mistake --
the next sync overwrites it. This check fails such edits and points the
contributor at the canonical source.

The protected folders and their owning repos are declared in
``readonly_copies.json`` next to this file. The check matches a list of changed
files (repo-relative paths, as they appear in the PR diff) against those
folders.

It has no CI-framework dependency: the Praktika job feeds it
``Info().get_changed_files()``; standalone you can pipe ``git diff --name-only``.
"""

import argparse
import json
import os
import sys

CONFIG_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "readonly_copies.json"
)


def load_sources(config_path):
    with open(config_path, encoding="utf-8") as f:
        return json.load(f)["sources"]


def _normalize(path):
    return path.strip().removeprefix("./")


def find_violations(changed_files, sources):
    """Return [(source, [offending files])] for every source that was touched."""
    normalized = [_normalize(f) for f in changed_files]
    violations = []
    for src in sources:
        prefix = src["path"].rstrip("/")
        hit = [f for f in normalized if f == prefix or f.startswith(prefix + "/")]
        if hit:
            violations.append((src, sorted(hit)))
    return violations


def format_message(violations):
    lines = []
    for src, files in violations:
        owner = src.get("repo") or src.get("url", "")
        subdir = src.get("subdir")
        where = f" (in its `{subdir}` folder)" if subdir else ""
        lines.append(f"'{src['path']}' is a one-way copy from {owner}{where}.")
        if src.get("url"):
            lines.append(f"  Canonical source: {src['url']}")
        lines.append(
            "  Edit it there -- changes made directly here are overwritten by the "
            "next sync."
        )
        lines.append("  Offending files:")
        lines.extend(f"    {f}" for f in files)
        lines.append("")
    return "\n".join(lines).rstrip()


def check_readonly_copies(changed_files, config_path=CONFIG_PATH):
    """Return True if no read-only copied docs were edited, else False."""
    sources = load_sources(config_path)
    violations = find_violations(changed_files, sources)
    if not violations:
        print("OK: no edits to read-only copied docs.")
        return True
    print("Edits to read-only copied docs detected:\n")
    print(format_message(violations))
    return False


def _read_changed_files(args):
    if args.changed_file:
        return args.changed_file
    return [line.strip() for line in sys.stdin.read().splitlines() if line.strip()]


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--config", default=CONFIG_PATH, help="Path to readonly_copies.json.")
    p.add_argument(
        "--changed-file",
        action="append",
        default=[],
        help="A changed file path (repeatable). If omitted, newline-separated "
        "paths are read from stdin.",
    )
    args = p.parse_args(argv)
    return 0 if check_readonly_copies(_read_changed_files(args), args.config) else 1


if __name__ == "__main__":
    sys.exit(main())