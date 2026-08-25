#!/usr/bin/env python3
"""Check that generated changelog cards, content, and navigation are current."""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from pathlib import Path


CLOUD_GENERATED_PATHS = (
    Path("resources/changelogs/cloud/index.mdx"),
    Path("resources/changelogs/cloud/release-notes-index.mdx"),
)
SHARED_GENERATED_PATHS = (
    Path("resources/changelogs/navigation.json"),
)
SCOPES = ("cloud", "oss")
OSS_YEAR = re.compile(r"^# (\d{4}) Changelog\s*$", re.MULTILINE)
GENERATOR_COMMAND = "python3 _site/scripts/update_changelogs.py"


def generated_paths(docs_root: Path, scopes: set[str]) -> list[Path]:
    paths = []

    if "cloud" in scopes:
        paths.extend(CLOUD_GENERATED_PATHS)

    if scopes == set(SCOPES):
        paths.extend(SHARED_GENERATED_PATHS)

    if "oss" in scopes:
        changelog_path = docs_root.parent / "CHANGELOG.md"
        content = changelog_path.read_text(encoding="utf-8")
        years = OSS_YEAR.findall(content)
        if len(years) != 1:
            raise ValueError(
                "Expected one current-year heading in CHANGELOG.md, "
                f"found {len(years)}"
            )
        paths.append(Path("resources/changelogs/oss") / f"{years[0]}.mdx")

    return [docs_root / path for path in paths]


def check_freshness(docs_root: Path, scopes: set[str]) -> list[str]:
    try:
        paths = generated_paths(docs_root, scopes)
    except (OSError, ValueError) as error:
        return [f"cannot determine generated changelog files: {error}"]

    missing = [path for path in paths if not path.is_file()]
    if missing:
        return [
            "missing generated changelog file(s): "
            + ", ".join(str(path.relative_to(docs_root)) for path in missing)
        ]

    before = {path: path.read_bytes() for path in paths}
    generator = docs_root / "_site/scripts/update_changelogs.py"
    scope_args = [
        argument
        for scope in SCOPES
        if scope in scopes
        for argument in ("--scope", scope)
    ]
    process = subprocess.run(
        [sys.executable, str(generator), *scope_args],
        cwd=docs_root,
        capture_output=True,
        text=True,
    )
    stale = [path for path in paths if path.read_bytes() != before[path]]

    for path, content in before.items():
        if path.read_bytes() != content:
            path.write_bytes(content)

    if process.returncode != 0:
        return [
            f"generator failed with exit code {process.returncode}:\n"
            f"{process.stdout[-2000:]}\n{process.stderr[-2000:]}"
        ]
    if stale:
        listing = "\n".join(
            f"  {path.relative_to(docs_root)}" for path in stale
        )
        command = " ".join((GENERATOR_COMMAND, *scope_args))
        return [
            "Changelog cards, content, or navigation are out of date. Run:\n"
            f"    {command}\n"
            "from the docs/ directory and commit the changes. "
            "Out-of-date files:\n"
            + listing
        ]
    return []


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("docs_root", nargs="?", default=".")
    parser.add_argument("--scope", action="append", choices=SCOPES, dest="scopes")
    args = parser.parse_args(argv)

    docs_root = Path(args.docs_root).resolve()
    if not (docs_root / "docs.json").is_file():
        print(f"Error: no docs.json in {docs_root}; pass the docs root.")
        return 2

    scopes = set(args.scopes or SCOPES)
    errors = check_freshness(docs_root, scopes)
    if errors:
        print(f"FAIL: {len(errors)} changelog problem(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print("OK: changelog cards, content, and navigation are up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
