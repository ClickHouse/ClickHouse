#!/usr/bin/env python3
"""Check that generated changelog cards, content, and navigation are current."""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path


STATIC_GENERATED_PATHS = (
    Path("resources/changelogs/cloud/index.mdx"),
    Path("resources/changelogs/cloud/release-notes-index.mdx"),
    Path("resources/changelogs/navigation.json"),
)
OSS_YEAR = re.compile(r"^# (\d{4}) Changelog\s*$", re.MULTILINE)
GENERATOR_COMMAND = "python3 _site/scripts/update_changelogs.py"


def generated_paths(docs_root: Path) -> list[Path]:
    changelog_path = docs_root.parent / "CHANGELOG.md"
    content = changelog_path.read_text(encoding="utf-8")
    years = OSS_YEAR.findall(content)
    if len(years) != 1:
        raise ValueError(
            "Expected one current-year heading in CHANGELOG.md, "
            f"found {len(years)}"
        )
    oss_changelog = Path("resources/changelogs/oss") / f"{years[0]}.mdx"
    return [docs_root / path for path in (*STATIC_GENERATED_PATHS, oss_changelog)]


def check_freshness(docs_root: Path) -> list[str]:
    try:
        paths = generated_paths(docs_root)
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
    process = subprocess.run(
        [sys.executable, str(generator)],
        cwd=docs_root,
        capture_output=True,
        text=True,
    )
    stale = [path for path in paths if path.read_bytes() != before[path]]

    for path in stale:
        path.write_bytes(before[path])

    if process.returncode != 0:
        return [
            f"generator failed with exit code {process.returncode}:\n"
            f"{process.stdout[-2000:]}\n{process.stderr[-2000:]}"
        ]
    if stale:
        listing = "\n".join(
            f"  {path.relative_to(docs_root)}" for path in stale
        )
        return [
            "Changelog cards, content, or navigation are out of date. Run:\n"
            f"    {GENERATOR_COMMAND}\n"
            "from the docs/ directory and commit the changes. "
            "Out-of-date files:\n"
            + listing
        ]
    return []


def main() -> int:
    docs_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    if not (docs_root / "docs.json").is_file():
        print(f"Error: no docs.json in {docs_root}; pass the docs root.")
        return 2

    errors = check_freshness(docs_root)
    if errors:
        print(f"FAIL: {len(errors)} changelog problem(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print("OK: changelog cards, content, and navigation are up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
