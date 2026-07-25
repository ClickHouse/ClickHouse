#!/usr/bin/env python3
"""Check that generated Cloud release-note cards and navigation are current."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


GENERATED_PATHS = (
    Path("resources/changelogs/cloud/release-notes-index.mdx"),
    Path("resources/changelogs/navigation.json"),
)
GENERATOR_COMMAND = "python3 _site/scripts/update_cloud_release_notes.py"


def check_freshness(docs_root: Path) -> list[str]:
    paths = [docs_root / path for path in GENERATED_PATHS]
    missing = [path for path in paths if not path.is_file()]
    if missing:
        return [
            "missing generated Cloud release-note file(s): "
            + ", ".join(str(path.relative_to(docs_root)) for path in missing)
        ]

    before = {path: path.read_bytes() for path in paths}
    generator = docs_root / "_site/scripts/update_cloud_release_notes.py"
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
            "Cloud release-note cards or navigation are out of date. Run:\n"
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
        print(f"FAIL: {len(errors)} Cloud release-note problem(s):")
        for error in errors:
            print(f"- {error}")
        return 1

    print("OK: Cloud release-note cards and navigation are up to date")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
