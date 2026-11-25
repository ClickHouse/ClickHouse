#!/usr/bin/env python3
"""Run `mint validate` while recognizing localized-route false positives.

Mintlify currently reports localized documents and snippets as missing during a
full validation even when the corresponding files are present.  The locale link
checks cover those trees separately, so accept that specific diagnostic only
after verifying the referenced path exists on disk.  Any other warning or
validation error remains fatal.
"""

import argparse
import os
import re
import subprocess
import sys
from pathlib import Path


LOCALES = ("ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh")
ANSI = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
NAV_WARNING = re.compile(
    r'warning - "(?P<path>(?:' + "|".join(LOCALES) + r')/[^"\s]+)" '
    r"is referenced in the docs\.json navigation but the file does not exist\."
)
IMPORT_WARNING = re.compile(
    r"warning - Could not find file /(?P<path>snippets/(?:"
    + "|".join(LOCALES)
    + r")/[^\s]+) - imported from /(?:"
    + "|".join(LOCALES)
    + r")/[^\s]*"
)


def page_exists(docs_root: Path, route: str) -> bool:
    path = docs_root / route
    return any(
        candidate.is_file()
        for candidate in (
            path.with_suffix(".mdx"),
            path.with_suffix(".md"),
            path / "index.mdx",
            path / "index.md",
        )
    )


def is_known_false_positive(docs_root: Path, warning: str) -> bool:
    navigation = NAV_WARNING.fullmatch(warning)
    if navigation:
        return page_exists(docs_root, navigation.group("path"))

    imported = IMPORT_WARNING.fullmatch(warning)
    return bool(imported and (docs_root / imported.group("path")).is_file())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("docs_root", nargs="?", default=".")
    args = parser.parse_args(argv)
    docs_root = Path(args.docs_root).resolve()

    environment = os.environ.copy()
    environment.setdefault("NODE_OPTIONS", "--max-old-space-size=8192")
    result = subprocess.run(
        ["mint", "validate"],
        cwd=docs_root,
        env=environment,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    sys.stdout.write(result.stdout)
    if result.returncode == 0:
        return 0

    output = ANSI.sub("", result.stdout).replace("\r", "")
    warnings = [line.strip() for line in output.splitlines() if "warning - " in line]
    terminal_error = re.search(r"error Build validation failed with \d+ warning\(s\)\. See above for details\.", output)
    if warnings and terminal_error and all(is_known_false_positive(docs_root, warning) for warning in warnings):
        print("OK: Mintlify reported only existing localized navigation and snippet paths.")
        return 0
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
