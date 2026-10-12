#!/usr/bin/env python3
"""Check that every publishable English docs page appears in live navigation.

Run from the docs root (the directory containing ``docs.json``):

    python3 ../ci/jobs/scripts/docs/navigation_check.py .

The translated trees are generated from the English source and have their own
validation. Legacy content, snippets, authoring files, and other paths ignored
by Mintlify are not pages. Quickstart leaf guides are discovered through the
generated quickstart explorer, while its landing page remains in sidebar
navigation. Managed ClickStack onboarding guides are linked from the product
onboarding flow, and the site-wide search page is opened through the search
interface. Those linked pages do not belong in sidebar navigation.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path


LOCALE_DIRS = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}
IGNORED_TOP_LEVEL_DIRS = {
    ".mintlify",
    "_includes",
    "_migration",
    "_site",
    "_templates",
    "changelogs",
    "en",
    "images",
    "img",
    "snippets",
    "tmp",
} | LOCALE_DIRS
IGNORED_FILENAMES = {
    "AGENTS.md",
    "README.md",
    "README.mdx",
    "changelog_entry_guidelines.md",
}
PAGE_SUFFIXES = {".md", ".mdx"}
NAVIGATION_EXEMPT_FILES = {Path("search.mdx")}
NAVIGATION_EXEMPT_DIRS = (Path("clickstack/managed-onboarding"),)
QUICKSTARTS_DIR = Path("get-started/quickstarts")
QUICKSTARTS_HOME = QUICKSTARTS_DIR / "home.mdx"


def is_publishable_page(relative_path: Path) -> bool:
    """Return whether a docs-root-relative path must appear in navigation."""
    if relative_path.suffix not in PAGE_SUFFIXES:
        return False
    if relative_path.parts[0] in IGNORED_TOP_LEVEL_DIRS:
        return False
    if any(part.startswith((".", "_")) for part in relative_path.parts):
        return False
    if relative_path.name in IGNORED_FILENAMES:
        return False
    if relative_path.name.endswith(".draft.mdx"):
        return False
    if relative_path.parts[0] == "drafts":
        return False
    if relative_path in NAVIGATION_EXEMPT_FILES:
        return False
    if relative_path.is_relative_to(QUICKSTARTS_DIR):
        return relative_path == QUICKSTARTS_HOME
    if any(
        relative_path.is_relative_to(directory)
        for directory in NAVIGATION_EXEMPT_DIRS
    ):
        return False
    return True


def page_reference(relative_path: Path) -> str:
    """Convert a Markdown path to the extensionless form used by Mintlify."""
    path = relative_path.as_posix()
    return path[: -len(relative_path.suffix)]


def discover_pages(docs_root: Path) -> dict[str, Path]:
    """Map navigation references to publishable page paths on disk."""
    pages = {}
    for path in docs_root.rglob("*"):
        if not path.is_file():
            continue
        relative_path = path.relative_to(docs_root)
        if is_publishable_page(relative_path):
            pages[page_reference(relative_path)] = relative_path
    return pages


def normalize_navigation_reference(reference: str) -> str | None:
    reference = reference.strip().removeprefix("./")
    if not reference or reference.startswith(("/", "#", "http://", "https://")):
        return None
    for suffix in PAGE_SUFFIXES:
        if reference.endswith(suffix):
            return reference[: -len(suffix)]
    return reference


def collect_navigation_references(node: object) -> set[str]:
    """Collect page entries from ``pages`` and ``root`` navigation fields."""
    references = set()

    def add_page_container(value: object) -> None:
        if isinstance(value, str):
            reference = normalize_navigation_reference(value)
            if reference:
                references.add(reference)
        elif isinstance(value, list):
            for item in value:
                add_page_container(item)
        elif isinstance(value, dict):
            visit(value)

    def visit(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                if key in {"pages", "root"}:
                    add_page_container(item)
                elif isinstance(item, (dict, list)):
                    visit(item)

    visit(node)
    return references


def collect_navigation_file_references(node: object) -> set[str]:
    """Collect navigation fragment paths from ``$ref`` fields."""
    references = set()

    def visit(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                if key == "$ref" and isinstance(item, str):
                    references.add(item)
                elif isinstance(item, (dict, list)):
                    visit(item)

    visit(node)
    return references


def discover_navigation_references(docs_root: Path) -> set[str]:
    docs_root = docs_root.resolve()
    references = set()
    pending = [docs_root / "docs.json"]
    visited = set()

    while pending:
        path = pending.pop()
        if path in visited:
            continue
        visited.add(path)

        navigation = json.loads(path.read_text(encoding="utf-8"))
        references.update(collect_navigation_references(navigation))

        for reference in sorted(collect_navigation_file_references(navigation)):
            file_reference = reference.partition("#")[0]
            if not file_reference or file_reference.startswith(
                ("/", "http://", "https://")
            ):
                continue

            referenced_path = (path.parent / file_reference).resolve()
            try:
                relative_path = referenced_path.relative_to(docs_root)
            except ValueError as error:
                raise ValueError(
                    f"{path}: $ref target {reference!r} escapes the docs root"
                ) from error

            if not relative_path.parts or relative_path.parts[0] not in LOCALE_DIRS:
                pending.append(referenced_path)

    return references


def find_unlisted_pages(docs_root: Path) -> list[Path]:
    pages = discover_pages(docs_root)
    references = discover_navigation_references(docs_root)

    # Mintlify accepts a directory reference for its ``index`` page.
    covered = references | {f"{reference}/index" for reference in references}
    return sorted(path for reference, path in pages.items() if reference not in covered)


def main() -> int:
    docs_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    if not (docs_root / "docs.json").is_file():
        print(f"Error: no docs.json in {docs_root}; pass the docs root.")
        return 2

    try:
        unlisted_pages = find_unlisted_pages(docs_root)
    except (OSError, json.JSONDecodeError, ValueError) as error:
        print(f"Error: could not inspect the docs navigation: {error}")
        return 2

    if unlisted_pages:
        print(
            f"FAIL: {len(unlisted_pages)} documentation page(s) are missing "
            "from navigation:"
        )
        for path in unlisted_pages:
            print(f"- {path.as_posix()}")
        print(
            "Add each page to a `pages` or `root` field in docs.json or a "
            "navigation.json file. The site-wide search page, explorer-managed "
            "quickstart leaf guides, and Managed ClickStack onboarding guides "
            "are exempt."
        )
        return 1

    print("OK: every publishable English documentation page is in navigation")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
