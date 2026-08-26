#!/usr/bin/env python3
"""Reject legacy Docusaurus admonitions in canonical documentation sources."""

import re
import sys
from pathlib import Path


LEGACY_ADMONITION_RE = re.compile(
    r"^[ \t]*:{3,}(?:note|warning|tip|info|caution|danger|important)"
    r"(?:[ \t\[].*)?$",
    re.MULTILINE,
)
TOKEN_SEPARATOR_RE = r"(?:[ \t\r\n]|/\*[\s\S]*?\*/)*"
ESCAPED_ADMONITION_RE = re.compile(
    r":{3,}(?:note|warning|tip|info|caution|danger|important)"
    rf"(?:\\n|[ \t\[]|\"{TOKEN_SEPARATOR_RE}\"(?:\\n|[ \t\[])"
    rf"|'{TOKEN_SEPARATOR_RE}\|\|{TOKEN_SEPARATOR_RE}'(?:\\n|[ \t\[]))",
)
SOURCE_EXTENSIONS = {".cpp", ".h", ".hpp", ".inc"}
# This compatibility test intentionally exercises the legacy renderer syntax.
SOURCE_EXCLUSIONS = {
    Path("Client/tests/gtest_terminal_markdown_renderer.cpp"),
}
DOC_EXTENSIONS = {".md", ".mdx"}
LOCALES = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}


def is_localized_doc(relative_path):
    """Localized pages are read-only output owned by the translation workflow."""
    return relative_path.parts[0] in LOCALES or (
        relative_path.parts[0] == "snippets"
        and len(relative_path.parts) > 1
        and relative_path.parts[1] in LOCALES
    )


def canonical_documentation_files(repo_root):
    source_root = repo_root / "src"
    for path in source_root.rglob("*"):
        if (
            path.suffix in SOURCE_EXTENSIONS
            and path.relative_to(source_root) not in SOURCE_EXCLUSIONS
        ):
            yield path

    docs_root = repo_root / "docs"
    for path in docs_root.rglob("*"):
        if path.suffix not in DOC_EXTENSIONS:
            continue
        relative_path = path.relative_to(docs_root)
        if relative_path.parts[0] == "_migration" or is_localized_doc(relative_path):
            continue
        yield path

    yield from (repo_root / "ci/jobs/scripts/docs/autogenerate/sql").glob("*.sql")


def find_legacy_admonitions(repo_root):
    findings = []
    for path in canonical_documentation_files(repo_root):
        text = path.read_text(encoding="utf-8")
        patterns = [LEGACY_ADMONITION_RE]
        if path.suffix in SOURCE_EXTENSIONS or path.suffix == ".sql":
            patterns.append(ESCAPED_ADMONITION_RE)
        for pattern in patterns:
            for match in pattern.finditer(text):
                line_number = text.count("\n", 0, match.start()) + 1
                findings.append(
                    f"{path.relative_to(repo_root)}:{line_number}:{match.group(0).strip()}"
                )
    return findings


def main():
    repo_root = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
    findings = find_legacy_admonitions(repo_root)
    if findings:
        print("Legacy Docusaurus admonitions found in canonical documentation sources:")
        print("\n".join(findings))
        return 1
    print("OK: canonical documentation sources use Mintlify admonition components.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
