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
ESCAPED_ADMONITION_RE = re.compile(
    r":{3,}(?:note|warning|tip|info|caution|danger|important)(?:\\n|[ \t\[])",
)
SOURCE_EXTENSIONS = {".cpp", ".h", ".hpp", ".inc"}
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
    for path in (repo_root / "src").rglob("*"):
        if path.suffix in SOURCE_EXTENSIONS:
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
        pattern = (
            ESCAPED_ADMONITION_RE
            if path.suffix == ".sql"
            else LEGACY_ADMONITION_RE
        )
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
