#!/usr/bin/env python3
"""Find .mdx pages on disk that have no entry in docs.json."""
import json
from collections import defaultdict
from pathlib import Path

REPO = Path("/Users/sstruw/Desktop/mintlify-docs-dev")
SKIP_DIRS = {
    ".git", "node_modules", ".playwright-mcp", ".idea", ".mintlify", ".claude",
    "snippets", "_snippets", "_site", "_migration",
    "i18n",  # localization, not in default nav
}
SKIP_NAMES = {"AGENTS.md", "README.md", "changelog_entry_guidelines.mdx"}
# Path prefixes whose pages are intentionally outside docs.json (e.g. wired
# via a dynamic explorer component instead of the sidebar nav).
SKIP_PREFIXES = (
    "core/get-started/quickstarts/",
)


def collect_disk_pages() -> set[str]:
    pages = set()
    for path in REPO.rglob("*"):
        if not path.is_file():
            continue
        if path.suffix not in {".mdx", ".md"}:
            continue
        parts = path.relative_to(REPO).parts
        if set(parts) & SKIP_DIRS:
            continue
        # Skip partials (underscore-prefixed file or any underscore-prefixed dir)
        if any(p.startswith("_") for p in parts):
            continue
        rel = path.relative_to(REPO).as_posix()
        if rel in SKIP_NAMES or path.name == "README.md":
            continue
        ref = rel[:-4] if rel.endswith(".mdx") else rel[:-3]
        if any(ref.startswith(p) for p in SKIP_PREFIXES):
            continue
        pages.add(ref)
    return pages


def collect_docs_json_refs(node) -> set[str]:
    refs = set()

    def visit(obj):
        if isinstance(obj, list):
            for item in obj:
                visit(item)
        elif isinstance(obj, dict):
            # Follow $ref includes (e.g. products/kubernetes-operator/navigation.json)
            if "$ref" in obj:
                ref_path = obj["$ref"]
                ref_file = REPO / ref_path
                if ref_file.exists():
                    visit(json.loads(ref_file.read_text()))
                return
            for k, v in obj.items():
                if k in ("pages", "groups"):
                    visit(v)
                elif k == "root" and isinstance(v, str):
                    refs.add(v)
                elif isinstance(v, (dict, list)):
                    visit(v)
        elif isinstance(obj, str):
            if obj.startswith(("http://", "https://", "/")):
                return
            if obj.endswith((".json", ".js", ".css", ".svg", ".png", ".jpg", ".ico")):
                return
            refs.add(obj)

    visit(node)
    return refs


def main():
    docs_json = json.loads((REPO / "docs.json").read_text())
    disk = collect_disk_pages()
    referenced = collect_docs_json_refs(docs_json)

    # A page referenced as `X` also covers `X/index` (Mintlify auto-routes both)
    expanded = set(referenced)
    for r in referenced:
        expanded.add(r + "/index")

    orphans = sorted(disk - expanded)

    # Group by top-level section
    by_section = defaultdict(list)
    for o in orphans:
        section = o.split("/", 1)[0]
        by_section[section].append(o)

    print(f"Disk pages: {len(disk)}")
    print(f"docs.json refs: {len(referenced)}")
    print(f"Orphans: {len(orphans)}\n")
    for section in sorted(by_section):
        print(f"=== {section} ({len(by_section[section])}) ===")
        for p in by_section[section]:
            print(f"  {p}")
        print()


if __name__ == "__main__":
    main()