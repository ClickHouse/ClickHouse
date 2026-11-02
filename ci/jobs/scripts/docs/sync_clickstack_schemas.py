#!/usr/bin/env python3
"""Sync canonical ClickStack OTel schema snippets from upstream HyperDX.

The canonical English snippets in ``docs/snippets/clickstack/`` embed the
upstream source URL and schema name in a ``sync-clickstack-schema`` marker.
Each marker owns the SQL fence immediately following it. Localized snippets
are translated copies and are deliberately left untouched.
"""

from __future__ import annotations

import re
import sys
import urllib.request
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
SNIPPETS_DIR = REPO_ROOT / "docs/snippets/clickstack"

BLOCK_RE = re.compile(
    r"\{/\* sync-clickstack-schema: id=(?P<id>\S+) source=(?P<source>\S+) \*/\}\n"
    r"```sql\n(?P<body>.*?)\n```",
    re.DOTALL,
)


def fetch_text(url: str) -> str:
    """Fetch one trusted HyperDX schema source with a bounded request."""
    with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
        return response.read().decode("utf-8")


def extract_create_table(sql: str, table_id: str) -> str:
    """Extract the schema statement for ``table_id`` from an upstream seed file."""
    cleaned = "\n".join(
        line for line in sql.splitlines() if not re.match(r"--\s*\+goose\b", line)
    )
    pattern = re.compile(
        r"CREATE TABLE(?:\s+IF NOT EXISTS)?\s+\$\{DATABASE\}\."
        + re.escape(table_id)
        + r"\b.*?;",
        re.DOTALL,
    )
    match = pattern.search(cleaned)
    if not match:
        raise RuntimeError(f'Could not find CREATE TABLE for id="{table_id}"')
    return match.group(0).strip()


def sync_file(path: Path, source_cache: dict[str, str]) -> bool:
    """Synchronize every marked schema fence in ``path`` and return whether it changed."""
    original = path.read_text(encoding="utf-8")
    blocks = list(BLOCK_RE.finditer(original))
    if not blocks:
        print(f"[skip] no sync-clickstack-schema marker in {path}")
        return False

    updated = original
    for block in blocks:
        table_id = block.group("id")
        source = block.group("source")
        if source not in source_cache:
            print(f"  fetching {source}")
            source_cache[source] = fetch_text(source)
        create_table = extract_create_table(source_cache[source], table_id)
        replacement = (
            f"{{/* sync-clickstack-schema: id={table_id} source={source} */}}\n"
            "```sql\n"
            f"{create_table}\n"
            "```"
        )
        updated = updated.replace(block.group(0), replacement)

    if updated == original:
        print(f"[unchanged] {path}")
        return False

    path.write_text(updated, encoding="utf-8")
    print(f"[updated]   {path}")
    return True


def main() -> int:
    targets = sorted(SNIPPETS_DIR.glob("_schema_*.mdx"))
    if not targets:
        raise RuntimeError(f"No schema snippets found in {SNIPPETS_DIR}")

    source_cache: dict[str, str] = {}
    changed = False
    for target in targets:
        changed |= sync_file(target, source_cache)

    print("done" + (" (changes detected)" if changed else " (no changes)"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
