#!/usr/bin/env python3
"""Audit legacy documentation against the exact migration snapshot.

The slug map records the SHA-256 prefix of every Docusaurus source page at the
time it was migrated. This script compares an explicit Git ref from
ClickHouse/clickhouse-docs with that snapshot and reports:

* mapped source pages whose content changed after migration;
* mapped pages whose Mintlify destination is missing;
* source pages added after the manifest was generated.

The output is an inventory, not a claim that every changed source page is still
missing. Each changed page must be reconciled with its mapped destination,
because a later main-repository PR may have ported or superseded it.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
from collections import Counter
from pathlib import Path


PAGE_SUFFIXES = {".md", ".mdx"}
SOURCE_ROOTS = ("docs", "knowledgebase", "src/pages")


def content_hash(content: bytes) -> str:
    """Return the hash format stored in slug-map.csv."""
    return hashlib.sha256(content).hexdigest()[:16]


def load_manifest(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as source:
        return list(csv.DictReader(source))


def git_output(repository: Path, *arguments: str, text: bool = True):
    return subprocess.check_output(
        ["git", "-C", str(repository), *arguments],
        text=text,
        stderr=subprocess.DEVNULL,
    )


def is_source_page(name: str, content: bytes) -> bool:
    """Exclude snippets and support files that were never standalone pages."""
    path = Path(name)
    if path.name.lower() == "readme.md" or any(part.startswith("_") for part in path.parts):
        return False
    text = content.decode(encoding="utf-8", errors="replace")
    if not text.startswith("---"):
        return False
    frontmatter = text.split("---", 2)[1]
    return any(line.startswith(("title:", "slug:")) for line in frontmatter.splitlines())


def audit(clickhouse_root: Path, legacy_repository: Path, legacy_ref: str, manifest: Path) -> dict:
    rows = load_manifest(manifest)
    legacy_commit = git_output(legacy_repository, "rev-parse", "--verify", f"{legacy_ref}^{{commit}}").strip()
    source_names = set(
        git_output(legacy_repository, "ls-tree", "-r", "--name-only", legacy_commit).splitlines()
    )

    content_cache: dict[str, bytes] = {}

    def source_content(name: str) -> bytes:
        if name not in content_cache:
            content_cache[name] = git_output(
                legacy_repository,
                "show",
                f"{legacy_commit}:{name}",
                text=False,
            )
        return content_cache[name]

    mapped_sources: set[str] = set()
    mapped: list[dict[str, str | None]] = []

    for row in rows:
        source_name = row["docusaurus_file"]
        if not source_name:
            continue
        mapped_sources.add(source_name)

        destination_name = row["mintlify_file"].split(" | ")[0]
        destination_path = clickhouse_root / "docs" / destination_name if destination_name else None
        baseline_hash = row["migrated_hash"] or row["source_hash"]

        if source_name not in source_names:
            source_state = "deleted"
            current_hash = None
        else:
            current_hash = content_hash(source_content(source_name))
            source_state = "changed" if baseline_hash and current_hash != baseline_hash else "unchanged"

        if not destination_name:
            destination_state = "unmapped"
        elif destination_path and destination_path.exists():
            destination_state = "present"
        else:
            destination_state = "missing"

        mapped.append(
            {
                "source": source_name,
                "destination": destination_name or None,
                "source_state": source_state,
                "destination_state": destination_state,
                "snapshot_hash": baseline_hash or None,
                "current_hash": current_hash,
                "migrated_at": row["migrated_at"] or None,
            }
        )

    new_sources: list[str] = []
    for root_name in SOURCE_ROOTS:
        prefix = root_name + "/"
        for name in sorted(source_names):
            path = Path(name)
            if not name.startswith(prefix) or path.suffix not in PAGE_SUFFIXES:
                continue
            if not is_source_page(name, source_content(name)):
                continue
            if name not in mapped_sources:
                new_sources.append(name)

    counts = Counter(item["source_state"] for item in mapped)
    counts.update(f"destination_{item['destination_state']}" for item in mapped)
    counts["new_sources"] = len(new_sources)

    return {
        "manifest": str(manifest),
        "legacy_repository": str(legacy_repository),
        "legacy_ref": legacy_ref,
        "legacy_commit": legacy_commit,
        "counts": dict(sorted(counts.items())),
        "changed": [item for item in mapped if item["source_state"] == "changed"],
        "missing_destinations": [item for item in mapped if item["destination_state"] == "missing"],
        "new_sources": new_sources,
    }


def render_markdown(report: dict) -> str:
    counts = report["counts"]
    lines = [
        "# Legacy documentation source drift audit",
        "",
        f"- Legacy source commit: `{report['legacy_commit']}` (`{report['legacy_ref']}`)",
        f"- Changed mapped source pages: {counts.get('changed', 0)}",
        f"- New source pages absent from the manifest: {counts.get('new_sources', 0)}",
        f"- Missing mapped destinations: {counts.get('destination_missing', 0)}",
        "",
        "A changed source page is a reconciliation candidate, not automatically a",
        "missing page. Compare it with the destination to classify it as ported,",
        "superseded, partially ported, or missing.",
        "",
        "## Changed mapped pages",
        "",
        "| Legacy source | Main-repository destination | Snapshot hash | Current hash |",
        "|---|---|---|---|",
    ]
    for item in report["changed"]:
        lines.append(
            f"| `{item['source']}` | `{item['destination'] or 'unmapped'}` | "
            f"`{item['snapshot_hash'] or ''}` | `{item['current_hash'] or ''}` |"
        )

    lines.extend(["", "## New source pages absent from the manifest", ""])
    if report["new_sources"]:
        lines.extend(f"- `{source}`" for source in report["new_sources"])
    else:
        lines.append("None.")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("legacy_repository", type=Path, help="ClickHouse/clickhouse-docs Git repository")
    parser.add_argument(
        "--ref",
        required=True,
        help="explicit source ref or commit to audit, for example origin/main",
    )
    parser.add_argument(
        "--clickhouse-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="ClickHouse checkout root",
    )
    parser.add_argument("--manifest", type=Path, help="slug-map.csv override")
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--fail-on-drift", action="store_true")
    args = parser.parse_args()

    clickhouse_root = args.clickhouse_root.resolve()
    legacy_repository = args.legacy_repository.resolve()
    manifest = (args.manifest or clickhouse_root / "docs" / "_migration" / "slug-map.csv").resolve()

    report = audit(clickhouse_root, legacy_repository, args.ref, manifest)
    output = json.dumps(report, indent=2, sort_keys=True) + "\n" if args.format == "json" else render_markdown(report)
    if args.output:
        args.output.write_text(output, encoding="utf-8")
    else:
        print(output, end="")

    has_drift = bool(report["changed"] or report["new_sources"] or report["missing_destinations"])
    return 1 if args.fail_on_drift and has_drift else 0


if __name__ == "__main__":
    raise SystemExit(main())
