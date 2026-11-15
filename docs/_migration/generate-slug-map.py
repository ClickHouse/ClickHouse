#!/usr/bin/env python3
"""Generate slug-map.csv pairing every Docusaurus slug with its Mintlify URL.

Approach:
  1. Walk the Docusaurus repo (--docusaurus) and collect every page's
     frontmatter `slug:`.
  2. Walk the Mintlify repo (--mintlify, default = this repo) and index pages
     by their frontmatter `slug:`.
  3. For each Docusaurus slug:
       old_url = <old-base>/<slug>
       mintlify_file = path of the Mintlify page with the same slug
       new_url = <mintlify-base>/<mintlify_file without extension>
     If no Mintlify page has that slug, mark `unmatched`.

In Mintlify, a page's URL is its file path relative to docs.json (extension
stripped) — that's why the new URL is built from the file path, not from any
slug field.

Usage:
    python _migration/generate-slug-map.py
    python _migration/generate-slug-map.py --docusaurus ~/Desktop/clickhouse-docs
"""
from __future__ import annotations

import argparse
import csv
import re
import hashlib
from collections import defaultdict
from pathlib import Path

THIS_REPO = Path(__file__).resolve().parent.parent
DEFAULT_DOCUSAURUS = Path.home() / "Desktop" / "clickhouse-docs"
SKIP_DIRS = {"node_modules", ".git", "build", "i18n", ".claude", "scripts", "snippets"}
EXTS = (".md", ".mdx")
SLUG_RE = re.compile(r"^slug:\s*['\"]?(\S+?)['\"]?\s*$", re.MULTILINE)
FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---", re.DOTALL)


def hash_file(p: Path) -> str:
    """Stable short hash of a file's content (first 16 hex chars of SHA-256)."""
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:16]


def collect_knowledgebase(root: Path, kb_subdir: str = "knowledgebase") -> list[tuple[Path, str]]:
    """Yield (relative_path, implicit_slug) for Docusaurus knowledgebase files.

    Knowledgebase pages have no `slug:` field — Docusaurus auto-generates a slug
    from the filename, e.g. /knowledgebase/<basename-without-extension>.
    """
    out = []
    kb = root / kb_subdir
    if not kb.exists():
        return out
    for p in kb.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        rel = p.relative_to(root)
        # Skip i18n / build dirs by virtue of the kb_subdir scope.
        # Implicit slug: /<kb_subdir>/<rel-without-ext-and-without-leading-folder>
        sub = p.relative_to(kb).with_suffix("")
        slug = "/" + kb_subdir + "/" + str(sub).replace("\\", "/")
        out.append((rel, slug))
    return out


def collect(root: Path) -> list[tuple[Path, str]]:
    """Yield (relative_path, slug) for every page in root that has a slug."""
    out = []
    for p in root.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        rel = p.relative_to(root)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        try:
            head = p.read_text(encoding="utf-8", errors="replace")[:2000]
        except OSError:
            continue
        # Only look inside the file's frontmatter block (first --- … ---).
        # Avoids matching example `slug:` lines used in body content (e.g. a
        # style guide showing what frontmatter looks like).
        fm = FRONTMATTER_RE.match(head)
        scan = fm.group(1) if fm else ""
        m = SLUG_RE.search(scan)
        if m:
            out.append((rel, normalize_slug(m.group(1))))
    return out


def normalize_slug(slug: str) -> str:
    """Docusaurus permits slugs with or without a leading slash; canonicalize."""
    return "/" + slug.lstrip("/")


def file_to_url_path(rel_path: Path) -> str:
    """File path relative to docs.json (= repo root) -> Mintlify URL path.

    `foo/index.md` renders at `/foo`, not `/foo/index`. Top-level `index.md`
    renders at `/`.
    """
    stem_path = str(rel_path.with_suffix("")).replace("\\", "/")
    if stem_path == "index":
        return "/"
    if stem_path.endswith("/index"):
        stem_path = stem_path[: -len("/index")]
    return "/" + stem_path


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--docusaurus", type=Path, default=DEFAULT_DOCUSAURUS,
                    help="path to clickhouse-docs (Docusaurus) repo")
    ap.add_argument("--mintlify", type=Path, default=THIS_REPO,
                    help="path to Mintlify repo (default: this repo)")
    ap.add_argument("--out", type=Path, default=THIS_REPO / "_migration" / "slug-map.csv")
    ap.add_argument("--old-base", default="https://clickhouse.com/docs")
    ap.add_argument("--mintlify-base", default="https://private-7c7dfe99.mintlify.app")
    args = ap.parse_args()

    if not args.docusaurus.exists():
        ap.error(f"Docusaurus repo not found at {args.docusaurus}")
    if not args.mintlify.exists():
        ap.error(f"Mintlify repo not found at {args.mintlify}")

    docu_pages = collect(args.docusaurus)
    docu_kb = collect_knowledgebase(args.docusaurus)
    docu_pages.extend(docu_kb)

    mint_pages = collect(args.mintlify)

    mint_by_slug: dict[str, list[Path]] = defaultdict(list)
    for rel, slug in mint_pages:
        mint_by_slug[slug].append(rel)

    # Index Mintlify KB files by basename for implicit-slug pairing
    # (KB pages don't carry a `slug:` field on either side).
    mint_kb_root = args.mintlify / "resources" / "support-center" / "knowledge-base"
    mint_kb_by_basename: dict[str, list[Path]] = defaultdict(list)
    if mint_kb_root.exists():
        for p in mint_kb_root.rglob("*"):
            if p.is_file() and p.suffix in EXTS:
                mint_kb_by_basename[p.name].append(p.relative_to(args.mintlify))

    # Preserve tracking columns from any existing CSV so re-running doesn't wipe progress.
    existing_flags: dict[str, dict[str, str]] = {}
    if args.out.exists():
        with args.out.open(encoding="utf-8", newline="") as fh:
            for row in csv.DictReader(fh):
                key = row.get("docusaurus_slug", "")
                if key:
                    existing_flags[key] = {
                        "migrated": (row.get("migrated") or "false").strip().lower(),
                        "manually_checked": (row.get("manually_checked") or "false").strip().lower(),
                        "migrated_hash": (row.get("migrated_hash") or "").strip(),
                        "migrated_at": (row.get("migrated_at") or "").strip(),
                    }

    rows = []
    counts = {"matched": 0, "unmatched": 0, "ambiguous": 0}
    old_base = args.old_base.rstrip("/")
    new_base = args.mintlify_base.rstrip("/")

    for docu_path, slug in sorted(docu_pages, key=lambda x: x[1]):
        old_url = f"{old_base}{slug}"
        source_hash = hash_file(args.docusaurus / docu_path)
        candidates = mint_by_slug.get(slug, [])

        # Knowledgebase fallback: pair by basename when no slug match exists,
        # since KB files don't declare slugs on either side.
        if not candidates and slug.startswith("/knowledgebase/"):
            candidates = mint_kb_by_basename.get(docu_path.name, [])

        if len(candidates) == 1:
            mint_file = str(candidates[0])
            new_url = f"{new_base}{file_to_url_path(candidates[0])}"
            status = "matched"
        elif len(candidates) > 1:
            mint_file = " | ".join(str(c) for c in candidates)
            new_url = ""
            status = "ambiguous"
        else:
            mint_file = ""
            new_url = ""
            status = "unmatched"
        counts[status] += 1

        flags = existing_flags.get(slug, {})
        rows.append({
            "docusaurus_slug": slug,
            "docusaurus_file": str(docu_path),
            "mintlify_file": mint_file,
            "old_url": old_url,
            "new_url": new_url,
            "status": status,
            "source_hash": source_hash,
            "migrated": flags.get("migrated", "false"),
            "migrated_hash": flags.get("migrated_hash", ""),
            "migrated_at": flags.get("migrated_at", ""),
            "manually_checked": flags.get("manually_checked", "false"),
        })

    with args.out.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {args.out} with {len(rows)} rows")
    for k, v in counts.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    main()