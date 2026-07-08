#!/usr/bin/env python3
"""Suggest target URLs for every unresolved slug in the migrated repo.

Walks every .md/.mdx file looking for `<!-- MIGRATE: unknown slug -->` markers,
collects the unique slugs that failed to resolve, and proposes the most-likely
current Mintlify page for each based on:

  1. clickhouse-main redirects.json (authoritative when it has an entry).
  2. Basename + parent-dir match against this repo's actual file tree.
  3. Basename-only match against Mintlify URLs derived from slug-map.csv.

Output: _migration/slug-aliases.csv with columns
    old_slug, count, suggested_target, confidence, alternates

Review/edit that file, then run _migration/apply-slug-aliases.py to rewrite the
markers.

Usage:
    python _migration/suggest-slug-aliases.py
    python _migration/suggest-slug-aliases.py --reference ~/Desktop/clickhouse-main
"""
from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path

THIS_REPO = Path(__file__).resolve().parent.parent
DEFAULT_REFERENCE = Path.home() / "Desktop" / "clickhouse-main"
SKIP_DIRS = {"node_modules", ".git", "build", "i18n", ".claude", "scripts", "snippets"}
EXTS = (".md", ".mdx")
MARKER_RE = re.compile(r"\(([^)]*)\s*<!--\s*MIGRATE: unknown slug\s*-->\)")


def file_to_url(rel_path: str) -> str:
    s = rel_path.rsplit(".", 1)[0].replace("\\", "/")
    if s == "index":
        return "/"
    if s.endswith("/index"):
        s = s[: -len("/index")]
    return "/" + s


def collect_unknown_slugs() -> Counter:
    counts = Counter()
    for p in THIS_REPO.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        rel = p.relative_to(THIS_REPO)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        try:
            text = p.read_text(encoding="utf-8")
        except OSError:
            continue
        if "MIGRATE: unknown slug" not in text:
            continue
        for m in MARKER_RE.finditer(text):
            href = m.group(1).split()[0]
            # Skip noise that isn't a real link target (no /, no http).
            if not href.startswith(("/", "http://", "https://")):
                continue
            counts[href] += 1
    return counts


def load_reference_redirects(reference_root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    f = reference_root / "redirects.json"
    if not f.exists():
        return out
    for r in json.loads(f.read_text(encoding="utf-8")):
        src = r.get("source", "")
        dst = r.get("destination", "")
        if src and dst:
            out[src.removeprefix("/docs")] = dst.removeprefix("/docs")
    return out


def load_slug_map() -> tuple[dict[str, str], list[str]]:
    """Return (slug -> mintlify_url, [all mintlify URLs])."""
    slug_to_url: dict[str, str] = {}
    urls: list[str] = []
    for r in csv.DictReader((THIS_REPO / "_migration" / "slug-map.csv").open(encoding="utf-8")):
        mfile = r["mintlify_file"].split(" | ")[0]
        if r["status"] == "matched" and mfile:
            url = file_to_url(mfile)
            slug_to_url[r["docusaurus_slug"]] = url
            urls.append(url)
    return slug_to_url, sorted(set(urls))


def normalize(slug: str) -> str:
    """Reduce a slug to a basename-style key for fuzzy match."""
    s = slug.lower()
    # Strip fragment first, then any extension and trailing slashes.
    if "#" in s:
        s = s.split("#", 1)[0]
    s = s.rstrip("/")
    for tail in ("/index.mdx", "/index.md", ".mdx", ".md"):
        if s.endswith(tail):
            s = s[: -len(tail)]
            break
    s = s.rstrip("/")
    return s


def basename(s: str) -> str:
    return s.rsplit("/", 1)[-1].replace("_", "-")


def parent(s: str) -> str:
    parts = s.rstrip("/").split("/")
    return parts[-2].replace("_", "-") if len(parts) >= 2 else ""


def suggest(slug: str, urls: list[str], redirects: dict[str, str], slug_to_url: dict[str, str]) -> tuple[str, str, list[str]]:
    """Return (suggested_target, confidence, alternates)."""
    norm = normalize(slug)

    # 1. Use reference redirects if present (highest confidence).
    if norm in redirects:
        target_slug = redirects[norm]
        # Translate target_slug -> Mintlify URL via slug_to_url, else keep as-is.
        if target_slug in slug_to_url:
            return slug_to_url[target_slug], "redirect-mapped", []
        return target_slug, "redirect-direct", []

    # 2. Basename + parent dir match against URLs.
    base = basename(norm)
    par = parent(norm)
    if not base:
        return "", "", []

    by_basename = [u for u in urls if u.rsplit("/", 1)[-1] == base]
    if len(by_basename) == 1:
        return by_basename[0], "basename-unique", []
    if len(by_basename) > 1:
        # narrow by parent dir
        narrowed = [u for u in by_basename if u.rstrip("/").split("/")[-2:-1] == [par]] if par else []
        if len(narrowed) == 1:
            return narrowed[0], "basename+parent-unique", [u for u in by_basename if u not in narrowed]
        return by_basename[0], "basename-ambiguous", by_basename[1:6]

    # 3. Looser: any URL containing the basename as a path segment.
    contains = [u for u in urls if f"/{base}" in u or f"/{base}/" in u]
    if len(contains) == 1:
        return contains[0], "contains-unique", []
    if contains:
        return contains[0], "contains-ambiguous", contains[1:6]

    return "", "no-match", []


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--reference", type=Path, default=DEFAULT_REFERENCE,
                    help="path to clickhouse-main reference repo")
    ap.add_argument("--out", type=Path, default=THIS_REPO / "_migration" / "slug-aliases.csv")
    args = ap.parse_args()

    counts = collect_unknown_slugs()
    print(f"Found {sum(counts.values())} unknown-slug markers across {len(counts)} unique slugs.")

    redirects = load_reference_redirects(args.reference) if args.reference.exists() else {}
    print(f"Loaded {len(redirects)} reference redirects.")

    slug_to_url, urls = load_slug_map()
    print(f"Loaded {len(urls)} known Mintlify URLs.")

    rows = []
    for slug, n in counts.most_common():
        target, conf, alts = suggest(slug, urls, redirects, slug_to_url)
        rows.append({
            "old_slug": slug,
            "count": n,
            "suggested_target": target,
            "confidence": conf,
            "alternates": " | ".join(alts),
        })

    args.out.parent.mkdir(parents=True, exist_ok=True)
    with args.out.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        w.writeheader()
        w.writerows(rows)

    by_conf = Counter(r["confidence"] for r in rows)
    print(f"\nWrote {args.out} with {len(rows)} rows")
    for k, v in by_conf.most_common():
        print(f"  {v:5d}  {k}")


if __name__ == "__main__":
    main()