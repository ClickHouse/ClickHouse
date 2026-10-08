#!/usr/bin/env python3
"""Rewrite `<!-- MIGRATE: unknown slug -->` markers using _migration/slug-aliases.csv.

Reads the alias CSV produced by suggest-slug-aliases.py and rewrites every
marker whose old_slug appears with a non-empty `suggested_target`.

By default only high-confidence aliases are applied (basename-unique,
basename+parent-unique, redirect-direct, redirect-mapped). Pass
--include-ambiguous or --include-contains to opt in to lower-confidence rows.

Fragments (`#anchor`) on the original link are preserved.

Usage:
    python _migration/apply-slug-aliases.py
    python _migration/apply-slug-aliases.py --dry-run
    python _migration/apply-slug-aliases.py --include-ambiguous
"""
from __future__ import annotations

import argparse
import csv
import re
from collections import Counter
from pathlib import Path

THIS_REPO = Path(__file__).resolve().parent.parent
SKIP_DIRS = {"node_modules", ".git", "build", "i18n", ".claude", ".mintlify", "scripts"}
EXTS = (".md", ".mdx")

# Match either `(slug <!-- ... -->)` (markdown link) or `"slug <!-- ... -->"`
# (JSX/HTML href attribute) with the slug captured.
MARKER_RE = re.compile(r'\((?P<slug>[^)\s]+)\s*<!--\s*MIGRATE: unknown slug\s*-->\)')
HTML_MARKER_RE = re.compile(r'"(?P<slug>[^"\s]+)\s*<!--\s*MIGRATE: unknown slug\s*-->\s*"')

HIGH = {"basename-unique", "basename+parent-unique", "redirect-direct", "redirect-mapped"}
AMBIG = {"basename-ambiguous"}
CONTAINS = {"contains-unique", "contains-ambiguous"}
ALWAYS = {"manual"}  # tag any row this way to mark it hand-reviewed


def split_frag(href: str) -> tuple[str, str]:
    if "#" in href:
        a, b = href.split("#", 1)
        return a, "#" + b
    return href, ""


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--aliases", type=Path, default=THIS_REPO / "_migration" / "slug-aliases.csv")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--include-ambiguous", action="store_true",
                    help="also apply basename-ambiguous suggestions")
    ap.add_argument("--include-contains", action="store_true",
                    help="also apply contains-* suggestions")
    args = ap.parse_args()

    if not args.aliases.exists():
        ap.error(f"alias CSV not found at {args.aliases}; run suggest-slug-aliases.py first")

    accept = set(HIGH) | set(ALWAYS)
    if args.include_ambiguous:
        accept |= AMBIG
    if args.include_contains:
        accept |= CONTAINS

    alias_map: dict[str, str] = {}
    for r in csv.DictReader(args.aliases.open(encoding="utf-8")):
        if r["confidence"] in accept and r["suggested_target"]:
            alias_map[r["old_slug"]] = r["suggested_target"]

    print(f"Loaded {len(alias_map)} alias(es) at confidence levels: {', '.join(sorted(accept))}")

    rewrites = Counter()
    files_touched = 0
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

        def make_repl(open_ch: str, close_ch: str):
            def repl(m: re.Match) -> str:
                href = m.group("slug")
                bare, frag = split_frag(href)
                # Try exact match first (alias key may include a fragment). If
                # so, drop the source fragment — the user picked a specific
                # target.
                target = alias_map.get(href)
                if target is not None:
                    rewrites[href] += 1
                    return f"{open_ch}{target}{close_ch}"
                target = alias_map.get(bare)
                if target is None:
                    return m.group(0)
                rewrites[bare] += 1
                return f"{open_ch}{target}{frag}{close_ch}"
            return repl

        new_text = MARKER_RE.sub(make_repl("(", ")"), text)
        new_text = HTML_MARKER_RE.sub(make_repl('"', '"'), new_text)
        if new_text != text:
            files_touched += 1
            if not args.dry_run:
                p.write_text(new_text, encoding="utf-8")

    total = sum(rewrites.values())
    print(f"Rewrote {total} link(s) across {files_touched} file(s)" + (" (dry-run)" if args.dry_run else ""))
    print(f"Distinct aliases used: {len(rewrites)} of {len(alias_map)} loaded")


if __name__ == "__main__":
    main()
