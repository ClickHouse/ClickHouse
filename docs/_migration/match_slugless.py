#!/usr/bin/env python3
"""
Dry-run: find Mintlify pages with no `slug:` frontmatter and try to match
each one to a Docusaurus source file.

Matching rules (in priority order, all case-insensitive on filename):
  1. Exact relative-path match (case-insensitive)
  2. Longest trailing-path-segment match (case-insensitive)
  3. Filename-only match (only if exactly one Docusaurus candidate)

On --apply:
  - Overwrites the Mintlify file with the Docusaurus file's bytes.
  - If the Docusaurus filename uses different casing, the Mintlify file
    is renamed to the Docusaurus casing (lowercase file removed).
  - Updates page-path references in docs.json to the new casing.

Usage:
    python scripts/match_slugless.py \
        --mintlify /path/to/mintlify \
        --docusaurus /path/to/docusaurus
"""

import argparse
import re
import sys
from collections import defaultdict
from pathlib import Path


GREEN = "\033[92m"
RED = "\033[91m"
YELLOW = "\033[93m"
BLUE = "\033[94m"
BOLD = "\033[1m"
RESET = "\033[0m"

SKIP_DIRS = {
    "node_modules", ".git", "snippets", "_snippets",
    "__pycache__", "en", "i18n", "src", "test-results",
    ".playwright-mcp", "tmp", ".mintlify",
}


def has_slug(path: Path) -> bool:
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    m = re.match(r"^---\s*\n(.*?)\n---\s*\n", text, re.DOTALL)
    if not m:
        return False
    for line in m.group(1).splitlines():
        if re.match(r"^\s*slug\s*:\s*\S", line):
            return True
    return False


def walk_md(root: Path):
    for p in root.rglob("*"):
        if p.suffix not in (".md", ".mdx"):
            continue
        if any(part in SKIP_DIRS for part in p.relative_to(root).parts):
            continue
        yield p


def trailing_overlap_ci(a_parts, b_parts) -> int:
    n = 0
    for x, y in zip(reversed(a_parts), reversed(b_parts)):
        if x.lower() == y.lower():
            n += 1
        else:
            break
    return n


def update_docs_json(docs_json_path: Path, renames: list[tuple[str, str]], apply: bool) -> int:
    """Replace old page-path references with new ones. Each rename is
    (old_no_ext, new_no_ext) using POSIX-style relative paths."""
    if not docs_json_path.is_file() or not renames:
        return 0
    text = docs_json_path.read_text(encoding="utf-8")
    n = 0
    for old, new in renames:
        if old == new:
            continue
        # Page entries are JSON strings; match the exact path inside quotes
        # so we don't accidentally rewrite substrings of unrelated paths.
        pat = re.compile(r'"' + re.escape(old) + r'"')
        new_text, count = pat.subn(f'"{new}"', text)
        if count:
            n += count
            text = new_text
    if apply and n:
        docs_json_path.write_text(text, encoding="utf-8")
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mintlify", required=True)
    ap.add_argument("--docusaurus", required=True)
    ap.add_argument("--show", choices=["unique", "ambiguous", "missing", "all", "none"],
                    default="none")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--csv", help="Optional CSV output of mapping decisions")
    ap.add_argument("--apply", action="store_true",
                    help="Overwrite Mintlify files with Docusaurus content. "
                         "Renames to Docusaurus filename casing and updates docs.json.")
    args = ap.parse_args()

    mint_root = Path(args.mintlify).resolve()
    docu_root = Path(args.docusaurus).resolve()

    if not mint_root.is_dir() or not docu_root.is_dir():
        print("Bad paths", file=sys.stderr)
        sys.exit(1)

    docu_by_name_ci: dict[str, list[Path]] = defaultdict(list)
    docu_by_relpath_ci: dict[str, Path] = {}
    for p in walk_md(docu_root):
        rel = p.relative_to(docu_root)
        docu_by_name_ci[p.name.lower()].append(p)
        docu_by_relpath_ci[str(rel).lower()] = p

    print(f"Indexed {sum(len(v) for v in docu_by_name_ci.values())} Docusaurus files "
          f"({len(docu_by_name_ci)} unique filenames, case-insensitive)")

    exact, suffix, filename, ambiguous, missing = [], [], [], [], []

    for p in walk_md(mint_root):
        if has_slug(p):
            continue
        rel = p.relative_to(mint_root)
        rel_str_ci = str(rel).lower()
        candidates = docu_by_name_ci.get(p.name.lower(), [])

        if rel_str_ci in docu_by_relpath_ci:
            exact.append((rel, docu_by_relpath_ci[rel_str_ci].relative_to(docu_root)))
            continue

        if not candidates:
            missing.append(rel)
            continue

        scores = [
            (trailing_overlap_ci(rel.parts, c.relative_to(docu_root).parts), c)
            for c in candidates
        ]
        max_score = max(s for s, _ in scores)
        best = [c for s, c in scores if s == max_score]
        if max_score >= 2 and len(best) == 1:
            suffix.append((rel, best[0].relative_to(docu_root), max_score))
            continue

        if len(candidates) == 1:
            filename.append((rel, candidates[0].relative_to(docu_root)))
            continue

        ambiguous.append((rel, [c.relative_to(docu_root) for c in candidates], max_score, best))

    total_matched = len(exact) + len(suffix) + len(filename)
    total = total_matched + len(ambiguous) + len(missing)

    print(f"\n{BOLD}=== Slug-less Mintlify pages: {total} ==={RESET}")
    print(f"  {GREEN}Exact path match:    {len(exact)}{RESET}")
    print(f"  {GREEN}Suffix path match:   {len(suffix)}{RESET}")
    print(f"  {GREEN}Filename-only match: {len(filename)}{RESET}")
    print(f"  {YELLOW}Ambiguous:           {len(ambiguous)}{RESET}")
    print(f"  {RED}No match:            {len(missing)}{RESET}")
    print(f"  {BOLD}Total auto-matched:  {total_matched} / {total}{RESET}")

    # Build the apply list. New Mintlify path keeps the Mintlify directory
    # but uses the Docusaurus *filename* casing.
    apply_list: list[tuple[Path, Path, Path]] = []  # (old_mint_rel, new_mint_rel, docu_abs)
    for m, d in exact:
        new_name = Path(d).name
        new_rel = m.parent / new_name
        apply_list.append((m, new_rel, docu_root / d))
    for m, d, _ in suffix:
        new_name = Path(d).name
        new_rel = m.parent / new_name
        apply_list.append((m, new_rel, docu_root / d))
    for m, d in filename:
        new_name = Path(d).name
        new_rel = m.parent / new_name
        apply_list.append((m, new_rel, docu_root / d))

    rename_count = sum(1 for old, new, _ in apply_list if old != new)
    print(f"  {BLUE}Casing renames:      {rename_count}{RESET}")

    if args.csv:
        import csv
        with open(args.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["mintlify_old", "mintlify_new", "docusaurus_path", "match_type", "notes"])
            type_map = {}
            for m, d in exact:
                type_map[str(m)] = ("exact", str(d), "")
            for m, d, s in suffix:
                type_map[str(m)] = ("suffix", str(d), f"overlap={s}")
            for m, d in filename:
                type_map[str(m)] = ("filename", str(d), "")
            for old, new, docu in apply_list:
                t, dpath, notes = type_map[str(old)]
                w.writerow([old, new, dpath, t, notes])
            for m, cs, s, best in ambiguous:
                w.writerow([m, "", "", "ambiguous",
                            f"max_overlap={s}; candidates=" +
                            "|".join(str(c) for c in cs)])
            for m in missing:
                w.writerow([m, "", "", "missing", ""])
        print(f"\n  Wrote {args.csv}")

    if args.apply:
        print(f"\n{BOLD}Applying {len(apply_list)} overwrites "
              f"({rename_count} with rename)...{RESET}")
        n_written = 0
        n_renamed = 0
        renames_for_docs_json: list[tuple[str, str]] = []
        for old_rel, new_rel, docu_abs in apply_list:
            old_abs = mint_root / old_rel
            new_abs = mint_root / new_rel
            try:
                new_abs.parent.mkdir(parents=True, exist_ok=True)
                new_abs.write_bytes(docu_abs.read_bytes())
                n_written += 1
                if old_rel != new_rel:
                    # Remove the lowercase file. On case-insensitive FS the
                    # write above may have already overwritten it, so check.
                    if old_abs.exists() and old_abs.resolve() != new_abs.resolve():
                        old_abs.unlink()
                    n_renamed += 1
                    old_no_ext = str(old_rel.with_suffix("")).replace("\\", "/")
                    new_no_ext = str(new_rel.with_suffix("")).replace("\\", "/")
                    renames_for_docs_json.append((old_no_ext, new_no_ext))
            except Exception as e:
                print(f"  {RED}Failed {old_rel}: {e}{RESET}")
        print(f"  {GREEN}Wrote {n_written} files; renamed {n_renamed}{RESET}")

        docs_json = mint_root / "docs.json"
        replaced = update_docs_json(docs_json, renames_for_docs_json, apply=True)
        print(f"  {GREEN}docs.json: replaced {replaced} page-path references{RESET}")

    def show(label, color, items, fmt):
        print(f"\n{BOLD}{color}--- {label} ---{RESET}")
        shown = items if args.limit == 0 else items[: args.limit]
        for it in shown:
            print(fmt(it))
        if args.limit and len(items) > args.limit:
            print(f"  ... ({len(items) - args.limit} more)")

    if args.show in ("unique", "all"):
        show("Exact + suffix + filename", GREEN,
             [(m, d, "exact") for m, d in exact] +
             [(m, d, f"suffix({s})") for m, d, s in suffix] +
             [(m, d, "filename") for m, d in filename],
             lambda x: f"  [{x[2]:>10}]  {x[0]}  <-  {x[1]}")
    if args.show in ("ambiguous", "all"):
        show("Ambiguous", YELLOW, ambiguous,
             lambda x: f"  {x[0]}  (max_overlap={x[2]})\n      candidates: " +
                       ", ".join(str(c) for c in x[1]))
    if args.show in ("missing", "all"):
        show("No match", RED, missing, lambda x: f"  {x}")


if __name__ == "__main__":
    main()
