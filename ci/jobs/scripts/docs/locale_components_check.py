#!/usr/bin/env python3
"""Validate navigation links inside localized components and page data.

The published locale pages render JSX components (QuickStartsGrid,
SampleDatasetExplorer, KBExplorer, ...) and MDX `export const` data whose card
navigation lives in `href:`/`to:` string literals -- not markdown links. lychee
neither sees `snippets/<locale>/...` nor parses JSX/JS, so `--mode locale-links`
cannot catch when a localized component routes users to the wrong place.

For every localized file (under `<locale>/` and `snippets/<locale>/`), check:

  * static `href`/`to` paths: one already under `/<locale>/` must resolve; an
    unprefixed path whose localized counterpart `/<locale>/...` EXISTS is a
    regression (routes readers to English instead of the localized page; if no
    localized counterpart exists, English is an acceptable fallback);
  * template-literal href bases, e.g. `` `/get-started/quickstarts/${id}` `` --
    the fallback the "featured" cards render -- flagged when localized pages
    exist under the base (GT copies the English base verbatim into every locale);
  * `image`/`img`/`src` asset refs to /images or /assets must exist on disk
    (catches stale JS data left over from an old English structure).

`--fix` rewrites the href/template regressions to the localized path. Asset
issues are report-only (a broken image needs a content decision). Without --fix
the script only reports and exits non-zero when violations remain.
"""
import argparse
import json
import os
import re
import sys

LOCALE_DIRS = ["ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"]
EXTS = (".mdx", ".md", ".jsx", ".tsx", ".js")
# Non-page asset/base paths that are legitimately unprefixed. `/docs/` is the
# production mount some shared components hardcode (identically in English), not
# a repo-relative doc path, so it is out of scope for the locale check.
SKIP_PREFIXES = ("/images/", "/assets/", "/_site/", "/.well-known/", "/docs/")
SKIP_EXACT = {"/docs", "/"}
# `href: "/x"`, `href="/x"`, `href={'/x'}`, `to: "/x"`, ...
HREF = re.compile(r"""\b(?:href|to)\s*[:=]\s*\{?\s*(['"`])(/[^'"`\s]+)\1""")
# A template literal whose static prefix is a doc path, e.g.
# `` `/get-started/quickstarts/${f.id}` `` -- the href fallbacks in
# QuickStartsGrid/KBExplorer. lychee and the static HREF pattern both miss these,
# yet they are exactly what the "featured" cards render. Capture the static base.
TEMPLATE = re.compile(r"`(/[A-Za-z0-9][A-Za-z0-9/_.#-]*)\$\{")
# `image:`/`img=`/`src=` asset refs to /images or /assets. These live in JS data
# (e.g. a stale `featuredQuickstarts` image left over from an old English
# structure) that lychee never checks; the referenced file must exist on disk.
ASSET = re.compile(r"""\b(?:image|img|src)\s*[:=]\s*\{?\s*(['"`])(/(?:images|assets)/[^'"`\s]+)\1""")


def build_targets(docs_root):
    pages = set()
    for root, dirs, files in os.walk(docs_root):
        dirs[:] = [d for d in dirs if d not in (".git", "node_modules")]
        for n in files:
            if n.endswith((".mdx", ".md")):
                rel = os.path.relpath(os.path.join(root, n), docs_root)
                pages.add(re.sub(r"\.mdx?$", "", rel))
    redirects = set()
    rj = os.path.join(docs_root, "_site", "redirects.json")
    if os.path.isfile(rj):
        for r in json.load(open(rj)):
            s = (r.get("source") or "").strip().strip("/")
            if s:
                redirects.add(s)
    return pages, redirects


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("docs_root", nargs="?", default=".")
    p.add_argument("--fix", action="store_true", help="Rewrite regressions in place.")
    args = p.parse_args(argv)
    docs_root = os.path.abspath(args.docs_root)
    pages, redirects = build_targets(docs_root)

    def resolves(bare):
        return bare in pages or (bare + "/index") in pages or bare in redirects

    violations = []   # (file, path, kind, suggestion)
    fixed = 0
    for loc in LOCALE_DIRS:
        roots = [os.path.join(docs_root, loc),
                 os.path.join(docs_root, "snippets", loc)]
        for base in roots:
            for root, dirs, files in os.walk(base):
                dirs[:] = [d for d in dirs if d not in (".git", "node_modules")]
                for n in files:
                    if not n.endswith(EXTS):
                        continue
                    fp = os.path.join(root, n)
                    rel = os.path.relpath(fp, docs_root)
                    s = open(fp, encoding="utf-8", errors="replace").read()

                    def check(m):
                        nonlocal fixed
                        path = m.group(2)
                        raw = path
                        path = path.split("#")[0].split("?")[0]
                        if (path in SKIP_EXACT or path.startswith(SKIP_PREFIXES)):
                            return m.group(0)
                        bare = path.lstrip("/")
                        seg = bare.split("/")
                        if seg and seg[0] == loc:
                            # already localized -- must resolve
                            if not resolves(bare):
                                violations.append((rel, raw, "broken-localized", None))
                            return m.group(0)
                        # unprefixed: localized counterpart exists => must localize
                        localized = f"{loc}/{bare}"
                        if resolves(localized):
                            suggestion = "/" + loc + raw  # keep fragment
                            violations.append((rel, raw, "should-localize", suggestion))
                            if args.fix:
                                fixed += 1
                                return m.group(0).replace(raw, "/" + loc + raw, 1)
                            return m.group(0)
                        if not resolves(bare):
                            violations.append((rel, raw, "broken", None))
                        return m.group(0)

                    def check_template(m):
                        nonlocal fixed
                        tbase = m.group(1).rstrip("/")
                        if tbase in SKIP_EXACT or tbase.startswith(SKIP_PREFIXES):
                            return m.group(0)
                        bare = tbase.lstrip("/")
                        if bare.split("/")[0] == loc:
                            return m.group(0)  # base already localized
                        # A template building doc URLs from a dynamic id: flag only
                        # when localized pages actually exist under this base (so an
                        # English-only section stays an acceptable fallback). The
                        # per-id target can't be resolved statically; the base is
                        # what routes locale readers to English.
                        if not any(p.startswith(f"{loc}/{bare}/") for p in pages):
                            return m.group(0)
                        violations.append(
                            (rel, m.group(1), "should-localize-template",
                             "/" + loc + m.group(1)))
                        if args.fix:
                            fixed += 1
                            return "`/" + loc + m.group(1) + "${"
                        return m.group(0)

                    ns = TEMPLATE.sub(check_template, HREF.sub(check, s))
                    if args.fix and ns != s:
                        open(fp, "w", encoding="utf-8").write(ns)

                    # Asset refs are not rewritten (a broken image needs a content
                    # decision, not a mechanical fix) -- report only.
                    for am in ASSET.finditer(s):
                        ap = am.group(2).split("#")[0].split("?")[0]
                        if not os.path.exists(os.path.join(docs_root, ap.lstrip("/"))):
                            violations.append((rel, am.group(2), "broken-asset", None))

    kinds = {}
    for _, _, k, _ in violations:
        kinds[k] = kinds.get(k, 0) + 1
    if args.fix:
        print(f"fixed (localized): {fixed}")
    FIXABLE = {"should-localize", "should-localize-template"}
    remaining = [v for v in violations if not (args.fix and v[2] in FIXABLE)]
    print(f"violations: {len(remaining)}  by kind: {kinds}")
    for rel, raw, k, sug in remaining[:40]:
        print(f"  [{k}] {raw}  in {rel}" + (f"  -> {sug}" if sug else ""))
    return 0 if not remaining else 1


if __name__ == "__main__":
    sys.exit(main())
