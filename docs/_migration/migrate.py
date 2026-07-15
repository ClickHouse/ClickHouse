#!/usr/bin/env python3
"""Migrate Docusaurus pages in this repo into Mintlify form.

Implements the rules in .claude/skills/migrate-docusaurus-to-mintlify/SKILL.md.
Reads slug-map.csv (from _migration/generate-slug-map.py) for slug -> URL lookups
and source-hash tracking.

Usage:
    python _migration/migrate.py <path>                 # one file or directory
    python _migration/migrate.py --all                  # every page in this repo
    python _migration/migrate.py <path> --dry-run       # show what would change
    python _migration/migrate.py --all --force          # re-migrate even if up-to-date

Incremental behavior:
    A page is skipped when slug-map.csv shows migrated=true AND its stored
    migrated_hash equals the current source_hash (the Docusaurus source has not
    changed since the last migration). Use --force to override.

    On success, the row's migrated_hash and migrated_at columns are updated.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import shutil
import sys
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from posixpath import normpath as posix_normpath

THIS_REPO = Path(__file__).resolve().parent.parent
DEFAULT_DOCUSAURUS = Path.home() / "Desktop" / "clickhouse-docs"
SKIP_DIRS = {"node_modules", ".git", "i18n", ".claude", ".mintlify", "scripts", "snippets", "static"}
# Translation dirs are managed by the localisation bot — never migrate them.
TRANSLATION_DIRS = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}
# Same as SKIP_DIRS but allows the migrator's `--all` to descend into snippets/
# so partials get the same transforms as pages.
ITER_SKIP_DIRS = (SKIP_DIRS | TRANSLATION_DIRS) - {"snippets"}
EXTS = (".md", ".mdx")
RUNNABLE_IMPORT = 'import { RunnableCode } from "/snippets/components/RunnableCode/RunnableCode.jsx";'
IMAGE_IMPORT = 'import { Image } from "/snippets/components/Image.jsx";'
AGENT_PROMPT_IMPORT = (
    'import { AgentPrompt } from "/snippets/components/AgentPrompt/AgentPrompt.jsx";'
)
ADMON_TAG = {"note": "Note", "tip": "Tip", "info": "Info", "warning": "Warning",
             "caution": "Warning", "danger": "Danger", "important": "Warning"}


# ----- lookups ---------------------------------------------------------------

@dataclass
class Lookups:
    slug_to_url: dict[str, str] = field(default_factory=dict)
    docu_file_to_slug: dict[str, str] = field(default_factory=dict)
    snippet_basename_to_path: dict[str, str] = field(default_factory=dict)  # legacy: first path
    snippet_basename_to_paths: dict[str, list[str]] = field(default_factory=dict)  # all paths
    by_mintlify_file: dict[str, dict] = field(default_factory=dict)
    asset_basenames: set = field(default_factory=set)  # files in mintlify/assets/


def file_to_url(rel_path: str) -> str:
    s = rel_path.rsplit(".", 1)[0].replace("\\", "/")
    if s == "index":
        return "/"
    return "/" + s


def normalize_slug(s: str) -> str:
    return "/" + s.lstrip("/")


def build_lookups(slug_map_csv: Path) -> tuple[Lookups, list[dict]]:
    lk = Lookups()
    rows = list(csv.DictReader(slug_map_csv.open(encoding="utf-8")))
    for r in rows:
        slug = r["docusaurus_slug"]
        mfile_field = r["mintlify_file"]
        # Also register every Mintlify path mentioned by the row (for
        # ambiguous slugs the field is "path1 | path2 | ..."). This lets
        # POST_TRANSFORM_OVERRIDES + the source-of-truth read find the row
        # even when the slug itself is ambiguous.
        all_paths = [p.strip() for p in mfile_field.split(" | ") if p.strip()]
        if r["status"] == "matched" and all_paths:
            mfile = all_paths[0]
            url = file_to_url(mfile)
            lk.slug_to_url[slug] = url
            lk.slug_to_url.setdefault(slug.rstrip("/"), url)
            lk.slug_to_url.setdefault(slug + "/", url)
        elif r["status"] == "deleted":
            # Deleted pages have no Mintlify file; rewrite links to new_url
            # (which may be an internal Mintlify path or an external URL).
            dest = r.get("new_url", "").strip()
            if dest:
                lk.slug_to_url[slug] = dest
                lk.slug_to_url.setdefault(slug.rstrip("/"), dest)
                lk.slug_to_url.setdefault(slug + "/", dest)
        for p in all_paths:
            lk.by_mintlify_file.setdefault(p, r)
        if r["docusaurus_file"]:
            lk.docu_file_to_slug[r["docusaurus_file"]] = slug

    # Make already-Mintlify-form URLs idempotent: a link that's already a current
    # mintlify URL should resolve to itself on re-runs. We register URLs for both
    # (a) files in slug-map.csv and (b) every file actually present on disk —
    # the latter covers pages with no `slug:` field (e.g. landing pages).
    for r in rows:
        mfile = r["mintlify_file"].split(" | ")[0]
        if mfile:
            url = file_to_url(mfile)
            lk.slug_to_url.setdefault(url, url)
    for p in THIS_REPO.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        rel = p.relative_to(THIS_REPO)
        if any(part in SKIP_DIRS for part in rel.parts):
            continue
        if any(part in TRANSLATION_DIRS for part in rel.parts):
            continue
        url = file_to_url(str(rel).replace("\\", "/"))
        lk.slug_to_url.setdefault(url, url)
        # For index pages, also register the stripped form (/foo/bar/index → /foo/bar)
        # so links written either way resolve to the canonical /index form.
        if url.endswith("/index"):
            stripped = url[: -len("/index")]
            lk.slug_to_url.setdefault(stripped, url)
            lk.slug_to_url.setdefault(stripped + "/", url)
        # Also register the file's slug: frontmatter so Docusaurus links using
        # the canonical slug resolve correctly for hand-authored pages not in
        # the slug-map CSV.
        try:
            head = p.read_text(encoding="utf-8", errors="replace")[:2000]
            fm = FRONTMATTER_RE.match(head)
            if fm:
                for line in fm.group(1).split("\n"):
                    k, _, v = line.partition(":")
                    if k.strip() == "slug":
                        slug_val = normalize_slug(v.strip().strip("'\""))
                        lk.slug_to_url.setdefault(slug_val, url)
                        lk.slug_to_url.setdefault(slug_val.rstrip("/"), url)
                        break
        except OSError:
            pass

    # Trust every alias target the user has reviewed in slug-aliases.csv as a
    # valid URL — covers Mintlify-generated routes (e.g. OpenAPI pages) that
    # have no .mdx file on disk for the walker to find. Also register the
    # alias's `old_slug` so the in-process link resolver can apply aliases
    # (the apply-slug-aliases.py script handles markers in already-written
    # files; this enables resolution during migration too).
    aliases_csv = THIS_REPO / "_migration" / "slug-aliases.csv"
    if aliases_csv.exists():
        HIGH = {"basename-unique", "basename+parent-unique", "redirect-direct",
                "redirect-mapped", "manual"}
        for r in csv.DictReader(aliases_csv.open(encoding="utf-8")):
            target = (r.get("suggested_target") or "").strip()
            old = (r.get("old_slug") or "").strip()
            conf = (r.get("confidence") or "").strip()
            if not target.startswith("/"):
                continue
            # `manual` aliases are user-curated, so trust them as truth even
            # when the target isn't a known file (covers OpenAPI-generated
            # routes like `/api-reference/...`). For lower-confidence
            # auto-aliases (e.g. derived from vercel-redirect rules that may
            # still use stale Docusaurus paths) only register when the target
            # is already known — otherwise we silently introduce self-redirects
            # to nonexistent URLs.
            target_known = target in lk.slug_to_url
            if conf == "manual":
                lk.slug_to_url.setdefault(target, target)
                if old.startswith("/"):
                    lk.slug_to_url.setdefault(old, target)
            elif target_known and old.startswith("/") and conf in HIGH:
                lk.slug_to_url.setdefault(old, target)

    snippets_root = THIS_REPO / "snippets"
    if snippets_root.exists():
        for p in snippets_root.rglob("*.mdx"):
            url = "/" + str(p.relative_to(THIS_REPO)).replace("\\", "/")
            lk.snippet_basename_to_path.setdefault(p.name, url)
            lk.snippet_basename_to_paths.setdefault(p.name, []).append(url)

    assets_root = THIS_REPO / "assets"
    if assets_root.exists():
        for p in assets_root.rglob("*"):
            if p.is_file():
                lk.asset_basenames.add(p.name)
    return lk, rows


# ----- frontmatter -----------------------------------------------------------

FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n?", re.DOTALL)
DROP_KEYS = {"sidebar_position", "sidebar_class_name", "hide_table_of_contents"}
DROP_PREFIXES = ("pagination_",)


def transform_frontmatter(text: str) -> tuple[str, str | None, str | None]:
    m = FRONTMATTER_RE.match(text)
    if not m:
        return text, None, None
    body_start = m.end()
    out_lines: list[str] = []
    slug = title = None
    for line in m.group(1).split("\n"):
        if not line.strip():
            out_lines.append(line)
            continue
        key, _, val = line.partition(":")
        key = key.strip()
        if key in DROP_KEYS or any(key.startswith(p) for p in DROP_PREFIXES):
            continue
        if key == "sidebar_label":
            line = "sidebarTitle:" + val
        if key == "slug":
            slug = val.strip().strip("'\"")
        if key == "title":
            title = val.strip().strip("'\"")
        out_lines.append(line)
    new_fm = "---\n" + "\n".join(out_lines).strip("\n") + "\n---\n"
    return new_fm + text[body_start:], slug, title


# ----- redundant H1 ----------------------------------------------------------

def drop_redundant_h1(text: str, title: str | None) -> str:
    # Mintlify renders the frontmatter `title:` as the page's H1, so a leading
    # H1 in the body is always a duplicate. Remove the first H1 that opens the
    # body — allowing it to be preceded only by blank lines, import statements,
    # or comments. The H1 text does NOT have to match `title` exactly: anchors
    # (`# Title {#foo}`), casing, and trailing punctuation frequently differ,
    # and any of those still render as a duplicate heading. Only a top-of-body
    # H1 is removed, so genuine mid-page `#` headings are left untouched.
    if not title:
        return text
    m = FRONTMATTER_RE.match(text)
    body_start = m.end() if m else 0
    head, body = text[:body_start], text[body_start:]

    lines = body.split("\n")

    def skippable(ln: str) -> bool:
        # Lines that may legitimately precede the title H1: blanks, imports,
        # comments, and standalone JSX/HTML elements (e.g. a `<...Badge />`).
        # Only self-closing or single-line elements are skipped — a bare
        # opening tag like `<Note>` is left in place so we never reach into a
        # container's body and delete a heading that lives inside it.
        s = ln.strip()
        return (
            s == ""
            or s.startswith("import ")
            or s.startswith("{/*")
            or s.startswith("<!--")
            or (s.startswith("<") and (s.endswith("/>") or "</" in s))
        )

    i = 0
    while i < len(lines) and skippable(lines[i]):
        i += 1
    if i < len(lines) and re.match(r"^# +\S", lines[i]):
        del lines[i]
        while i < len(lines) and lines[i].strip() == "":
            del lines[i]
        return head + "\n".join(lines)

    # Fallback: an H1 that exactly matches the title but sits after content the
    # skip logic doesn't recognise. Preserve the original behaviour of removing
    # the first such H1 anywhere in the body so we're never worse than before.
    pattern = re.compile(r"^# +" + re.escape(title) + r"\s*\n+", re.MULTILINE)
    return head + pattern.sub("", body, count=1)


# ----- imports ---------------------------------------------------------------

IMPORT_LINE_RE = re.compile(
    r"^import[ \t]+(?P<spec>(?:\{[^}]*\}|\w+|\*[ \t]+as[ \t]+\w+))[ \t]+from[ \t]+['\"](?P<src>[^'\"]+)['\"][ \t]*;?"
    r"(?:[ \t]*\{/\*\s*MIGRATE:[^*]*\*/\})?"  # strip a stale MIGRATE marker comment if present
    r"[ \t]*$",
    re.MULTILINE,
)


# `useBaseUrl('x')` -> `'x'`. The Mintlify URL is the path as-is, so the hook
# is a no-op. Matches single- or double-quoted string args, including a
# trailing-slash boolean second arg (which we just drop).
USE_BASE_URL_RE = re.compile(r"useBaseUrl\(\s*(['\"][^'\"]*['\"])(?:\s*,[^)]*)?\s*\)")


def transform_use_base_url(text: str) -> str:
    return USE_BASE_URL_RE.sub(r"\1", text)


# Docusaurus headings escape the anchor braces (`# Title \{#anchor\}`) because
# its MDX parser would otherwise treat `{` as a JSX expression. Mintlify's MDX
# accepts `{#anchor}` directly, so the backslashes leak through as visible
# characters. Strip them on heading lines.
HEADING_ANCHOR_RE = re.compile(r"^(#{1,6} .*?)\s*\\\{(#[^\\}]+)\\\}\s*$", re.MULTILINE)


def transform_heading_anchors(text: str) -> str:
    return HEADING_ANCHOR_RE.sub(r"\1 {\2}", text)


# `<iframe ...>...</iframe>` -> `<Frame><iframe ...>...</iframe></Frame>`
# also strips `width="..."` and `height="..."` attributes so the iframe scales
# naturally inside the Frame. Skip iframes that are already inside a <Frame>.
IFRAME_RE = re.compile(r"(?<!<Frame>\n)(?<!<Frame>)\s*(<iframe\b.*?</iframe>)", re.DOTALL)
IFRAME_DIM_RE = re.compile(r"\s+(?:width|height)=\"[^\"]*\"")


def transform_iframes(text: str) -> str:
    def repl(m: re.Match) -> str:
        tag = IFRAME_DIM_RE.sub("", m.group(1))
        return f"\n<Frame>\n{tag}\n</Frame>\n"
    # Only wrap iframes that aren't already inside a Frame.
    out = []
    i = 0
    for m in re.finditer(r"<iframe\b.*?</iframe>", text, re.DOTALL):
        # Look back ~30 chars for an opening <Frame> tag without an intervening </Frame>.
        before = text[max(0, m.start() - 60):m.start()]
        if "<Frame" in before and "</Frame>" not in before:
            continue
        out.append(text[i:m.start()])
        cleaned = IFRAME_DIM_RE.sub("", m.group(0))
        out.append(f"<Frame>\n{cleaned}\n</Frame>")
        i = m.end()
    out.append(text[i:])
    return "".join(out)


# `<TOCInline ... />` and `<TOCInline ...></TOCInline>` -> ""
# Mintlify auto-generates per-page TOC; the explicit Docusaurus component is
# unnecessary and won't render. Drop the whole element.
TOCINLINE_RE = re.compile(r"<TOCInline\b[^>]*?(?:/>|>\s*</TOCInline>)\s*", re.DOTALL)


def transform_tocinline(text: str) -> str:
    return TOCINLINE_RE.sub("", text)


# `<!-- ... -->` is valid in Markdown but illegal in MDX (it's parsed as JSX
# and `!` cannot start a JSX identifier). Convert to MDX-style `{/* ... */}`.
# Skip occurrences inside fenced code blocks so `<!-- comment -->` shown as a
# code example stays verbatim. Also leave MIGRATE markers alone: they are
# tooling-detected by their `<!--` form and should remain visible until fixed.
HTML_COMMENT_RE = re.compile(r"<!--(.*?)-->", re.DOTALL)
FENCE_LINE_RE = re.compile(r"^[ \t]{0,3}(```+|~~~+)", re.MULTILINE)


def _fenced_spans(text: str) -> list[tuple[int, int]]:
    """Return [start, end) byte ranges of every fenced code block.

    Walk the text line by line, toggling state when we hit a fence opener/closer.
    Closer must use the same fence-char and at least the opener's length.
    """
    spans: list[tuple[int, int]] = []
    in_fence = False
    fence_char = ""
    fence_len = 0
    block_start = 0
    pos = 0
    for line in text.splitlines(keepends=True):
        m = re.match(r"^[ \t]{0,3}(`{3,}|~{3,})", line)
        # CommonMark: a fence opener's info string must not contain backticks.
        # `\`\`\`text\`\`\`` on a single line is inline code, not a fence.
        if m and m.group(1)[0] == "`" and "`" in line[m.end():]:
            m = None
        if not in_fence:
            if m:
                in_fence = True
                fence_char = m.group(1)[0]
                fence_len = len(m.group(1))
                block_start = pos
        else:
            if m and m.group(1)[0] == fence_char and len(m.group(1)) >= fence_len:
                spans.append((block_start, pos + len(line)))
                in_fence = False
        pos += len(line)
    if in_fence:
        spans.append((block_start, len(text)))
    return spans


def transform_html_comments(text: str) -> str:
    spans = _fenced_spans(text)

    def in_fence(p: int) -> bool:
        for s, e in spans:
            if s <= p < e:
                return True
        return False

    def repl(m: re.Match) -> str:
        if in_fence(m.start()):
            return m.group(0)
        body = m.group(1)
        if "MIGRATE:" in body:
            return m.group(0)
        body = body.replace("*/", "*\\/")
        return "{/*" + body + "*/}"

    return HTML_COMMENT_RE.sub(repl, text)


# MDX requires a blank line between an `import ...` line and the first JSX
# element. Without it, the JSX is parsed as the import's expression and fails
# with "Unexpected ExpressionStatement".
IMPORT_THEN_JSX_RE = re.compile(
    r"(^import\s.*\n)(<[A-Za-z])",
    re.MULTILINE,
)


def ensure_blank_after_imports(text: str) -> str:
    return IMPORT_THEN_JSX_RE.sub(r"\1\n\2", text)


# ----- per-file overrides ----------------------------------------------------
#
# When a Docusaurus page uses components or patterns that don't exist as JSX
# upstream but want a Mintlify-native rewrite (e.g. <CardSecondary> wrapped in
# <Link>), register a hand-written override here keyed by the source path under
# clickhouse-docs. The function receives the post-transform text and returns
# the final text. Run AFTER all standard transforms so we can replace patterns
# that the resolver couldn't fix (e.g. references to `@docusaurus/Link`).

def _override_newjson(text: str) -> str:
    """Replace the <Link><CardSecondary .../></Link> wrapper with a Mintlify <Card>."""
    # Drop the leftover Docusaurus Link/CardSecondary imports — the new <Card>
    # is a built-in Mintlify component and needs no import.
    text = re.sub(r"^\{/\* MIGRATE: unmapped import '@docusaurus/Link' \*/\}\s*\n",
                  "", text, flags=re.MULTILINE)
    text = re.sub(r"^import\s*\{\s*CardSecondary\s*\}\s+from\s+'@clickhouse/click-ui/bundled';\s*\n",
                  "", text, flags=re.MULTILINE)
    block_re = re.compile(
        r"<Link\b[^>]*>\s*<CardSecondary\b.*?/>\s*</Link>\s*<br\s*/>\s*",
        re.DOTALL,
    )
    replacement = (
        '<Card title="Looking for a guide?" '
        'href="/concepts/best-practices/json-type" icon="book">\n'
        '  Check out our JSON best practice guide for examples, advanced features '
        'and considerations for using the JSON type.\n'
        '</Card>\n\n'
    )
    return block_re.sub(replacement, text)


def _override_librechat(text: str) -> str:
    """Replace <Link><CardHorizontal/></Link> -> Mintlify <Card> for the
    "Get started with ClickHouse Cloud" CTA."""
    text = re.sub(r"^\{/\* MIGRATE: unmapped import '@docusaurus/Link' \*/\}\s*\n",
                  "", text, flags=re.MULTILINE)
    text = re.sub(r"^import\s*\{\s*CardHorizontal\s*\}\s+from\s+'@clickhouse/click-ui/bundled';?\s*\n",
                  "", text, flags=re.MULTILINE)
    block_re = re.compile(
        r"<Link\b[^>]*>\s*<CardHorizontal\b.*?/>\s*</Link>",
        re.DOTALL,
    )
    replacement = (
        '<Card title="Get started with ClickHouse Cloud" '
        'href="https://cloud.clickhouse.com/" icon="cloud">\n'
        "  If you don't have a Cloud account yet, get started with ClickHouse Cloud "
        "today and receive $300 in credits. At the end of your 30-day free trial, "
        "continue with a pay-as-you-go plan, or contact us to learn more about our "
        "volume-based discounts. Visit our pricing page for details.\n"
        "</Card>"
    )
    return block_re.sub(replacement, text)


def _override_java_client_versions(text: str) -> str:
    """Replace Docusaurus <ClientVersionDropdown><Version>...</Version></...>
    with Mintlify <View title="..."> blocks, one per version.

    <View> is a built-in Mintlify component — no import required. It renders a
    version-switcher dropdown using the `title` prop as the label.
    The `versions={[ {version: 'v0.8+'}, {version: 'v0.7.x'} ]}` array on the
    opening tag drives the `title=` attribute on each <View>, in order.
    """
    # Drop the now-stale MIGRATE comments left for the deleted imports.
    text = re.sub(
        r"^\{/\* MIGRATE: unmapped (?:@theme )?import '@theme/ClientVersionDropdown/[^']+' \*/\}\s*\n",
        "", text, flags=re.MULTILINE,
    )

    open_re = re.compile(r"<ClientVersionDropdown\s+versions=\{\[(?P<arr>.*?)\]\}\s*>", re.DOTALL)
    close_re = re.compile(r"</ClientVersionDropdown>")
    version_open_re = re.compile(r"<Version>")
    version_close_re = re.compile(r"</Version>")
    version_str_re = re.compile(r"['\"]version['\"]\s*:\s*['\"]([^'\"]+)['\"]")

    m = open_re.search(text)
    if not m:
        return text
    versions = version_str_re.findall(m.group("arr"))
    if not versions:
        return text

    # Drop the wrapper tags.
    text = open_re.sub("", text, count=1)
    text = close_re.sub("", text, count=1)

    # Replace each <Version>/</Version> in order with <View title="vX">/</View>.
    idx = [0]
    def open_repl(_):
        v = versions[idx[0]] if idx[0] < len(versions) else f"v{idx[0]}"
        idx[0] += 1
        return f'<View title="{v}">'
    text = version_open_re.sub(open_repl, text)
    text = version_close_re.sub("</View>", text)
    return text


def _override_gcs_s3_link(text: str) -> str:
    """Rewrite the broken `../s3/index.md` link in gcs/index.md to the
    absolute slug. The link is broken upstream (s3 lives under data-ingestion,
    not as a sibling of data-ingestion/gcs) and unlikely to change there."""
    return re.sub(
        r"\.\./s3/index\.md(#[^)\s]*)?\s*<!--\s*MIGRATE:[^>]*-->",
        r"/integrations/s3\1",
        text,
    )


def _override_strip_markers(text: str) -> str:
    """Strip MIGRATE markers verbatim. Used for files whose broken-looking
    links are intentional (e.g. contributor templates with placeholder paths
    like `../data-types/float.md` shown as examples for authors)."""
    return re.sub(r"\s*<!--\s*MIGRATE:[^>]*-->\s*", "", text)


def _override_tutorial_indent_accordion(text: str) -> str:
    """Indent the unindented `</Accordion>` closes so MDX recognizes them as
    children of the surrounding list item. Upstream uses 4-space-indented
    `<Accordion>` opens but a column-0 close, which breaks MDX's list parser."""
    return re.sub(r"^</Accordion>$", "    </Accordion>", text, flags=re.MULTILINE)


def _override_sparse_primary_strip_inline_anchor(text: str) -> str:
    """Drop the inline `<a name="...">` HTML anchor inside an Accordion title.
    JSX attribute values can't contain literal backslashes (the upstream uses
    `title="...<a name=\\"x\\"></a>"`), so the simplest fix is to strip the
    anchor — Mintlify generates per-heading anchors anyway."""
    return re.sub(r'<a name=\\"[^"]*\\"></a>', "", text)


def _override_rds_maria_unclosed_fence(text: str) -> str:
    """Upstream `rds_maria.md` has an unclosed ```sql fence around the
    `GRANT REPLICATION SLAVE` block, which makes Mintlify swallow the rest of
    the page as code and trip MDX. Close the fence."""
    return re.sub(
        r"(GRANT REPLICATION SLAVE ON \*\.\* TO 'clickpipes_user'@'%';)\n\n(## Configure network access)",
        r"\1\n    ```\n\n\2",
        text,
    )


def _override_formats_arrays_jsonl(text: str) -> str:
    """Keep the `arrays.json` download link pointing at the renamed
    `arrays.jsonl` file. We renamed the asset to `.jsonl` so Mintlify's
    OpenAPI scanner skips it (the file is JSON-lines, not valid JSON)."""
    return re.sub(
        r"\]\(\.\./assets/arrays\.json[^)]*\)",
        "](/assets/arrays.jsonl)",
        text,
    )


def _override_install_selector(text: str) -> str:
    """Replace the unmapped `@site/src/components/Install/Install` import with
    the local InstallSelector shim at /snippets/components/Install/Install.jsx.
    Also rewrites Docusaurus <Link to="..."> → <a href="..."> and drops the
    @docusaurus/Link unmapped-import marker."""
    text = re.sub(
        r"\{/\*\s*MIGRATE:\s*unmapped import '@site/src/components/Install/Install'\s*\*/\}",
        'import {InstallSelector} from "/snippets/components/Install/Install.jsx";',
        text,
    )
    text = re.sub(r"^\{/\* MIGRATE: unmapped import '@docusaurus/Link' \*/\}\s*\n",
                  "", text, flags=re.MULTILINE)
    text = re.sub(r"<Link\b([^>]*)\bto=", r"<a\1href=", text)
    text = re.sub(r"</Link>", "</a>", text)
    return text


def _override_cloud_changelog_rss(text: str) -> str:
    """Drop the manual RSS-feed admonition. Mintlify ships native RSS support,
    and the link `/docs/cloud/changelog-rss.xml` doesn't resolve under our URL
    conventions. The whole admonition is redundant."""
    # The admonition has been transformed by transform_admonitions to `<Tip>...</Tip>`.
    return re.sub(
        r"\n*<Tip>\s*\n\*\*Automatically keep up to date!\*\*\s*\n.*?</Tip>\s*\n*",
        "\n\n",
        text,
        flags=re.DOTALL,
    )


def _override_cloud_changelog_wrap_updates(text: str) -> str:
    """Wrap each date section with Mintlify <Update> components.

    Upstream Docusaurus format (after standard transforms):
        ## May 8, 2026 {#2026-05-08}

        ### Feature name {#feature-slug}
        Content...

    Mintlify target format:
        <Update label="May 8, 2026">

        ### Feature name
        Content...

        </Update>

    Also strips {#anchor} IDs from all headings since they become noise in
    the changelog view.
    """
    # Strip {#anchor} IDs from all headings — they become the Update label for
    # ## headings and are decorative noise inside Update blocks for ### headings.
    text = re.sub(r"^(#{1,6} .*?)\s+\{#[^}]+\}\s*$", r"\1", text, flags=re.MULTILINE)

    # Split on ## date headings — "## Month Day, Year" or "## Month Year"
    date_heading_re = re.compile(
        r"^## ([A-Z][a-z]+ \d{1,2},? \d{4})\s*$",
        re.MULTILINE,
    )
    parts = date_heading_re.split(text)
    if len(parts) < 3:
        return text  # no date headings found — leave unchanged

    result = [parts[0]]
    for i in range(1, len(parts), 2):
        label = parts[i].strip()
        content = parts[i + 1] if i + 1 < len(parts) else ""
        result.append(f'\n<Update label="{label}">\n\n{content.strip(chr(10))}\n\n</Update>\n')

    return "".join(result)


def _override_cloud_changelog_2026(text: str) -> str:
    """Chain the RSS-admonition removal and the <Update> wrapper for 2026."""
    return _override_cloud_changelog_wrap_updates(_override_cloud_changelog_rss(text))


def _override_reference_home(text: str) -> str:
    """Replace the upstream SQL Reference index with a curated cards landing page.

    The upstream content is a plain SQL Reference intro that doesn't reflect the
    broader Mintlify reference section structure. This override replaces it with
    a CardGroup landing so re-migration stays idempotent — force-migrating will
    always produce the same curated page regardless of upstream edits.
    """
    # Preserve the canonical slug from the upstream frontmatter.
    slug_match = re.search(r"^slug:\s*(.+)$", text, re.MULTILINE)
    slug = slug_match.group(1).strip() if slug_match else "/sql-reference"

    return f"""---
slug: {slug}
title: 'Reference'
sidebarTitle: 'Home'
description: 'Reference documentation for ClickHouse — SQL statements, data types, engines, functions, formats, settings, and system tables.'
keywords: ['clickhouse', 'reference', 'sql', 'data types', 'engines', 'functions', 'formats', 'settings']
doc_type: 'landing-page'
---

<CardGroup cols={{2}}>
  <Card title="SQL Reference" icon="code" href="/reference/syntax">
    SQL statements, clauses, operators, and syntax reference.
  </Card>
  <Card title="Data Types" icon="database" href="/reference/data-types">
    All supported data types including numeric, string, date/time, arrays, maps, and more.
  </Card>
  <Card title="Engines" icon="gear" href="/reference/engines">
    Table and database engine reference — MergeTree family, Log, Integration, and Special engines.
  </Card>
  <Card title="Functions" icon="function" href="/reference/functions">
    Regular, aggregate, table, and window functions.
  </Card>
  <Card title="Formats" icon="file-code" href="/reference/formats">
    Input and output format reference for all supported data formats.
  </Card>
  <Card title="Settings" icon="sliders" href="/reference/settings">
    Server, session, and MergeTree settings reference.
  </Card>
  <Card title="System Tables" icon="table" href="/reference/system-tables">
    System tables for monitoring, diagnostics, and introspection.
  </Card>
  <Card title="Data Lakes" icon="water" href="/reference/datalakes">
    Data lake integration reference — Iceberg, Delta Lake, and Hudi.
  </Card>
</CardGroup>
"""


def _override_third_party_libraries_sidebar(text: str) -> str:
    """Force the sidebar label to "Third-party libraries". Upstream's
    `sidebar_label: 'Integrations'` is too generic for this page's spot under
    the Connectors > Tools group, so we relabel it. Rewrites the migrated
    `sidebarTitle:` line; idempotent on re-migration."""
    return re.sub(
        r"^sidebarTitle:.*$",
        "sidebarTitle: 'Third-party libraries'",
        text,
        count=1,
        flags=re.MULTILINE,
    )


def _override_kapalink_ask_ai(text: str) -> str:
    """Upstream embeds Docusaurus's globally-registered <KapaLink> component to
    open the Kapa "Ask AI" widget. Mintlify has no such component, so the bare
    tag is an undefined component that fails the page build. Rewrite it to a
    button that opens Kapa's Ask AI via the global window.Kapa API, matching the
    homepage's Ask AI entry point. Idempotent: the source always carries the
    <KapaLink> tag, so each migration regenerates the same button."""
    def repl(m: "re.Match") -> str:
        label = (m.group(1) or "").strip() or "Ask AI"
        return (
            '<button type="button" onClick={() => { '
            "if (typeof window !== 'undefined' && window.Kapa && "
            "typeof window.Kapa.open === 'function') window.Kapa.open({ mode: 'ai' }); }} "
            "style={{ background: 'none', border: 'none', padding: 0, "
            "color: 'var(--primary)', cursor: 'pointer', font: 'inherit', "
            "textDecoration: 'underline' }}>" + label + "</button>"
        )
    return re.sub(r"<KapaLink>(.*?)</KapaLink>", repl, text, flags=re.DOTALL)


def _override_clickstack_collector_agent_prompt(text: str) -> str:
    """Point the ClickStack collector prompt at the official agent skill.

    The upstream Docusaurus page still uses the docs-hosted ``SKILL.md`` URL.
    The canonical skill now lives in ``ClickHouse/agent-skills``. Installation
    is a prerequisite; the copied prompt must then invoke the collector skill.
    """
    text = re.sub(
        r'prompt="(?:[^"]*clickstack-otel-collector/SKILL\.md|'
        r'npx skills add clickhouse/agent-skills)"',
        (
            'prompt="Use the clickstack-otel-collector skill to wire an '
            'OpenTelemetry collector into my Managed ClickStack service."'
        ),
        text,
    )
    if "npx skills add clickhouse/agent-skills" not in text:
        text = text.replace(
            "<AgentPrompt",
            "Install the official ClickHouse agent skills before using the setup assistant:\n\n"
            "```bash\n"
            "npx skills add clickhouse/agent-skills\n"
            "```\n\n"
            "<AgentPrompt",
            1,
        )
    if "repositoryUrl=" not in text:
        text = re.sub(
            r'(^\s*description="[^"]*"\s*$)',
            r'\1\n  repositoryUrl="https://github.com/ClickHouse/agent-skills"',
            text,
            count=1,
            flags=re.MULTILINE,
        )
    return text


def _override_clickpipe_get_started(text: str, snippet_path: str) -> str:
    """Use provider-specific wrappers because Mint snippets do not reliably
    pass variables through a parent that renders nested snippets."""
    text = text.replace("/snippets/_create_clickpipe.mdx", snippet_path)
    return re.sub(
        r'<CreateClickPipe\s+(?:storageProvider|provider)=(?:"[^"]*"|\'[^\']*\')\s*/>',
        "<CreateClickPipe />",
        text,
    )


def _override_s3_clickpipe_get_started(text: str) -> str:
    return _override_clickpipe_get_started(
        text,
        "/snippets/clickpipes/object-storage/_create_s3_clickpipe.mdx",
    )


def _override_gcs_clickpipe_get_started(text: str) -> str:
    return _override_clickpipe_get_started(
        text,
        "/snippets/clickpipes/object-storage/_create_gcs_clickpipe.mdx",
    )


POST_TRANSFORM_OVERRIDES: dict[str, callable] = {
    "docs/sql-reference/data-types/newjson.md": _override_newjson,
    "docs/use-cases/AI_ML/MCP/03_librechat.md": _override_librechat,
    "docs/integrations/language-clients/java/client/client.mdx": _override_java_client_versions,
    "docs/integrations/language-clients/java/jdbc/jdbc.mdx": _override_java_client_versions,
    "docs/integrations/data-ingestion/gcs/index.md": _override_gcs_s3_link,
    "docs/cloud/reference/01_changelog/01_cloud_changelog/2026.md": _override_cloud_changelog_2026,
    "docs/sql-reference/index.md": _override_reference_home,
    "docs/getting-started/install/install.mdx": _override_install_selector,
    "docs/integrations/data-ingestion/clickpipes/mysql/source/rds_maria.md": _override_rds_maria_unclosed_fence,
    "docs/integrations/data-ingestion/data-formats/json/formats.md": _override_formats_arrays_jsonl,
    "docs/tutorial.md": _override_tutorial_indent_accordion,
    "docs/guides/best-practices/sparse-primary-indexes.md": _override_sparse_primary_strip_inline_anchor,
    # Contributor template — its placeholder example links (../data-types/float.md
    # etc.) are intentionally illustrative and not meant to resolve.
    "docs/development/developer-instruction.md": _override_strip_markers,
    "docs/interfaces/third-party/integrations.md": _override_third_party_libraries_sidebar,
    # <KapaLink> is a Docusaurus-only component; rewrite to a Mintlify Ask AI button.
    "docs/troubleshooting/index.md": _override_kapalink_ask_ai,
    "docs/use-cases/observability/clickstack/managed-onboarding/setting-up-your-opentelemetry-collector.md": (
        _override_clickstack_collector_agent_prompt
    ),
    "docs/integrations/data-ingestion/clickpipes/object-storage/amazon-s3/02_get-started.md": (
        _override_s3_clickpipe_get_started
    ),
    "docs/integrations/data-ingestion/clickpipes/object-storage/google-cloud-storage/02_get-started.md": (
        _override_gcs_clickpipe_get_started
    ),
}


def prune_unused_mdx_imports(text: str) -> str:
    """Drop `import X from '/path/foo.mdx';` lines where X is never used in
    the body. Each snippet resolves the imports declared in its own file, so
    retaining an unused nested snippet import only adds dead code."""

    def is_used(name: str, body: str) -> bool:
        # <Name>, <Name />, <Name/>, <Name attr=...>, {Name}, or {expr Name expr}
        if re.search(r"<\s*" + re.escape(name) + r"(?:[\s/>])", body):
            return True
        return bool(re.search(r"\{[^{}]*\b" + re.escape(name) + r"\b[^{}]*\}", body))

    def keep(line: str, rest: str) -> bool:
        m = IMPORT_LINE_RE.match(line)
        if not m:
            return True
        src = m.group("src")
        if not src.endswith((".mdx", ".md")):
            return True
        spec = m.group("spec").strip()
        # Only consider plain default imports (`import X from "..."`). Named
        # imports from MDX snippets are unusual; leave them alone.
        if "{" in spec or "*" in spec or "," in spec:
            return True
        name = spec.strip()
        return is_used(name, rest)

    lines = text.split("\n")
    out: list[str] = []
    for i, line in enumerate(lines):
        rest = "\n".join(lines[:i] + lines[i + 1 :])
        if keep(line, rest):
            out.append(line)
    return "\n".join(out)


def transform_imports(text: str, lk: Lookups, issues: list[str], source_docu_path: Path | None = None, json_vars: dict[str, str] | None = None, dest_path: Path | None = None) -> tuple[str, dict[str, str]]:
    image_vars: dict[str, str] = {}
    if json_vars is None:
        json_vars = {}

    def _is_self_import(target_url: str) -> bool:
        # Snippet basename matching can resolve an import to the same file
        # being migrated (this happens when a Docusaurus page is also copied
        # into /snippets/ under the same basename). The resulting self-import
        # makes Mintlify's MDX loader recurse infinitely and hangs the dev
        # server, so detect and drop it.
        if dest_path is None:
            return False
        try:
            return ("/" + dest_path.relative_to(THIS_REPO).as_posix()) == target_url
        except ValueError:
            return False

    def _is_translated_snippet(target_url: str | None) -> bool:
        if not target_url:
            return False
        parts = Path(target_url.lstrip("/")).parts
        return (
            len(parts) > 1
            and parts[0] == "snippets"
            and parts[1] in TRANSLATION_DIRS
        )

    def _prefer_snippet_candidate(
        candidate: str,
        common_suffix_length: int,
        current: str | None,
        current_suffix_length: int,
    ) -> bool:
        """Prefer the closest path match, then the default locale on ties.

        The migrator only processes default-locale source files. Translation
        directories contain snippets with the same basename and path suffix,
        so an unordered tie can otherwise make an English snippet import a
        translated copy.
        """
        if common_suffix_length != current_suffix_length:
            return common_suffix_length > current_suffix_length
        return _is_translated_snippet(current) and not _is_translated_snippet(candidate)

    def _import_or_drop(spec: str, target_url: str, basename: str) -> str:
        if _is_self_import(target_url):
            issues.append(f"dropped self-referential snippet import in {dest_path}: {basename}")
            return ""
        return f"import {spec} from '{target_url}';"

    def replace(m: re.Match) -> str:
        spec = m.group("spec").strip()
        src = m.group("src")

        # Drop `import { ... } from 'react';` — Mintlify only allows local
        # imports, but exposes React hooks as runtime globals so the import is
        # both forbidden and unnecessary.
        if src == "react":
            return ""

        # Relative `./*.json` data imports — record the var → resolved-path
        # mapping so a body-level transform can inline the data later, then
        # drop the import (Mintlify cannot import JSON modules).
        if (src.startswith("./") or src.startswith("../")) and src.endswith(".json"):
            varname = spec.lstrip("{").rstrip("}").strip().split(" as ")[-1]
            if source_docu_path is not None:
                resolved = (source_docu_path.parent / src).resolve()
                json_vars[varname] = str(resolved)
            return ""

        # Relative `./X.md` / `../X.md` imports point to a sibling file in the
        # upstream Docusaurus tree. Map by basename to the migrated
        # `/snippets/<basename>.mdx` (sibling layout flattens during migration).
        if src.startswith("./") or src.startswith("../"):
            basename = src.rsplit("/", 1)[-1]
            mdx_basename = basename if basename.endswith(".mdx") else basename.rsplit(".", 1)[0] + ".mdx"
            paths = lk.snippet_basename_to_paths.get(mdx_basename, [])
            if paths:
                # When multiple snippets share a basename, prefer the one whose
                # path best matches the import's resolved upstream path.
                if len(paths) > 1 and source_docu_path is not None:
                    try:
                        resolved = (source_docu_path.parent / src).resolve().with_suffix(".mdx")
                        src_parts = resolved.parts
                        best, best_len = paths[0], -1
                        for path in paths:
                            cand_parts = path.lstrip("/").split("/")
                            n = 0
                            while n < len(cand_parts) and n < len(src_parts) and cand_parts[-1-n] == src_parts[-1-n]:
                                n += 1
                            if _prefer_snippet_candidate(path, n, best, best_len):
                                best, best_len = path, n
                        return _import_or_drop(spec, best, basename)
                    except (OSError, ValueError):
                        pass
                return _import_or_drop(spec, paths[0], basename)
            issues.append(f"relative snippet not found in /snippets/: {basename}")
            return f"import {spec} from '{src}'; {{/* MIGRATE: snippet {basename!s} not found */}}"

        if src.startswith("@site/static/images/"):
            varname = spec.lstrip("{").rstrip("}").strip().split(" as ")[-1]
            # Upstream Docusaurus serves /static/images/... but in this repo
            # the assets live at /images/... — the leading `static/` segment
            # is dropped by the file copy, not the URL.
            path = "/images/" + src[len("@site/static/images/"):]
            image_vars[varname] = path
            return ""

        if src.startswith("@site/docs/") and (".md" in src or ".mdx" in src):
            basename = src.rsplit("/", 1)[-1]
            mdx_basename = basename if basename.endswith(".mdx") else basename.rsplit(".", 1)[0] + ".mdx"

            # Prefer the snippet whose path-suffix best matches the import's
            # original path. Multiple snippets can share a basename (e.g.
            # _1-data-source.md exists per cloud provider) so basename alone is
            # ambiguous — match by the longest common suffix instead.
            src_suffix = src[len("@site/"):]  # e.g. docs/_snippets/.../foo/_1-data-source.md
            src_suffix_mdx = src_suffix.rsplit(".", 1)[0] + ".mdx"
            best = None
            best_len = -1
            for path in lk.snippet_basename_to_paths.get(mdx_basename, []):
                # Strip leading "/" and trailing path components; compare with src_suffix.
                cand = path.lstrip("/")
                # Walk back from full path, find longest common suffix segments.
                cand_parts = cand.split("/")
                src_parts = src_suffix_mdx.split("/")
                n = 0
                while n < len(cand_parts) and n < len(src_parts) and cand_parts[-1-n] == src_parts[-1-n]:
                    n += 1
                if _prefer_snippet_candidate(path, n, best, best_len):
                    best, best_len = path, n
            if best:
                return _import_or_drop(spec, best, basename)
            issues.append(f"snippet not found in /snippets/: {basename}")
            return f"import {spec} from '{src}'; {{/* MIGRATE: snippet {basename!s} not found */}}"

        if src == "@theme/IdealImage":
            return IMAGE_IMPORT

        if src in ("@theme/Tabs", "@theme/TabItem"):
            return ""

        # @theme/TOCInline — Mintlify auto-generates per-page TOC, so delete
        # the import; the body `<TOCInline ...>` usages are also removed below.
        if src == "@theme/TOCInline":
            return ""

        # @theme/<...>/<Name>  ->  /snippets/components/<Name>/<Name>.jsx
        # or                      ->  /snippets/components/<Name>.jsx
        # Some Docusaurus badges drop the "Badge" suffix that the Mintlify
        # shim retains (e.g. @theme/badges/CommunityMaintained <-> CommunityMaintainedBadge).
        if src.startswith("@theme/"):
            tail = src[len("@theme/"):]
            name = tail.rsplit("/", 1)[-1]
            candidates = [name]
            if "/badges/" in src and not name.endswith("Badge"):
                candidates.append(name + "Badge")
            for n in candidates:
                folder = THIS_REPO / "snippets" / "components" / n / f"{n}.jsx"
                flat = THIS_REPO / "snippets" / "components" / f"{n}.jsx"
                if folder.exists():
                    return f'import {spec} from "/snippets/components/{n}/{n}.jsx";'
                if flat.exists():
                    return f'import {spec} from "/snippets/components/{n}.jsx";'
            issues.append(f"unmapped @theme import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        # @site/src/components/<...>/<Name>  -> /snippets/components/<Name>/<Name>.jsx
        # @site/src/theme/<...>/<Name>       -> same lookup pattern (mirrors @theme/<Name>)
        if src.startswith("@site/src/components/") or src.startswith("@site/src/theme/"):
            prefix = "@site/src/components/" if src.startswith("@site/src/components/") else "@site/src/theme/"
            tail = src[len(prefix):]
            name = tail.rsplit("/", 1)[-1]
            folder = THIS_REPO / "snippets" / "components" / name / f"{name}.jsx"
            flat = THIS_REPO / "snippets" / "components" / f"{name}.jsx"
            if folder.exists():
                return f'import {spec} from "/snippets/components/{name}/{name}.jsx";'
            if flat.exists():
                return f'import {spec} from "/snippets/components/{name}.jsx";'
            issues.append(f"unmapped @site/src import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        # @site/src/lib/<...>/<Name>  ->  /snippets/lib/<Name>.jsx
        if src.startswith("@site/src/lib/"):
            tail = src[len("@site/src/lib/"):]
            name = tail.rsplit("/", 1)[-1]
            target = THIS_REPO / "snippets" / "lib" / f"{name}.jsx"
            if target.exists():
                return f'import {spec} from "/snippets/lib/{name}.jsx";'
            issues.append(f"unmapped @site/src/lib import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        # @docusaurus/useBaseUrl is a hook that prepends the site's baseUrl.
        # In Mintlify the URL is the file path as-is, so we delete the import
        # and unwrap `useBaseUrl(x)` calls in the body via transform_use_base_url.
        if src == "@docusaurus/useBaseUrl":
            return ""

        # @docusaurus/useBrokenLinks is a Docusaurus build-time hook with no
        # Mintlify analog. Body usage typically does nothing visible — delete
        # the import; usages would already be no-ops or absent in this repo.
        if src == "@docusaurus/useBrokenLinks":
            return ""

        # @clickhouse/click-ui/bundled — Mintlify only allows local imports.
        # We transform the body usages of these components to native Mintlify
        # equivalents (<Card>, <Steps>/<Step>) elsewhere in the pipeline, so
        # the import line is no longer needed.
        if src == "@clickhouse/click-ui/bundled":
            return ""

        # `/src/components/...` — Docusaurus-style absolute import path that
        # points into the upstream React component tree. Mintlify has no
        # equivalent, and these particular components (TwoColumnList,
        # ClickableSquare, HorizontalDivide, ViewAllLink, VideoContainer)
        # are handled by dedicated body transforms (or simply unused once the
        # body is rewritten). Drop the import.
        if src.startswith("/src/components/") or src.startswith("/src/theme/"):
            return ""

        if src.startswith("@docusaurus/") or src.startswith("@site/src/"):
            issues.append(f"unmapped import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        return m.group(0)

    new_text = IMPORT_LINE_RE.sub(replace, text)
    new_text = re.sub(r"\n{3,}", "\n\n", new_text)
    return new_text, image_vars


# ----- admonitions -----------------------------------------------------------

ADMON_RE = re.compile(
    r"^[ \t]*:::(?P<type>note|tip|info|warning|caution|danger|important)"
    r"(?:\[(?P<btitle>[^\]]+)\]| +(?P<stitle>[^\n]+))?[ \t]*\n"
    r"(?P<body>.*?)"
    r"^[ \t]*:::+[ \t]*$",  # tolerate upstream typos like `::::`; do not match \n in trailing whitespace — it eats a blank line
    re.MULTILINE | re.DOTALL,
)


def transform_admonitions(text: str) -> str:
    def repl(m: re.Match) -> str:
        atype = m.group("type")
        raw_title = m.group("btitle") or m.group("stitle")
        title = raw_title.strip() if raw_title else None
        body = m.group("body").rstrip("\n")
        if title:
            tag = "Info" if atype == "note" else ADMON_TAG[atype]
            return f"<{tag}>\n**{title}**\n\n{body}\n</{tag}>"
        tag = ADMON_TAG[atype]
        return f"<{tag}>\n{body}\n</{tag}>"
    return ADMON_RE.sub(repl, text)


# ----- details / summary -----------------------------------------------------

DETAILS_RE = re.compile(
    r"<details>\s*\n?\s*<summary>(?P<title>.*?)</summary>(?P<body>.*?)</details>",
    re.DOTALL,
)


def _strip_inline_markdown(s: str) -> str:
    # Mintlify's `<Accordion title="...">` is plain text — markdown isn't
    # rendered, so leave-in `**bold**` etc. would print as literal asterisks.
    # Strip the most common inline markers; keep link text from `[text](url)`.
    s = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", s)  # [text](url) -> text
    s = re.sub(r"\*\*([^*]+)\*\*", r"\1", s)         # **bold**
    s = re.sub(r"__([^_]+)__", r"\1", s)             # __bold__
    s = re.sub(r"(?<!\*)\*([^*\n]+)\*(?!\*)", r"\1", s)  # *italic*
    s = re.sub(r"(?<!_)_([^_\n]+)_(?!_)", r"\1", s)      # _italic_
    s = re.sub(r"`([^`]+)`", r"\1", s)               # `code`
    # Inline HTML formatting tags (Mintlify renders the title as plain text,
    # so any leftover `<strong>` / `<em>` / etc. would print as literal tags).
    s = re.sub(r"</?(?:strong|b|em|i|u|span|code|small|mark|sub|sup)\b[^>]*>", "", s, flags=re.IGNORECASE)
    return s


def transform_details(text: str) -> str:
    def repl(m: re.Match) -> str:
        title = re.sub(r"\s+", " ", m.group("title")).strip()
        title = _strip_inline_markdown(title)
        body = m.group("body").strip("\n")
        title_attr = title.replace('"', '\\"')
        return f'<Accordion title="{title_attr}">\n{body}\n</Accordion>'
    return DETAILS_RE.sub(repl, text)


# ----- tabs ------------------------------------------------------------------

def transform_tabs(text: str) -> str:
    text = re.sub(r"<Tabs\b[^>]*>", "<Tabs>", text)

    # Match <TabItem ... > where ... may contain `>` inside quoted attr values
    # (e.g. label="<proxy>"). A naive `[^>]*` truncates the tag mid-attr.
    tabitem_re = re.compile(r'<TabItem\b((?:[^>"\']|"[^"]*"|\'[^\']*\')*)>')

    def tab_repl(m: re.Match) -> str:
        attrs = m.group(1)
        label = re.search(r'\blabel\s*=\s*"([^"]*)"', attrs) or re.search(r"\blabel\s*=\s*'([^']*)'", attrs)
        title = label.group(1) if label else ""
        # `<` and `>` inside a JSX attribute value are illegal in MDX even
        # when wrapped in quotes — replace with HTML entities.
        title = title.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
        return f'<Tab title="{title}">'
    text = tabitem_re.sub(tab_repl, text)
    return text.replace("</TabItem>", "</Tab>")


# ----- <Image> img={var} -> img="path" --------------------------------------

def transform_image_components(text: str, image_vars: dict[str, str]) -> str:
    if not image_vars:
        return text
    pattern = re.compile(r"<Image\b([^>]*?)\bimg=\{(\w+)\}([^>]*)>")

    def repl(m: re.Match) -> str:
        before, var, after = m.group(1), m.group(2), m.group(3)
        path = image_vars.get(var)
        if not path:
            return m.group(0)
        return f'<Image{before}img="{path}"{after}>'
    return pattern.sub(repl, text)


# Docusaurus also permits inline webpack-style image expressions such as
# `img={require('@site/static/images/example.png')}`. There is no `require`
# function in Mintlify's browser runtime, so these expressions throw at render
# time and the screenshots disappear. Rewrite them to public `/images/...`
# paths, preferring the converted WebP asset when it exists in this repo.
INLINE_SITE_IMAGE_REQUIRE_RE = re.compile(
    r"img=\{\s*require\(\s*(['\"])@site/(?:static/)?images/([^'\"]+)\1\s*\)\s*\}"
)


def transform_inline_image_requires(text: str) -> str:
    def repl(m: re.Match) -> str:
        relative_path = Path(m.group(2))
        asset_path = THIS_REPO / "images" / relative_path
        if not asset_path.is_file() and relative_path.suffix.lower() in {
            ".jpg",
            ".jpeg",
            ".png",
        }:
            webp_path = relative_path.with_suffix(".webp")
            if (THIS_REPO / "images" / webp_path).is_file():
                relative_path = webp_path
        return f'img="/images/{relative_path.as_posix()}"'

    return INLINE_SITE_IMAGE_REQUIRE_RE.sub(repl, text)


# ----- <SvgVar ... /> -> plain <img src="..." style={{width:"3rem"}} /> -----
# Docusaurus' webpack SVG loader treats `import Foo from '...foo.svg'` as a
# React component, so authors write `<Foo class="image" alt="..." style={...} />`
# inline in tables. Mintlify's MDX renderer has no equivalent — an undefined
# `<Foo>` blanks the whole page. The import itself is already stripped by
# transform_imports (image_vars records the mapping); this transform rewrites
# the body usage to a plain `<img>` HTML element. Deliberately NOT wrapping in
# Mintlify's `<Image>` component: that adds a Frame (border/padding) which is
# wrong for tiny inline icons in table cells, and the component ignores `size`
# anyway. Width is constrained inline to match the original Docusaurus look.
SVG_ALT_RE = re.compile(r'\balt\s*=\s*"([^"]*)"')
# Plain HTML `width` attribute, not JSX `style={{...}}`. The double-brace JSX
# style attribute breaks MDX parsing when an `<img>` sits inside a markdown
# table cell — the row parser sees the inner `{` as the start of an MDX
# expression and 500s the whole page. `width` in pixels has no such hazard,
# and browsers auto-scale `height` from the intrinsic aspect ratio.
SVG_INLINE_STYLE = ' width="32"'


def transform_svg_components(text: str, image_vars: dict[str, str]) -> str:
    svg_vars = {v: p for v, p in image_vars.items() if p.lower().endswith(".svg")}
    if not svg_vars:
        return text
    name_alt = "|".join(re.escape(n) for n in svg_vars)
    pattern = re.compile(rf"<({name_alt})\b([^>]*?)/?>")

    def repl(m: re.Match) -> str:
        name, attrs = m.group(1), m.group(2)
        path = svg_vars[name]
        alt_m = SVG_ALT_RE.search(attrs)
        alt = f' alt="{alt_m.group(1)}"' if alt_m else ""
        return f'<img src="{path}"{alt}{SVG_INLINE_STYLE} />'
    return pattern.sub(repl, text)


# ----- <Image size="logo"> -> plain <img> -----------------------------------
# Mintlify's <Image> component wraps content in a Frame (border/padding) and
# ignores the `size` prop. For inline icons in table cells that's the wrong
# look, so lower every `<Image ... size="logo" .../>` to a plain `<img>` with
# an inline width. This also catches the Docusaurus pattern where a PNG is
# imported as a variable and used as `<Image img={var} size="logo" />`
# (transform_image_components has already inlined the path by this point).
IMAGE_LOGO_RE = re.compile(r'<Image\b([^>]*?)/?>')


def transform_image_logo_to_img(text: str) -> str:
    def repl(m: re.Match) -> str:
        attrs = m.group(1)
        if not re.search(r'\bsize\s*=\s*"logo"', attrs):
            return m.group(0)
        img_m = re.search(r'\bimg\s*=\s*"([^"]*)"', attrs)
        if not img_m:
            return m.group(0)
        src = img_m.group(1)
        alt_m = SVG_ALT_RE.search(attrs)
        alt = f' alt="{alt_m.group(1)}"' if alt_m else ""
        return f'<img src="{src}"{alt}{SVG_INLINE_STYLE} />'
    return IMAGE_LOGO_RE.sub(repl, text)


# ----- click-ui body transforms ---------------------------------------------

# Convert `<VerticalStepper>...</VerticalStepper>` (Click UI, splits content by
# headings of `headerLevel` — defaults to h2) into Mintlify `<Steps>`/`<Step>`
# blocks. Each matching heading inside the stepper marks the start of a new
# Step and becomes its `title`/`id` props: the `id` carries the heading's
# `{#anchor}` (or its auto-generated slug) so existing in-page links keep
# working, and a heading left inside the body would render the step number
# above the title. Headings inside fenced code blocks are ignored — e.g.
# `## comment` inside a `shell` fence is a shell comment, not a Markdown
# heading.
VERTICAL_STEPPER_RE = re.compile(r"<VerticalStepper\b(?P<attrs>[^>]*)>(?P<body>.*?)</VerticalStepper>", re.DOTALL)
VERTICAL_STEPPER_SELFCLOSE_RE = re.compile(r"<VerticalStepper\b[^>]*/>\s*\n?")
HEADER_LEVEL_RE = re.compile(r'\bheaderLevel\s*=\s*"h([1-6])"')
FENCE_RE = re.compile(r"^[ \t]*(```+|~~~+)", re.MULTILINE)
STEP_HEADING_RE = re.compile(
    r"^\s*#{1,6}\s+(?P<title>.*?)(?:\s*\{#(?P<anchor>[^}]+)\})?\s*$"
)
# Inline markup that a plain string `title` prop would render literally.
# Only non-interactive markup (code spans, bold) is serialized into the
# title; links and JSX are flattened out beforehand (see _flatten_links).
STEP_TITLE_MARKUP_RE = re.compile(r"`|\*\*")
STEP_TITLE_TOKEN_RE = re.compile(
    r"`(?P<code>[^`]+)`"
    r"|\*\*(?P<bold>[^*]+)\*\*"
)
# Interactive content in a step heading: a markdown link or a JSX element
# (e.g. `<TrackedLink ...>`). The Step title element owns the anchor click
# handler, so interactive nodes must never be serialized into `title`.
STEP_TITLE_INTERACTIVE_RE = re.compile(r"\[[^\]]*\]\(|</?[A-Za-z]")


def _step_slug(text: str) -> str:
    # GitHub-style slug of the heading's plain text, for headings without an
    # explicit `{#anchor}` — matches the anchor the renderer auto-generates.
    text = re.sub(r"\[([^\]]*)\]\([^)\s]+\)", r"\1", text)
    text = re.sub(r"</?[A-Za-z][^>]*>", "", text).replace("`", "").replace("**", "")
    out = []
    for ch in text.strip().lower():
        if ch in (" ", "-"):
            out.append("-")
        else:
            cat = unicodedata.category(ch)
            if cat[0] in ("L", "N", "M") or ch == "_":
                out.append(ch)
    return "".join(out)


def _flatten_links(title: str) -> str:
    # Linked text keeps its words, tags are dropped:
    # `[Download](url) the config` -> `Download the config`,
    # `<TrackedLink ...>Download</TrackedLink> the config` -> `Download the config`.
    title = re.sub(r"\[([^\]]*)\]\([^)\s]+\)", r"\1", title)
    return re.sub(r"</?[A-Za-z][^>]*>", "", title).strip()


def _claim_step_anchor(claimed: set[str], title: str, anchor: str | None) -> str:
    # Register the step's fragment id: the explicit `{#anchor}` verbatim, or
    # the auto slug suffixed with `-1`, `-2`, ... past ids already claimed.
    if not anchor:
        base = anchor = _step_slug(title)
        n = 0
        while anchor in claimed:
            n += 1
            anchor = f"{base}-{n}"
    claimed.add(anchor)
    return anchor


def _claimed_ids_before(text: str, end: int) -> set[str]:
    # Fragment ids claimed by headings before `end`, in document order and
    # mirroring the renderer's assignment (explicit `{#anchor}` or the auto
    # slug), so anchorless stepper headings never reuse an earlier fragment.
    claimed: set[str] = set()
    in_fence = False
    for line in text[:end].split("\n"):
        if FENCE_RE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        hm = STEP_HEADING_RE.match(line)
        if hm:
            _claim_step_anchor(claimed, hm.group("title").strip(), hm.group("anchor"))
    return claimed


def _jsx_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("{", "&#123;")
        .replace("}", "&#125;")
        .replace("<", "&lt;")
    )


def _step_title_attr(title: str) -> str:
    # A plain string when possible, a JSX fragment when the title carries
    # non-interactive inline markup (code spans, bold). Links and JSX must be
    # flattened out with _flatten_links before calling this.
    if not STEP_TITLE_MARKUP_RE.search(title):
        if '"' in title:
            if "'" in title:
                return 'title="%s"' % title.replace('"', "&quot;")
            return "title='%s'" % title
        return 'title="%s"' % title
    pieces = []
    pos = 0
    for m in STEP_TITLE_TOKEN_RE.finditer(title):
        pieces.append(_jsx_escape(title[pos:m.start()]))
        if m.group("code") is not None:
            pieces.append("<code>%s</code>" % _jsx_escape(m.group("code")))
        else:
            pieces.append("<strong>%s</strong>" % _jsx_escape(m.group("bold")))
        pos = m.end()
    pieces.append(_jsx_escape(title[pos:]))
    return "title={<>%s</>}" % "".join(pieces)


def _heading_positions_outside_fences(body: str, level: int) -> list[int]:
    heading_re = re.compile(rf"^{'#' * level}[ \t]+\S.*$", re.MULTILINE)
    skip_spans: list[tuple[int, int]] = []
    # Headings inside fenced code blocks are not Markdown headings.
    in_fence = False
    fence_start = 0
    for fm in FENCE_RE.finditer(body):
        if not in_fence:
            in_fence = True
            fence_start = fm.start()
        else:
            in_fence = False
            skip_spans.append((fence_start, fm.end()))
    if in_fence:
        skip_spans.append((fence_start, len(body)))

    # Headings inside nested grouping containers (`<Tabs>`, `<details>`,
    # `<Accordion>`) belong to the inner block, not to the stepper. A
    # `<VerticalStepper>` page typically opens a `<Tabs>` per step body, with
    # its own `<TabItem>` h3s — those must not be promoted to step boundaries
    # or the resulting `<Steps>` close mid-`<Tab>`.
    for tag in ("Tabs", "details", "Accordion"):
        for cm in re.finditer(rf"<{tag}\b[^>]*>", body):
            close = re.search(rf"</{tag}>", body[cm.end():])
            if not close:
                continue
            skip_spans.append((cm.start(), cm.end() + close.end()))

    def in_skip(pos: int) -> bool:
        return any(s <= pos < e for s, e in skip_spans)

    return [hm.start() for hm in heading_re.finditer(body) if not in_skip(hm.start())]


def transform_vertical_stepper(text: str) -> str:
    # Self-closing `<VerticalStepper .../>` has no content — drop it entirely.
    text = VERTICAL_STEPPER_SELFCLOSE_RE.sub("", text)
    def repl(m: re.Match) -> str:
        attrs = m.group("attrs")
        body = m.group("body")
        level_m = HEADER_LEVEL_RE.search(attrs)
        level = int(level_m.group(1)) if level_m else 2
        positions = _heading_positions_outside_fences(body, level)
        if not positions:
            # No matching heading inside — unwrap, preserving content as-is.
            return body.strip("\n")
        prelude = body[: positions[0]].strip("\n")
        steps = []
        claimed = _claimed_ids_before(m.string, m.start())
        for i, start in enumerate(positions):
            end = positions[i + 1] if i + 1 < len(positions) else len(body)
            seg = body[start:end].strip("\n")
            heading, _, rest = seg.partition("\n")
            hm = STEP_HEADING_RE.match(heading)
            title = hm.group("title").strip()
            anchor = _claim_step_anchor(claimed, title, hm.group("anchor"))
            rest = rest.strip(chr(10))
            if STEP_TITLE_INTERACTIVE_RE.search(title):
                # The heading carries a link or JSX. Interactive nodes must
                # not go into the title (its element owns the anchor click
                # handler), so the title is flattened to plain text and the
                # original inline content moves to the top of the step body,
                # keeping the link usable.
                rest = title + ("\n\n" + rest if rest else "")
                title = _flatten_links(title)
            opener = '<Step %s id="%s">' % (_step_title_attr(title), anchor)
            steps.append(f"{opener}\n{rest}\n</Step>")
        out = ("" if not prelude else prelude + "\n\n") + "<Steps>\n" + "\n".join(steps) + "\n</Steps>"
        return out
    return VERTICAL_STEPPER_RE.sub(repl, text)


# Convert `<CardPrimary ... />` (Click UI hero card) to a Mintlify `<Card>`.
# We keep title, description (as body), icon, and href (from infoUrl). Other
# Click UI-only props (alignContent, size, isSelected, onButtonClick, etc.)
# don't have Mintlify equivalents and are dropped.
CARD_PRIMARY_RE = re.compile(r"<CardPrimary\b(?P<attrs>[^>]*?)/>", re.DOTALL)
JSX_ATTR_RE = re.compile(r'(?P<name>\w+)\s*=\s*(?:"(?P<dq>[^"]*)"|\{(?P<expr>[^{}]*)\}|\'(?P<sq>[^\']*)\')')


def _parse_jsx_attrs(attrs: str) -> dict[str, str]:
    out = {}
    for m in JSX_ATTR_RE.finditer(attrs):
        name = m.group("name")
        if m.group("dq") is not None:
            out[name] = m.group("dq")
        elif m.group("sq") is not None:
            out[name] = m.group("sq")
        else:
            out[name] = "{" + m.group("expr") + "}"
    return out


def transform_card_primary(text: str) -> str:
    def repl(m: re.Match) -> str:
        a = _parse_jsx_attrs(m.group("attrs"))
        title = a.get("title", "")
        description = a.get("description", "").strip()
        icon = a.get("icon")
        href = a.get("infoUrl")
        cta = a.get("infoText")
        parts = [f'title="{title}"']
        if icon:
            parts.append(f'icon="{icon}"')
        if href:
            parts.append(f'href="{href}"')
        if cta:
            parts.append(f'cta="{cta}"')
        return f'<Card {" ".join(parts)}>\n  {description}\n</Card>'
    return CARD_PRIMARY_RE.sub(repl, text)


# Some upstream pages declare an inline `Anchor` component (so Docusaurus's
# useBrokenLinks() can collect span ids) and use `<Anchor id="..."/>` markers
# inline. Mintlify has no useBrokenLinks() equivalent — replace the markers
# with plain `<a id="...">` anchors and strip the component definition along
# with the explanatory comment that precedes it.
ANCHOR_USE_RE = re.compile(r"<Anchor\s+id=\"([^\"]+)\"\s*/>")
ANCHOR_DEF_RE = re.compile(
    r"(?:\{/\*\s*needed as docusaurus can't resolve links to span ids[^*]*\*/\}\s*\n)?"
    r"export function Anchor\(props\)\s*\{.*?^\}\s*\n",
    re.DOTALL | re.MULTILINE,
)


def transform_inline_anchor(text: str) -> str:
    text = ANCHOR_DEF_RE.sub("", text)
    text = ANCHOR_USE_RE.sub(r'<a id="\1"></a>', text)
    return text


# Upstream pages reference image assets as `/static/images/...` (Docusaurus
# serves the `static/` dir at the URL root). In this Mintlify repo the assets
# live directly under `/images/...`, so rewrite any inline reference.
STATIC_IMAGES_RE = re.compile(r"/static/images/")


def transform_static_image_paths(text: str) -> str:
    return STATIC_IMAGES_RE.sub("/images/", text)


# `<HorizontalDivide />` (Docusaurus component) -> markdown horizontal rule.
HORIZONTAL_DIVIDE_RE = re.compile(r"<HorizontalDivide\s*/>")


def transform_horizontal_divide(text: str) -> str:
    return HORIZONTAL_DIVIDE_RE.sub("---", text)


# `<TwoColumnList items={IDENT} />` (Docusaurus component, paired with a
# JSON data import) -> Mintlify <Columns cols={2}> with one <Card> per JSON row.
# We resolve `IDENT` against the json_vars map collected in transform_imports
# (which holds the absolute upstream path of the JSON file).
TWO_COLUMN_LIST_RE = re.compile(r"<TwoColumnList\s+items=\{(?P<ident>\w+)\}\s*/>")


def transform_two_column_list(text: str, json_vars: dict[str, str]) -> str:
    if not json_vars:
        return text

    def repl(m: re.Match) -> str:
        ident = m.group("ident")
        path = json_vars.get(ident)
        if not path or not Path(path).exists():
            return m.group(0)
        try:
            data = json.loads(Path(path).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return m.group(0)
        cards = []
        for item in data:
            title = item.get("title", "")
            desc = item.get("description", "").strip()
            url = item.get("url", "")
            # Strip Docusaurus '/docs' prefix so links resolve in Mintlify.
            if url.startswith("/docs/"):
                url = url[len("/docs"):]
            cards.append(f'  <Card title="{title}" href="{url}">\n    {desc}\n  </Card>')
        return "<Columns cols={2}>\n" + "\n".join(cards) + "\n</Columns>"
    return TWO_COLUMN_LIST_RE.sub(repl, text)


# ----- runnable code fences --------------------------------------------------

RUNNABLE_FENCE_RE = re.compile(
    r"^```(?P<lang>\w+)\s+(?P<token>runnable(?:=false)?)\s*\n(?P<body>.*?)^```\s*$",
    re.MULTILINE | re.DOTALL,
)


def transform_runnable(text: str) -> tuple[str, bool]:
    used = False

    def repl(m: re.Match) -> str:
        nonlocal used
        lang, token, body = m.group("lang"), m.group("token"), m.group("body")
        if token == "runnable=false":
            return f"```{lang}\n{body}```"
        used = True
        return f"<RunnableCode>\n```{lang}\n{body}```\n</RunnableCode>"
    return RUNNABLE_FENCE_RE.sub(repl, text), used


# Docusaurus "magic comment" highlight directives that live inside code fences:
#   # highlight-next-line / -start (or -begin) / -end       (Python, bash, …)
#   // highlight-next-line / -start / -end                  (JS, TS, C, Java, …)
#   -- highlight-next-line / -start / -end                  (SQL)
#   <!-- highlight-next-line / -start / -end -->            (HTML, XML, MDX)
# Mintlify's renderer doesn't recognise these; they leak through as raw text
# inside the highlighted code block. The transform below rewrites them to
# Mintlify's `highlight={N-M,P}` info-string syntax and strips the comment
# lines from the body. See https://docusaurus.io/docs/markdown-features/code-blocks#custom-magic-comments.
_HIGHLIGHT_COMMENT_RE = re.compile(
    r"^[ \t]*"
    r"(?:#|//|--|<!--)\s*"
    r"highlight-(next-line|start|begin|end)"
    r"\s*(?:-->)?[ \t]*$",
    re.IGNORECASE,
)
_FENCE_OPEN_RE = re.compile(
    r"^(?P<indent>[ \t]*)(?P<fence>```+|~~~+)(?P<info>[^\n]*)$",
    re.MULTILINE,
)


def _highlight_ranges(line_nums: list[int]) -> str:
    line_nums = sorted(set(line_nums))
    if not line_nums:
        return ""
    out = []
    a = prev = line_nums[0]
    for n in line_nums[1:]:
        if n == prev + 1:
            prev = n
            continue
        out.append(str(a) if a == prev else f"{a}-{prev}")
        a = prev = n
    out.append(str(a) if a == prev else f"{a}-{prev}")
    return ",".join(out)


def transform_highlight_comments(text: str) -> str:
    """Convert Docusaurus highlight-* magic comments inside fenced code blocks
    to Mintlify's `highlight={ranges}` info-string syntax."""
    out: list[str] = []
    pos = 0
    while True:
        m = _FENCE_OPEN_RE.search(text, pos)
        if not m:
            out.append(text[pos:])
            break
        out.append(text[pos:m.start()])
        indent = m.group("indent")
        fence = m.group("fence")
        info = m.group("info")
        body_start = m.end() + 1  # past the opener's newline
        close_re = re.compile(
            r"^" + re.escape(indent) + re.escape(fence) + r"[ \t]*$",
            re.MULTILINE,
        )
        c = close_re.search(text, body_start)
        if not c:
            out.append(text[m.start():])
            break
        body = text[body_start:c.start()]
        body_lines = body.split("\n")
        if body_lines and body_lines[-1] == "":
            body_lines = body_lines[:-1]

        new_lines: list[str] = []
        highlights: list[int] = []
        pending_next = False
        block_start: int | None = None
        for line in body_lines:
            mh = _HIGHLIGHT_COMMENT_RE.match(line)
            if not mh:
                if pending_next:
                    highlights.append(len(new_lines) + 1)
                    pending_next = False
                new_lines.append(line)
                continue
            tok = mh.group(1).lower()
            if tok == "next-line":
                pending_next = True
            elif tok in ("start", "begin"):
                block_start = len(new_lines) + 1
            elif tok == "end":
                if block_start is not None:
                    for ln in range(block_start, len(new_lines) + 1):
                        highlights.append(ln)
                block_start = None
            # The magic-comment line itself is consumed (never appended).

        if not highlights or "highlight=" in info:
            out.append(text[m.start():c.end()])
            pos = c.end()
            continue
        range_str = _highlight_ranges(highlights)
        new_info = info.rstrip()
        new_info = f"{new_info} highlight={{{range_str}}}" if new_info else f" highlight={{{range_str}}}"
        new_body = "\n".join(new_lines)
        out.append(f"{indent}{fence}{new_info}\n{new_body}\n{indent}{fence}")
        if c.end() < len(text) and text[c.end()] == "\n":
            out.append("\n")
            pos = c.end() + 1
        else:
            pos = c.end()
    return "".join(out)


# ----- internal link rewrites ------------------------------------------------

# Capture everything up to the closing paren as the href; rewrite_one strips
# trailing whitespace, optional `"title"` and any leftover MIGRATE markers.
# Link text may contain `[...]` segments — either inside backticks
# (e.g. `\`Tuple(n1 T1[, n2 T2, ...])\``) or bare (e.g. `operator[]`).
# Allow non-`]` chars (including a single newline for soft-wrapped text),
# balanced `[...]` pairs, or `\`...\`` segments. The href is bounded to one
# line and rejects blank lines inside text so an unclosed link can't eat
# across paragraphs.
LINK_RE = re.compile(
    r"\[(?P<text>(?:[^\[\]`\n]|\n(?!\n)|\[[^\[\]\n]*\]|`[^`\n]*`)*)\]"
    r"\((?P<href>(?:[^)\n]|\n(?!\n))+)\)"
)
TITLE_TAIL_RE = re.compile(r"\s+\"[^\"]*\"\s*$")
HREF_ATTR_RE = re.compile(r'(\bhref\s*=\s*)"([^"]+)"')


def split_frag(href: str) -> tuple[str, str]:
    if "#" in href:
        a, b = href.split("#", 1)
        return a, "#" + b
    return href, ""


def strip_md_tail(p: str) -> str:
    # Some Docusaurus links end in `.md/` (trailing slash before the fragment).
    p = p.rstrip("/")
    for tail in ("/index.mdx", "/index.md", ".mdx", ".md"):
        if p.endswith(tail):
            return p[: -len(tail)]
    return p


_NUMBERED_PREFIX_RE = re.compile(r"(?:^|/)\d+_")


def _lookup_variants(s: str) -> list[str]:
    return [s, s + "/", s.rstrip("/")]


def lookup_slug(s: str, lk: Lookups) -> str | None:
    # Try the URL as-given first.
    for cand in _lookup_variants(s):
        if cand in lk.slug_to_url:
            return lk.slug_to_url[cand]
    # Fallback: strip a trailing `/index` or `/index.md(x)` (Mintlify collapses
    # those segments into the parent dir).
    if s.endswith("/index") or s.endswith("/index/"):
        stripped = s.rstrip("/")[: -len("/index")]
        for cand in _lookup_variants(stripped):
            if cand in lk.slug_to_url:
                return lk.slug_to_url[cand]
    # Fallback: strip Docusaurus numeric ordering prefixes from each path
    # segment (e.g. /docs/cloud/guides/01_setup -> /docs/cloud/guides/setup).
    if _NUMBERED_PREFIX_RE.search(s):
        stripped = re.sub(r"(^|/)\d+_", r"\1", s)
        for cand in _lookup_variants(stripped):
            if cand in lk.slug_to_url:
                return lk.slug_to_url[cand]
    return None


def resolve_relative(href: str, source_docu_file: str, lk: Lookups) -> str | None:
    src_dir = Path(source_docu_file).parent.as_posix()
    # Strip a fragment so it doesn't pollute the path lookup. The link rewriter
    # adds the original fragment back when it returns the resolved URL.
    href_path = href.split("#", 1)[0].split("?", 1)[0]
    # Two roots to try, in order:
    #   1. current file's directory (for ./foo.md, ../foo.md, foo.md siblings)
    #   2. the docs root (Docusaurus also treats bare paths like
    #      `operations/named-collections.md` as root-relative)
    roots = [src_dir, "docs"]
    for root in roots:
        full = posix_normpath(root + "/" + href_path)
        full = strip_md_tail(full)
        for cand in (full + ".md", full + ".mdx", full + "/index.md", full + "/index.mdx"):
            if cand in lk.docu_file_to_slug:
                slug = lk.docu_file_to_slug[cand]
                url = lookup_slug(slug, lk)
                if url:
                    return url
    # Asset fallback: a relative link to `../assets/list.json` (or any other
    # non-page file) that exists under mintlify-docs-dev/assets/ should resolve
    # to `/assets/<basename>`. The Mintlify build serves /assets/ as a static
    # tree; basename is unique enough for our purposes.
    basename = href_path.rsplit("/", 1)[-1]
    if basename and basename in lk.asset_basenames:
        return "/assets/" + basename

    # Slug-relative fallback: when no file or asset matches, Docusaurus also
    # resolves relative links against the *URL* of the current page. From
    # source page's slug, walk `./` and `../` segments to build a target slug,
    # then look it up. (Useful when the link author wrote `./X` for a sibling
    # page whose slug — not filename — is `<parent-slug>/X`.)
    src_slug = lk.docu_file_to_slug.get(source_docu_file)
    if src_slug:
        bare = href.split("#", 1)[0]
        # Walk slug parents:
        slug_parts = ("/" + src_slug.strip("/")).rsplit("/", 1)[0].split("/")
        for part in bare.split("/"):
            if part == "" or part == ".":
                continue
            if part == "..":
                if slug_parts:
                    slug_parts.pop()
                continue
            slug_parts.append(part.removesuffix(".md").removesuffix(".mdx"))
        candidate = "/".join(slug_parts) or "/"
        url = lookup_slug(candidate, lk)
        if url:
            return url
    return None


def _absolute(slug_path: str, frag: str, lk: Lookups, issues: list[str], original: str) -> str:
    s = strip_md_tail(slug_path)
    if not s.startswith("/"):
        s = "/" + s
    # Some Docusaurus links carry the /docs/ URL prefix; the slug doesn't.
    candidates = [s]
    if s.startswith("/docs/"):
        candidates.append(s[len("/docs"):])
    elif s == "/docs":
        candidates.append("/")
    for cand in candidates:
        url = lookup_slug(cand, lk)
        if url:
            return url + frag

    # Docusaurus also accepts file-path links (the path under clickhouse-docs/docs).
    # E.g. /operations/settings/settings-formats may not be a slug but
    # docs/operations/settings/settings-formats.md is a real file whose
    # frontmatter declares a different slug we can resolve via.
    for cand in candidates:
        rel = cand.lstrip("/")
        for tail in (".md", ".mdx", "/index.md", "/index.mdx"):
            docu = "docs/" + rel + tail
            if docu in lk.docu_file_to_slug:
                slug = lk.docu_file_to_slug[docu]
                url = lookup_slug(slug, lk)
                if url:
                    return url + frag

    # Asset fallback: an absolute path like `/assets/list.json` is already a
    # valid Mintlify URL if the file exists under mintlify-docs-dev/assets/.
    basename = slug_path.rsplit("/", 1)[-1]
    if basename and basename in lk.asset_basenames:
        return "/assets/" + basename + frag
    # Same idea but for any absolute path that points at an actual file in
    # this repo — Mintlify serves the file at that URL as-is. Covers
    # `/images/...`, `/static/...`, etc.
    if slug_path.startswith("/"):
        target = (THIS_REPO / slug_path.lstrip("/")).resolve()
        try:
            target.relative_to(THIS_REPO)
        except ValueError:
            target = None
        if target and target.exists() and target.is_file():
            return slug_path + frag
    issues.append(f"unknown slug: {original}")
    return f"{original}{_LINK_MARK}unknown slug"


def _resolve_local_asset(href: str, source_path: Path) -> str | None:
    """If href is a relative path to a file that exists on disk, return the
    correct URL form. Tries two roots: the source page's directory (for
    sibling assets) and the repo root (for bare paths like `assets/foo.csv`
    that authors meant to be repo-root-relative). Returns the original href
    when source-relative, an absolute `/path` when repo-root-relative."""
    bare, frag = (href.split("#", 1) + [""])[:2]
    bare = bare.split("?", 1)[0]
    if frag:
        frag = "#" + frag
    if not bare or bare.startswith(("/", "http://", "https://")):
        return None
    # First try source-relative (./foo, ../foo, sibling.png).
    target = (source_path.parent / bare).resolve()
    try:
        target.relative_to(THIS_REPO)
        if target.exists() and target.is_file():
            return href
    except ValueError:
        pass
    # Then try repo-root-relative (`assets/foo.csv`, `images/bar.png`) —
    # upstream Docusaurus tolerated bare paths as root-relative for assets,
    # but Mintlify needs an absolute URL.
    target = (THIS_REPO / bare).resolve()
    try:
        target.relative_to(THIS_REPO)
    except ValueError:
        return None
    if target.exists() and target.is_file():
        return "/" + bare + frag
    return None


_LINK_MARK = "\x00MIGRATE:"


def rewrite_links(text: str, source_docu_file: str | None, lk: Lookups, issues: list[str], dest_path: Path | None = None) -> str:
    # Older revisions of this script emitted MIGRATE markers as HTML comments
    # inline inside the markdown link href, e.g. `[x](url <!-- MIGRATE: ... -->)`.
    # That syntax breaks MDX parsing ("Unexpected character `!`") and renders
    # the page blank. Strip any legacy markers from prior runs.
    marker_re = re.compile(r"\s*(?:<!--\s*MIGRATE:[^>]*-->|\{/\*\s*MIGRATE:[^*]*\*/\})\s*")

    # Lines that start with `[//]:` are markdown reference-style comments —
    # their contents don't render. Skip rewriting any links on those lines.
    def is_comment_line_at(idx: int) -> bool:
        line_start = text.rfind("\n", 0, idx) + 1
        line_prefix = text[line_start:idx].lstrip()
        return line_prefix.startswith("[//]:")

    def rewrite_one(href: str) -> str:
        # Strip stale MIGRATE markers and any trailing markdown title attribute.
        href = marker_re.sub("", href)
        href = TITLE_TAIL_RE.sub("", href).strip()
        # Upstream sometimes writes `foo.md/#anchor` with a stray slash before
        # the fragment — normalize to `foo.md#anchor` so downstream resolution
        # treats it like any other `.md`-ending link.
        href = re.sub(r"\.(md|mdx)/(?=#|$)", r".\1", href)
        bare, frag = split_frag(href)
        if bare.startswith("https://clickhouse.com/docs"):
            return _absolute(bare[len("https://clickhouse.com/docs"):], frag, lk, issues, original=href)
        if bare.startswith("http://") or bare.startswith("https://"):
            return href
        if bare.startswith("/"):
            return _absolute(bare, frag, lk, issues, original=href)
        # Try local-asset resolution before classifying the link form.
        # Authors sometimes write bare paths like `assets/foo.csv` (which
        # Docusaurus treated as repo-root-relative) — _resolve_local_asset
        # absolutifies those to `/assets/foo.csv`.
        if dest_path is not None and not bare.endswith((".md", ".mdx")):
            local = _resolve_local_asset(href, dest_path)
            if local is not None:
                return local
        if bare.startswith("./") or bare.startswith("../") or bare.endswith((".md", ".mdx")):
            # First check whether the path resolves to an actual file in this
            # repo (image, .csv, etc. or even a sibling .md). If so, the
            # author already wrote the correct relative path — leave it alone.
            if dest_path is not None:
                local = _resolve_local_asset(href, dest_path)
                if local is not None:
                    return local
            if not source_docu_file:
                issues.append(f"relative link with no docusaurus source: {href}")
                return f"{href}{_LINK_MARK}cannot resolve relative link"
            url = resolve_relative(bare, source_docu_file, lk)
            if url:
                return url + frag
            issues.append(f"unresolved relative link: {href}")
            return f"{href}{_LINK_MARK}unresolved relative link"
        # Bare relative path without `./` or extension (e.g. `mergetree#x`,
        # `storing-data#x`, `settings/settings#x`). Docusaurus resolved these
        # against the source page's directory; try that path through
        # `resolve_relative`.
        if bare and source_docu_file:
            url = resolve_relative(bare, source_docu_file, lk)
            if url:
                return url + frag
        return href

    def _split_marker(rewritten: str) -> tuple[str, str | None]:
        """Pull a sentinel-encoded MIGRATE marker (if any) off the rewritten
        href. The marker is emitted outside the link/attr by the caller so
        the URL stays valid and the MDX parser doesn't choke on it."""
        if _LINK_MARK in rewritten:
            href, reason = rewritten.split(_LINK_MARK, 1)
            return href, reason
        return rewritten, None

    def md_repl(m):
        if is_comment_line_at(m.start()):
            return m.group(0)
        href, marker = _split_marker(rewrite_one(m.group("href")))
        link = f"[{m.group('text')}]({href})"
        if marker:
            link += f"{{/* MIGRATE: {marker} */}}"
        return link

    def html_repl(m):
        if is_comment_line_at(m.start()):
            return m.group(0)
        href, marker = _split_marker(rewrite_one(m.group(2)))
        attr = f'{m.group(1)}"{href}"'
        if marker:
            attr += f"{{/* MIGRATE: {marker} */}}"
        return attr

    text = LINK_RE.sub(md_repl, text)
    text = HREF_ATTR_RE.sub(html_repl, text)
    # Markdown reference-style link definitions: `[label]: <url>` or
    # `[label]: url` at the start of a line. Skip the special `[//]: # `
    # comment form (`is_comment_line_at` already excludes those for `[..](..)`).
    REF_DEF_RE = re.compile(
        r'^(?P<indent>[ \t]*)\[(?P<label>[^\]\n]+)\]:[ \t]+(?P<href>\S+)',
        re.MULTILINE,
    )
    def ref_repl(m):
        if m.group("label").strip() == "//":
            return m.group(0)
        href, marker = _split_marker(rewrite_one(m.group("href")))
        line = f'{m.group("indent")}[{m.group("label")}]: {href}'
        if marker:
            line += f"  {{/* MIGRATE: {marker} */}}"
        return line
    text = REF_DEF_RE.sub(ref_repl, text)
    # Strip any orphaned MIGRATE markers left outside a link's closing `)` from
    # previous migration runs. These appear as `](url){/* MIGRATE: ... */}...`
    # where the link itself was already rewritten cleanly.
    text = re.sub(r"(\]\([^)\n]+\))(\s*\{/\*\s*MIGRATE:[^*]*\*/\})+", r"\1", text)
    return text


# ----- per-file driver -------------------------------------------------------

def has_jsx(text: str) -> bool:
    return bool(re.search(r"^\s*import\s+", text, re.MULTILINE)) or bool(re.search(r"<[A-Z][A-Za-z0-9]*", text))


def _inject_import(text: str, import_line: str) -> str:
    m = FRONTMATTER_RE.match(text)
    insert_at = m.end() if m else 0
    rest = text[insert_at:]
    block = re.match(r"\n*((?:^\s*import\s.*\n)+)", rest, re.MULTILINE)
    if block:
        end = insert_at + block.end()
        return text[:end] + import_line + "\n" + text[end:]
    return text[:insert_at] + "\n" + import_line + "\n" + text[insert_at:]


@dataclass
class FileResult:
    src_path: Path
    dst_path: Path
    changed: bool
    skipped: bool
    skip_reason: str
    slug: str | None
    issues: list[str]


def migrate_file(path: Path, lk: Lookups, force: bool, dry_run: bool, docusaurus_root: Path) -> FileResult:
    rel = str(path.relative_to(THIS_REPO)).replace("\\", "/")
    # Allow lookup to succeed for either .md or .mdx form (extension may have
    # been flipped by a prior run before the slug-map was regenerated).
    row = lk.by_mintlify_file.get(rel)
    if not row and rel.endswith(".mdx"):
        row = lk.by_mintlify_file.get(rel[:-1])  # strip 'x'
    if not row and rel.endswith(".md"):
        row = lk.by_mintlify_file.get(rel + "x")
    issues: list[str] = []

    if row and not force:
        if row.get("migrated") == "true" and row.get("migrated_hash") == row.get("source_hash"):
            return FileResult(path, path, False, True, "up-to-date", row["docusaurus_slug"], [])

    # Read content from the Docusaurus source — that's the canonical version
    # that preserves heading IDs, blank lines, and other authoring detail. The
    # in-repo file is just the destination path. Fall back to in-repo only when
    # there is no Docusaurus counterpart (e.g. hand-authored landing pages).
    source_path = path
    if row and row.get("docusaurus_file"):
        candidate = docusaurus_root / row["docusaurus_file"]
        if candidate.exists():
            source_path = candidate
    elif not row:
        # No slug-map row. Two strategies to find the upstream source:
        #
        # 1. Path-suffix match: walk down the file's path components and look
        #    for an upstream docs/ file ending with the same suffix. Catches
        #    snippets/partials whose Docusaurus path mirrors this repo's
        #    layout under a different prefix.
        # 2. Sibling fallback: find a sibling .md/.mdx with a slug-map row
        #    and use ITS upstream parent dir as the source dir (for relative
        #    link resolution only — we don't pull its content).
        rel_parts = list(path.relative_to(THIS_REPO).parts)
        upstream_match = None
        docs_root = docusaurus_root / "docs"
        if docs_root.exists():
            # Two-pass: first try direct suffix-from-root (same path layout),
            # then try basename match anywhere in docs/ (handles dir renames
            # like interfaces/ -> core/reference/).
            for start in range(len(rel_parts)):
                cand_md = docs_root / Path(*rel_parts[start:])
                cand_md = cand_md.with_suffix(".md") if cand_md.suffix == ".mdx" else cand_md
                cand_mdx = cand_md.with_suffix(".mdx")
                if cand_md.exists():
                    upstream_match = cand_md; break
                if cand_mdx.exists():
                    upstream_match = cand_mdx; break

            if not upstream_match:
                # Basename match across docs/, prefer longest path suffix overlap.
                # Compare stems (extension-agnostic) so .md and .mdx siblings match.
                basename_md = path.name.replace(".mdx", ".md")
                basename_mdx = path.name if path.suffix == ".mdx" else path.with_suffix(".mdx").name
                rel_stems = [Path(p).stem for p in rel_parts]
                best = None
                best_overlap = -1
                for upstream in list(docs_root.rglob(basename_md)) + list(docs_root.rglob(basename_mdx)):
                    up_parts = upstream.relative_to(docs_root).parts
                    up_stems = [Path(p).stem for p in up_parts]
                    n = 0
                    while n < len(up_stems) and n < len(rel_stems) and up_stems[-1 - n] == rel_stems[-1 - n]:
                        n += 1
                    if n > best_overlap:
                        best_overlap = n
                        best = upstream
                # Require at least the basename + one parent dir to match,
                # to avoid pairing unrelated files that happen to share a name.
                # Snippets often live alone in `/snippets/`, so when the destination
                # has no parent context we accept basename-only.
                threshold = 1 if len(rel_parts) == 2 and rel_parts[0] == "snippets" else 2
                if best and best_overlap >= threshold:
                    upstream_match = best
        if upstream_match:
            source_path = upstream_match
            row = {
                "docusaurus_file": str(upstream_match.relative_to(docusaurus_root)).replace("\\", "/"),
                "docusaurus_slug": "",
                "source_hash": "",
                "migrated": "false",
                "migrated_hash": "",
            }
        else:
            sibling_row = None
            search_dir = path.parent
            for _ in range(4):
                for sib in search_dir.iterdir():
                    if not sib.is_file() or sib.suffix not in EXTS or sib == path:
                        continue
                    sib_rel = str(sib.relative_to(THIS_REPO)).replace("\\", "/")
                    r = lk.by_mintlify_file.get(sib_rel)
                    if r and r.get("docusaurus_file"):
                        sibling_row = r
                        break
                if sibling_row or search_dir == THIS_REPO:
                    break
                search_dir = search_dir.parent
            if sibling_row:
                # Use the sibling's upstream parent for relative-link resolution
                # only. We don't have an upstream source for THIS file, so read
                # in-repo content as-is.
                row = {
                    "docusaurus_file": sibling_row["docusaurus_file"],
                    "docusaurus_slug": "",
                    "source_hash": "",
                    "migrated": "false",
                    "migrated_hash": "",
                }
    original = source_path.read_text(encoding="utf-8")
    text, slug, title = transform_frontmatter(original)
    text = drop_redundant_h1(text, title)
    text = transform_heading_anchors(text)
    json_vars: dict[str, str] = {}
    text, image_vars = transform_imports(text, lk, issues, source_docu_path=source_path, json_vars=json_vars, dest_path=path)
    text = transform_use_base_url(text)
    text = transform_tocinline(text)
    text = transform_iframes(text)
    text = transform_admonitions(text)
    text = transform_details(text)
    text = transform_tabs(text)
    text = transform_inline_image_requires(text)
    text = transform_image_components(text, image_vars)
    text = transform_svg_components(text, image_vars)
    text = transform_image_logo_to_img(text)
    text = transform_static_image_paths(text)
    text = transform_inline_anchor(text)
    text = transform_vertical_stepper(text)
    text = transform_card_primary(text)
    text = transform_horizontal_divide(text)
    text = transform_two_column_list(text, json_vars)
    text, used_runnable = transform_runnable(text)
    text = transform_highlight_comments(text)

    source_docu_file = row["docusaurus_file"] if row else None
    text = rewrite_links(text, source_docu_file, lk, issues, dest_path=path)

    if used_runnable and "RunnableCode/RunnableCode.jsx" not in text:
        text = _inject_import(text, RUNNABLE_IMPORT)
    if image_vars and "/snippets/components/Image.jsx" not in text:
        text = _inject_import(text, IMAGE_IMPORT)
    if re.search(r"<AgentPrompt(?:[\s/>])", text) and "AgentPrompt/AgentPrompt.jsx" not in text:
        text = _inject_import(text, AGENT_PROMPT_IMPORT)

    # Remove unused nested snippet imports. Imports that remain stay local to
    # the file that renders the imported snippet.
    text = prune_unused_mdx_imports(text)

    # Apply any per-file override (rare; for upstream patterns that don't map
    # cleanly to a generic transform, e.g. <Link><CardSecondary/></Link>).
    if row and row.get("docusaurus_file") in POST_TRANSFORM_OVERRIDES:
        text = POST_TRANSFORM_OVERRIDES[row["docusaurus_file"]](text)

    text = transform_html_comments(text)
    text = ensure_blank_after_imports(text)

    # Standardize on `.mdx` for every page so contributors don't have to
    # think about which extension to use. Mintlify renders both, but a single
    # extension keeps cross-file imports/links predictable.
    dst = path if path.suffix == ".mdx" else path.with_suffix(".mdx")

    # When source is the Docusaurus file, "changed" should compare against the
    # current destination content — the destination is what we're writing.
    current_dst = path.read_text(encoding="utf-8") if path.exists() else ""
    changed = (text != current_dst) or (dst != path)
    if not dry_run and changed:
        dst.write_text(text, encoding="utf-8")
        if dst != path and path.exists():
            path.unlink()
    return FileResult(path, dst, changed, False, "", slug, issues)


# ----- driver ----------------------------------------------------------------

SKIP_TOPLEVEL = {"AGENTS.md", "README.md", "LICENSE.md", "CHANGELOG.md", "CONTRIBUTING.md"}
# Hand-authored files that have no faithful upstream counterpart and must not
# be regenerated by the migrator. Paths are relative to THIS_REPO.
SKIP_FILES = {
    # Mintlify does not reliably pass variables through a parent snippet that
    # renders nested snippets. These three hand-authored snippets split the
    # provider-specific ClickPipe steps from their shared tail.
    "snippets/_create_clickpipe.mdx",
    "snippets/clickpipes/object-storage/_create_s3_clickpipe.mdx",
    "snippets/clickpipes/object-storage/_create_gcs_clickpipe.mdx",
    # Bridges the Docusaurus pattern of importing one .md page inline into
    # another (`@site/docs/sql-reference/statements/truncate.md`). Mintlify
    # can't import full pages, so this snippet holds the SQL-ref body and is
    # consumed by `concepts/features/operations/delete/truncate.mdx`. Re-migrating it
    # would replace the body with the wrapper page's content and break the
    # render.
    "snippets/truncate.mdx",
    # Hand-authored Mintlify CardGroup landing page using brand SVG logos.
    # The upstream Docusaurus page is a markdown table; we deliberately
    # diverge here for a richer landing layout.
    "integrations/language-clients/index.mdx",
    # Heavily post-processed from the VLDB 2024 paper migration: figures
    # converted to <Frame caption="...">, inline citations wrapped in <sup>,
    # reference list styled with smaller font and superscript numbers, and
    # scroll-margin-top anchors added throughout. A force-migrate would
    # overwrite all of these Mintlify-specific transforms.
    "concepts/core-concepts/academic-overview.mdx",
    # Upstream SVG diagram was migrated with width="32" (icon-sized). Fixed to
    # use <Image size="lg"> for full-width display.
    "products/bring-your-own-cloud/overview/architecture.mdx",
    # Hand-authored card-grid landing page with custom React components (CsCard,
    # useDark) and icon grid for Applications, Infrastructure, and Databases &
    # Services. The upstream Docusaurus page is a plain markdown table; migrating
    # would destroy the grid layout.
    "clickstack/index.mdx",
    # MP4 import removed (Mintlify has no file-loader for video assets); src
    # changed to a plain string path. Video wrapped in <Frame>.
    "clickstack/service-maps.mdx",
    # GIF imported as a JS module variable (clickpy_trace) which Mintlify
    # can't resolve. Replaced with a plain string path inside a <Frame>.
    "clickstack/features/session-replay.mdx",
}
# Path prefixes (relative to THIS_REPO) whose pages are tracked outside the
# Docusaurus pipeline. The migrator must never overwrite them: their canonical
# source lives elsewhere (e.g. a product team's own repo) and the Docusaurus
# copy is downstream / often stale.
SKIP_PATH_PREFIXES = (
    # Synced manually from https://github.com/ClickHouse/clickhouse-operator/tree/main/docs
    "products/kubernetes-operator/",
    # Hand-authored use-case landing pages with custom React components
    # (ExclusiveGroup, PrimaryButton) and ClickStack-specific narrative that
    # deliberately diverges from the upstream Docusaurus source. Some pages
    # (agentic-analytics.mdx) have no upstream counterpart at all.
    "get-started/use-cases/",
)


def _is_skip_path(rel_posix: str) -> bool:
    return rel_posix in SKIP_FILES or any(
        rel_posix.startswith(p) for p in SKIP_PATH_PREFIXES
    )


def iter_targets(target: Path | None) -> list[Path]:
    out: list[Path] = []
    if target is None:
        for p in THIS_REPO.rglob("*"):
            if not p.is_file() or p.suffix not in EXTS:
                continue
            rel = p.relative_to(THIS_REPO)
            if any(part in ITER_SKIP_DIRS for part in rel.parts):
                continue
            # Repo-root meta files (AGENTS, README, LICENSE …) aren't pages.
            if len(rel.parts) == 1 and rel.parts[0] in SKIP_TOPLEVEL:
                continue
            if _is_skip_path(rel.as_posix()):
                continue
            out.append(p)
        return sorted(out)
    if target.is_file():
        if _is_skip_path(target.relative_to(THIS_REPO).as_posix()):
            return []
        return [target]
    if target.is_dir():
        for ext in EXTS:
            out.extend(target.rglob(f"*{ext}"))
        return sorted(p for p in out if not any(part in ITER_SKIP_DIRS for part in p.relative_to(THIS_REPO).parts) and not _is_skip_path(p.relative_to(THIS_REPO).as_posix()))
    return []


def sync_image_assets(docusaurus_root: Path, overwrite: bool = False) -> tuple[int, int]:
    """Copy upstream `static/images/**` files into this repo's `images/**`.

    Default: copy only files missing locally. Existing files are left alone so
    repo-specific overrides aren't clobbered.

    With `overwrite=True`: also replace local copies whose contents differ from
    upstream (same path, different bytes). Use when upstream images have been
    updated since the last sync. Orphan files (local-only paths) are never
    touched.

    Returns (copied_new, overwritten_existing).
    """
    src_root = docusaurus_root / "static" / "images"
    dst_root = THIS_REPO / "images"
    if not src_root.exists():
        return 0, 0
    copied = 0
    overwritten = 0
    for src in src_root.rglob("*"):
        if not src.is_file():
            continue
        rel = src.relative_to(src_root)
        dst = dst_root / rel
        if dst.exists():
            if not overwrite:
                continue
            try:
                if src.read_bytes() == dst.read_bytes():
                    continue
            except OSError:
                continue
            shutil.copy2(src, dst)
            overwritten += 1
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)
        copied += 1
    return copied, overwritten


def update_slug_map(slug_map_csv: Path, all_rows: list[dict], updates: dict[str, dict[str, str]]) -> None:
    fields = list(all_rows[0].keys())
    for r in all_rows:
        u = updates.get(r["docusaurus_slug"])
        if u:
            r.update(u)
    with slug_map_csv.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        w.writerows(all_rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path", nargs="?", help="file or directory to migrate")
    ap.add_argument("--all", action="store_true", help="migrate every page in this repo")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="re-migrate pages even if up-to-date")
    ap.add_argument("--slug-map", type=Path, default=THIS_REPO / "_migration" / "slug-map.csv")
    ap.add_argument("--docusaurus", type=Path, default=DEFAULT_DOCUSAURUS,
                    help="path to the clickhouse-docs (Docusaurus) repo — content source of truth")
    ap.add_argument("--sync-assets", action="store_true",
                    help="copy upstream static/images/** into this repo's images/** (skips files that already exist)")
    ap.add_argument("--overwrite-drifted", action="store_true",
                    help="with --sync-assets, also overwrite local images whose contents differ from upstream "
                         "(use when upstream has updated screenshots; orphan local-only files are never touched)")
    args = ap.parse_args()

    if args.sync_assets and not args.path and not args.all:
        copied, overwritten = sync_image_assets(args.docusaurus, overwrite=args.overwrite_drifted)
        print(f"Synced {copied} new image file(s) from {args.docusaurus}/static/images")
        if args.overwrite_drifted:
            print(f"Overwrote {overwritten} drifted file(s) with upstream content")
        return 0

    if not args.all and not args.path:
        ap.error("provide a path or pass --all (or --sync-assets)")
    if not args.slug_map.exists():
        ap.error(f"slug-map.csv not found at {args.slug_map}; run _migration/generate-slug-map.py first")

    lk, all_rows = build_lookups(args.slug_map)

    target = None
    docu_targets = None
    if args.path:
        target = Path(args.path)
        if not target.is_absolute():
            target = THIS_REPO / target
        # Allow the path to point into the Docusaurus source tree (e.g.
        # `--docusaurus ~/Desktop/clickhouse-docs .../docs/development`). The
        # migrator operates on this repo's files, so translate the source
        # path into the corresponding Mintlify destination files via the
        # slug map's docusaurus_file -> mintlify_file mapping.
        docu_root = args.docusaurus.resolve()
        try:
            rel_docu = target.resolve().relative_to(docu_root).as_posix()
        except ValueError:
            rel_docu = None
        if rel_docu is not None:
            prefix = rel_docu.rstrip("/")
            seen: set[Path] = set()
            docu_targets = []
            for r in all_rows:
                df = r["docusaurus_file"]
                if df != prefix and not df.startswith(prefix + "/"):
                    continue
                mf = r["mintlify_file"].split(" | ")[0] if r["mintlify_file"] else ""
                if not mf:
                    continue
                p = THIS_REPO / mf
                if p in seen:
                    continue
                seen.add(p)
                if p.exists() and not _is_skip_path(mf):
                    docu_targets.append(p)
            docu_targets = sorted(docu_targets)

    targets = docu_targets if docu_targets is not None else iter_targets(target)
    if not targets:
        print("No targets found", file=sys.stderr)
        return 1

    # Always sync image assets when running --all so missing referenced
    # /images/... assets get pulled in automatically.
    if args.all:
        copied, overwritten = sync_image_assets(args.docusaurus, overwrite=args.overwrite_drifted)
        if copied:
            print(f"Synced {copied} new image file(s) from {args.docusaurus}/static/images")
        if overwritten:
            print(f"Overwrote {overwritten} drifted file(s) with upstream content")

    updates: dict[str, dict[str, str]] = {}
    skipped = changed = total_issues = 0
    now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    print(f"Processing {len(targets)} file(s)" + (" (dry-run)" if args.dry_run else "") + (" --force" if args.force else ""))
    for p in targets:
        r = migrate_file(p, lk, args.force, args.dry_run, args.docusaurus)
        if r.skipped:
            skipped += 1
            continue
        if not r.changed:
            continue
        changed += 1
        total_issues += len(r.issues)
        arrow = " -> " + str(r.dst_path.relative_to(THIS_REPO)) if r.dst_path != r.src_path else ""
        mark = f" [{len(r.issues)} issue(s)]" if r.issues else ""
        print(f"  {r.src_path.relative_to(THIS_REPO)}{arrow}{mark}")
        for i in r.issues:
            print(f"      - {i}")
        if r.slug:
            slug_key = normalize_slug(r.slug)
            row = next((row for row in all_rows if row["docusaurus_slug"] == slug_key), None)
            if row:
                updates[slug_key] = {
                    "migrated": "true",
                    "migrated_hash": row.get("source_hash", ""),
                    "migrated_at": now,
                    "mintlify_file": str(r.dst_path.relative_to(THIS_REPO)).replace("\\", "/"),
                }

    if not args.dry_run and updates:
        update_slug_map(args.slug_map, all_rows, updates)
        print(f"Updated migrated/migrated_hash/migrated_at on {len(updates)} row(s)")

    print(f"\nDone. {changed} changed, {skipped} skipped (up-to-date), {total_issues} issue(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
