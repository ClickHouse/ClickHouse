"""Convert the Docusaurus-flavoured Markdown in ClickHouse source registrations
(settings, functions, engines, formats, ...) into Mintlify MDX for
`autogenerate_docs.py`.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from posixpath import normpath as posix_normpath

SKIP_DIRS = {"node_modules", ".git", "i18n", ".claude", ".mintlify", "scripts", "snippets", "static"}
# Translation dirs are managed by the localisation bot — never migrate them.
TRANSLATION_DIRS = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}
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
    docs_root: Path
    slug_to_url: dict[str, str] = field(default_factory=dict)
    docu_file_to_slug: dict[str, str] = field(default_factory=dict)
    snippet_basename_to_path: dict[str, str] = field(default_factory=dict)  # legacy: first path
    snippet_basename_to_paths: dict[str, list[str]] = field(default_factory=dict)  # all paths
    asset_basenames: set = field(default_factory=set)  # files in mintlify/assets/


def file_to_url(rel_path: str) -> str:
    s = rel_path.rsplit(".", 1)[0].replace("\\", "/")
    if s == "index":
        return "/"
    return "/" + s


def normalize_slug(s: str) -> str:
    return "/" + s.lstrip("/")


def build_lookups(docs_root: Path) -> Lookups:
    docs_root = docs_root.resolve()
    lk = Lookups(docs_root=docs_root)

    # Make already-Mintlify-form URLs idempotent: a link that's already a current
    # mintlify URL should resolve to itself on re-runs. We register URLs for
    # every file actually present on disk, including pages with no `slug:`
    # field (e.g. landing pages).
    for p in docs_root.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        rel = p.relative_to(docs_root)
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
        # the canonical slug resolve correctly.
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

    # Legacy (Docusaurus) routes still used by source registrations resolve
    # through the site's redirect table. A legacy file was served at its path.
    redirects = json.loads((docs_root / "_site" / "redirects.json").read_text(encoding="utf-8"))
    for redirect in redirects:
        source, destination = redirect["source"], redirect["destination"]
        for key in (source, source.rstrip("/"), source + "/"):
            lk.slug_to_url.setdefault(key, destination)
        for tail in (".md", ".mdx", "/index.md", "/index.mdx"):
            lk.docu_file_to_slug.setdefault("docs" + source.rstrip("/") + tail, source)

    snippets_root = docs_root / "snippets"
    if snippets_root.exists():
        for p in snippets_root.rglob("*.mdx"):
            url = "/" + str(p.relative_to(docs_root)).replace("\\", "/")
            lk.snippet_basename_to_path.setdefault(p.name, url)
            lk.snippet_basename_to_paths.setdefault(p.name, []).append(url)

    assets_root = docs_root / "assets"
    if assets_root.exists():
        for p in assets_root.rglob("*"):
            if p.is_file():
                lk.asset_basenames.add(p.name)
    return lk


# ----- frontmatter -----------------------------------------------------------

FRONTMATTER_RE = re.compile(r"\A---\n(.*?)\n---\n?", re.DOTALL)


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


# `<iframe ...>...</iframe>` -> `<Frame><iframe ...>...</iframe></Frame>`
# also strips `width="..."` and `height="..."` attributes so the iframe scales
# naturally inside the Frame. Skip iframes that are already inside a <Frame>.
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


# `<!-- ... -->` is valid in Markdown but illegal in MDX (it's parsed as JSX
# and `!` cannot start a JSX identifier). Convert to MDX-style `{/* ... */}`.
# Skip occurrences inside fenced code blocks so `<!-- comment -->` shown as a
# code example stays verbatim. Also leave MIGRATE markers alone: they are
# tooling-detected by their `<!--` form and should remain visible until fixed.
HTML_COMMENT_RE = re.compile(r"<!--(.*?)-->", re.DOTALL)


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
            return ("/" + dest_path.relative_to(lk.docs_root).as_posix()) == target_url
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

    def _named_badge_spec(spec: str, exported_name: str) -> str:
        if not re.fullmatch(r"\w+", spec):
            return spec
        if spec == exported_name:
            return f"{{ {exported_name} }}"
        return f"{{ {exported_name} as {spec} }}"

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

        # @theme/TOCInline: Mintlify auto-generates per-page TOC, so delete
        # the import.
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
                folder = lk.docs_root / "snippets" / "components" / n / f"{n}.jsx"
                flat = lk.docs_root / "snippets" / "components" / f"{n}.jsx"
                mintlify_spec = spec
                if "/badges/" in src:
                    # Mintlify's MDX compiler requires page badges to use their
                    # named exports. A default import can render the badge and
                    # silently omit every sibling that follows it.
                    mintlify_spec = _named_badge_spec(spec, n)
                if folder.exists():
                    return f'import {mintlify_spec} from "/snippets/components/{n}/{n}.jsx";'
                if flat.exists():
                    return f'import {mintlify_spec} from "/snippets/components/{n}.jsx";'
            issues.append(f"unmapped @theme import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        # Structured documentation already uses the migrated snippet path, so
        # it bypasses the @theme rewrite above. Normalize those badge imports
        # too, otherwise each docs regeneration changes them back to default
        # imports and silently hides the content following the badge.
        if src.startswith("/snippets/components/") and src.endswith("Badge.jsx"):
            exported_name = Path(src).stem
            return f'import {_named_badge_spec(spec, exported_name)} from "{src}";'

        # @site/src/components/<...>/<Name>  -> /snippets/components/<Name>/<Name>.jsx
        # @site/src/theme/<...>/<Name>       -> same lookup pattern (mirrors @theme/<Name>)
        if src.startswith("@site/src/components/") or src.startswith("@site/src/theme/"):
            prefix = "@site/src/components/" if src.startswith("@site/src/components/") else "@site/src/theme/"
            tail = src[len(prefix):]
            name = tail.rsplit("/", 1)[-1]
            folder = lk.docs_root / "snippets" / "components" / name / f"{name}.jsx"
            flat = lk.docs_root / "snippets" / "components" / f"{name}.jsx"
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
            target = lk.docs_root / "snippets" / "lib" / f"{name}.jsx"
            if target.exists():
                return f'import {spec} from "/snippets/lib/{name}.jsx";'
            issues.append(f"unmapped @site/src/lib import: {src}")
            return f"{{/* MIGRATE: unmapped import {src!r} */}}"

        # @docusaurus/useBaseUrl is a hook that prepends the site's baseUrl.
        # In Mintlify the URL is the file path as-is, so we delete the import.
        if src == "@docusaurus/useBaseUrl":
            return ""

        # @docusaurus/useBrokenLinks is a Docusaurus build-time hook with no
        # Mintlify analog. Body usage typically does nothing visible — delete
        # the import; usages would already be no-ops or absent in this repo.
        if src == "@docusaurus/useBrokenLinks":
            return ""

        # @clickhouse/click-ui/bundled: Mintlify only allows local imports.
        if src == "@clickhouse/click-ui/bundled":
            return ""

        # `/src/components/...` — Docusaurus-style absolute import path that
        # points into the upstream React component tree. Mintlify has no
        # equivalent. Drop the import.
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
        target = (lk.docs_root / slug_path.lstrip("/")).resolve()
        try:
            target.relative_to(lk.docs_root)
        except ValueError:
            target = None
        if target and target.exists() and target.is_file():
            return slug_path + frag
    issues.append(f"unknown slug: {original}")
    return f"{original}{_LINK_MARK}unknown slug"


def _resolve_local_asset(href: str, source_path: Path, docs_root: Path) -> str | None:
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
        target.relative_to(docs_root)
        if target.exists() and target.is_file():
            return href
    except ValueError:
        pass
    # Then try repo-root-relative (`assets/foo.csv`, `images/bar.png`) —
    # upstream Docusaurus tolerated bare paths as root-relative for assets,
    # but Mintlify needs an absolute URL.
    target = (docs_root / bare).resolve()
    try:
        target.relative_to(docs_root)
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
            local = _resolve_local_asset(href, dest_path, lk.docs_root)
            if local is not None:
                return local
        if bare.startswith("./") or bare.startswith("../") or bare.endswith((".md", ".mdx")):
            # First check whether the path resolves to an actual file in this
            # repo (image, .csv, etc. or even a sibling .md). If so, the
            # author already wrote the correct relative path — leave it alone.
            if dest_path is not None:
                local = _resolve_local_asset(href, dest_path, lk.docs_root)
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


# ----- body pipeline ---------------------------------------------------------

def _inject_import(text: str, import_line: str) -> str:
    m = FRONTMATTER_RE.match(text)
    insert_at = m.end() if m else 0
    rest = text[insert_at:]
    block = re.match(r"\n*((?:^\s*import\s.*\n)+)", rest, re.MULTILINE)
    if block:
        end = insert_at + block.end()
        return text[:end] + import_line + "\n" + text[end:]
    return text[:insert_at] + "\n" + import_line + "\n" + text[insert_at:]


def transform_body_content(
    text: str,
    *,
    title: str | None,
    lk: Lookups,
    issues: list[str],
    source_docu_path: Path | None,
    dest_path: Path | None,
    source_docu_file: str | None = None,
) -> str:
    """Apply the full Docusaurus->Mintlify body pipeline to a page body."""
    text = drop_redundant_h1(text, title)
    text, image_vars = transform_imports(text, lk, issues, source_docu_path=source_docu_path, dest_path=dest_path)
    text = transform_iframes(text)
    text = transform_admonitions(text)
    text = transform_details(text)
    text = transform_tabs(text)
    text = transform_image_components(text, image_vars)
    text = transform_svg_components(text, image_vars)
    text = transform_image_logo_to_img(text)
    text, used_runnable = transform_runnable(text)
    text = transform_highlight_comments(text)

    text = rewrite_links(text, source_docu_file, lk, issues, dest_path=dest_path)

    if used_runnable and "RunnableCode/RunnableCode.jsx" not in text:
        text = _inject_import(text, RUNNABLE_IMPORT)
    if image_vars and "/snippets/components/Image.jsx" not in text:
        text = _inject_import(text, IMAGE_IMPORT)
    if re.search(r"<AgentPrompt(?:[\s/>])", text) and "AgentPrompt/AgentPrompt.jsx" not in text:
        text = _inject_import(text, AGENT_PROMPT_IMPORT)

    # Remove unused nested snippet imports. Imports that remain stay local to
    # the file that renders the imported snippet.
    text = prune_unused_mdx_imports(text)

    text = transform_html_comments(text)
    text = ensure_blank_after_imports(text)
    return text
