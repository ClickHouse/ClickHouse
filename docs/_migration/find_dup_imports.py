"""Find every page where importing an .mdx snippet would cause a duplicate
identifier error (the snippet declares an import binding the page already has).

Mintlify hoists the imports from any imported MDX snippet into the compiled
output, even if the imported component is never used. That breaks the page
bundle ("Identifier 'X' has already been declared"), producing a blank page."""

import re
from pathlib import Path

REPO = Path("/Users/sstruw/Desktop/mintlify-docs-dev")
IMPORT_RE = re.compile(
    r'^\s*import\s+(?P<spec>.+?)\s+from\s+["\'](?P<src>[^"\']+)["\']',
    re.MULTILINE,
)

def frontmatter_end(text: str) -> int:
    if not text.startswith("---"):
        return 0
    end = text.find("\n---", 3)
    return end + 4 if end != -1 else 0


_FENCE_RE = re.compile(r"^([ \t]*)(```+|~~~+)", re.MULTILINE)


def _code_spans(text: str):
    """Return (start, end) byte spans of fenced code blocks so `import`
    lines inside code examples aren't treated as real top-level imports."""
    spans = []
    open_pos = None
    open_tok = None
    for m in _FENCE_RE.finditer(text):
        tok = m.group(2)[:3]
        if open_pos is None:
            open_pos, open_tok = m.start(), tok
        elif tok == open_tok:
            spans.append((open_pos, m.end()))
            open_pos = open_tok = None
    if open_pos is not None:
        spans.append((open_pos, len(text)))
    return spans


def parse_top_imports(text: str):
    """Return list of (specifier_names, source_path) for top-level imports."""
    end = frontmatter_end(text)
    head = text[end:]
    spans = _code_spans(head)
    def in_code(pos: int) -> bool:
        return any(s <= pos < e for s, e in spans)
    # Only consider the leading section before significant body content;
    # a top-level import after JSX is still allowed in MDX so scan whole file.
    out = []
    for m in IMPORT_RE.finditer(head):
        if in_code(m.start()):
            continue
        spec = m.group("spec").strip()
        src = m.group("src").strip()
        names = []
        # Default import: `import X from "..."`
        # Named imports: `import { A, B as C } from "..."`
        # Combo: `import X, { A, B } from "..."`
        # Namespace: `import * as X from "..."`
        if "{" in spec:
            default_part = spec.split("{", 1)[0].rstrip(",").strip()
            if default_part:
                names.append(default_part)
            inner = spec[spec.index("{") + 1 : spec.rindex("}")]
            for piece in inner.split(","):
                piece = piece.strip()
                if not piece:
                    continue
                if " as " in piece:
                    piece = piece.split(" as ", 1)[1].strip()
                names.append(piece)
        elif spec.startswith("*"):
            # `* as X`
            after = spec.split(" as ", 1)
            if len(after) == 2:
                names.append(after[1].strip())
        else:
            names.append(spec)
        out.append((names, src))
    return out


def resolve_snippet(src: str) -> Path | None:
    """Resolve an MDX import source to a filesystem path under the repo."""
    if not (src.startswith("/snippets/") or src.endswith((".mdx", ".md"))):
        return None
    if src.startswith("/"):
        # Mintlify-style absolute path: rooted at the repo root.
        return REPO / src.lstrip("/")
    return None  # relative snippet imports rare; ignore for now


def collect_snippet_imports(snippet_path: Path, seen: set):
    """Recursively gather the set of binding names declared by snippet
    and any snippets it transitively imports."""
    if snippet_path in seen or not snippet_path.is_file():
        return set()
    seen.add(snippet_path)
    text = snippet_path.read_text(encoding="utf-8", errors="ignore")
    names: set[str] = set()
    for n, src in parse_top_imports(text):
        names.update(n)
        if src.endswith((".mdx", ".md")):
            child = resolve_snippet(src)
            if child:
                names.update(collect_snippet_imports(child, seen))
    return names


def main():
    pages = []
    for p in REPO.rglob("*.mdx"):
        rel = p.relative_to(REPO).as_posix()
        if rel.startswith((".claude/", "node_modules/", "_site/", "_migration/")):
            continue
        pages.append(p)

    issues = []
    for page in pages:
        text = page.read_text(encoding="utf-8", errors="ignore")
        page_imports = parse_top_imports(text)
        page_names: dict[str, str] = {}  # name → source (first declarer)
        # First, build a record of the page's own top-level names
        for names, src in page_imports:
            for n in names:
                if n in page_names and page_names[n] != src:
                    issues.append((page.relative_to(REPO).as_posix(),
                                   "self", n, page_names[n], src))
                else:
                    page_names.setdefault(n, src)
        # Then, for each imported MDX snippet, check whether its hoisted
        # imports clash with the page's own imports.
        for names, src in page_imports:
            if not src.endswith((".mdx", ".md")):
                continue
            snippet_path = resolve_snippet(src)
            if not snippet_path or not snippet_path.is_file():
                continue
            snippet_names = collect_snippet_imports(snippet_path, set())
            for sn in snippet_names:
                if sn in page_names and page_names[sn] != src:
                    issues.append((page.relative_to(REPO).as_posix(),
                                   src, sn, page_names[sn], src))

    # Deduplicate by (page, name)
    seen = set()
    dedup = []
    for row in issues:
        key = (row[0], row[2])
        if key in seen:
            continue
        seen.add(key)
        dedup.append(row)

    print(f"Found {len(dedup)} duplicate-identifier issue(s) across {len(set(r[0] for r in dedup))} page(s)\n")
    for page, snippet, name, src1, src2 in sorted(dedup):
        print(f"  {page}")
        print(f"      duplicate '{name}' -> page imports from {src1!r}")
        print(f"                            snippet at {src2!r} re-declares it")
    return 0 if not dedup else 1


if __name__ == "__main__":
    raise SystemExit(main())
