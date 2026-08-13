#!/usr/bin/env python3
"""Guard against drift between a shared documentation snippet and its hand-embedded copies.

The website (Mintlify) resolves `import ... from '/snippets/*.mdx'` at build time, so a page
that uses a snippet as a self-closing tag (e.g. `<PrettyFormatSettings/>`) shows the snippet's
content on the website. The built-in help surfaces have no Mintlify: the terminal `help` renderer
(`src/Client/TerminalMarkdownRenderer.cpp`) and the embedded `/docs` page
(`programs/server/docs.html`) each carry their own copy of that content in a `DOC_SNIPPETS` table
and substitute it in place of the tag.

Those copies must not silently drift from the real snippet source under `docs/snippets/`: this
check regenerates the expected copy from the `.mdx` and fails if either embedded table disagrees,
so editing a shared snippet forces the embedded copies to be updated in the same change.

The check also scans the embedded documentation sources under `src/` for snippet imports
(`import Foo from '/snippets/foo.mdx'`): a snippet an embedded page actually uses must have an
entry in both `DOC_SNIPPETS` tables in the first place, otherwise the website would render it
while `help` and `/docs` silently drop it — so converting a page that introduces a new snippet
forces the tables to be extended in the same change.

Run standalone (`python3 check_embedded_doc_snippets.py`) or via the `Style check` job; prints one
line per mismatch and exits non-zero if anything is out of sync.
"""

import json
import os
import re
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", ".."))

CPP_PATH = os.path.join(ROOT, "src", "Client", "TerminalMarkdownRenderer.cpp")
HTML_PATH = os.path.join(ROOT, "programs", "server", "docs.html")
SNIPPETS_DIR = os.path.join(ROOT, "docs", "snippets")
SRC_DIR = os.path.join(ROOT, "src")

# An `import Foo from '/snippets/foo.mdx';` line inside an embedded documentation body. Anchored to
# the start of a physical line: that is how the help-surface renderers recognize an import, and it
# keeps mentions inside code comments (which are prefixed by `///`) out of the scan.
IMPORT_RE = re.compile(
    r"^[ \t]*import\s+[A-Za-z_$][A-Za-z0-9_$]*\s+from\s+['\"]([^'\"]+\.mdx)['\"]", re.MULTILINE
)

# A leading MDX block comment (`{/* ... */}`) is website-authoring machinery, not content, and is
# not carried into the embedded copies; drop it (and any surrounding blank lines) before comparing.
MDX_COMMENT_RE = re.compile(r"^\s*\{/\*.*\*/\}\s*$")


def normalize_snippet_body(text):
    """The body of a `docs/snippets/*.mdx` file as it is embedded: leading MDX comment and
    surrounding blank lines removed, inner content (including trailing whitespace) kept verbatim."""
    lines = text.split("\n")
    start = 0
    while start < len(lines) and (lines[start].strip() == "" or MDX_COMMENT_RE.match(lines[start])):
        start += 1
    return "\n".join(lines[start:]).strip("\n")


def extract_cpp_snippets(text):
    """The `DOC_SNIPPETS` table in `TerminalMarkdownRenderer.cpp`: `{"suffix", R"DOCS_MD(body)DOCS_MD"}`."""
    block = re.search(r"DOC_SNIPPETS\[\]\s*=\s*\{(.*?)\n\};", text, re.DOTALL)
    if not block:
        return None
    entries = re.findall(r'\{"([^"]+)",\s*R"DOCS_MD\((.*?)\)DOCS_MD"\}', block.group(1), re.DOTALL)
    return {suffix: body.strip("\n") for suffix, body in entries}


def extract_html_snippets(text):
    """The `DOC_SNIPPETS` object in `docs.html`: `"suffix": "json-escaped-body"` (one per line)."""
    block = re.search(r"const DOC_SNIPPETS\s*=\s*\{(.*?)\n\s*\};", text, re.DOTALL)
    if not block:
        return None
    result = {}
    for m in re.finditer(r'^\s*"([^"]+)":\s*"(.*)",\s*$', block.group(1), re.MULTILINE):
        result[m.group(1)] = json.loads('"' + m.group(2) + '"').strip("\n")
    return result


def find_snippet_imports():
    """Every snippet import inside the embedded documentation under `src/`, as a list of
    (path relative to the repository root, line number, imported path) tuples. Test sources are
    excluded: gtests exercise the snippet resolution with inline fixtures, not real documentation.
    A native `grep` narrows the walk to candidate files first — the sources are many and reading
    them all from Python is much slower."""
    result = subprocess.run(
        [
            "grep", "-rlF", ".mdx", "--include=*.cpp", "--include=*.h",
            "--exclude-dir=tests", "--exclude-dir=examples", "--exclude-dir=fuzzers",
            SRC_DIR,
        ],
        stdout=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode > 1:  # 1 only means "no matches"
        raise RuntimeError(f"grep over {SRC_DIR} failed with exit code {result.returncode}")
    used = []
    for path in sorted(result.stdout.splitlines()):
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
        for m in IMPORT_RE.finditer(text):
            used.append((os.path.relpath(path, ROOT), text.count("\n", 0, m.start()) + 1, m.group(1)))
    return used


def main():
    with open(CPP_PATH, encoding="utf-8") as f:
        cpp = extract_cpp_snippets(f.read())
    with open(HTML_PATH, encoding="utf-8") as f:
        html = extract_html_snippets(f.read())

    errors = []
    if cpp is None:
        errors.append(f"{CPP_PATH}: could not find the DOC_SNIPPETS table")
    if html is None:
        errors.append(f"{HTML_PATH}: could not find the DOC_SNIPPETS table")

    if cpp is not None and html is not None:
        if set(cpp) != set(html):
            errors.append(
                "DOC_SNIPPETS keys differ between TerminalMarkdownRenderer.cpp and docs.html: "
                f"only in .cpp={sorted(set(cpp) - set(html))}, only in docs.html={sorted(set(html) - set(cpp))}"
            )

        # A snippet the embedded docs actually use must be resolvable on both help surfaces: the
        # website expands the import at build time, but `help` and `/docs` can only substitute
        # entries present in their `DOC_SNIPPETS` tables (matched by path suffix) — a missing entry
        # means the snippet's content silently disappears from the built-in help.
        resolvable = set(cpp) & set(html)
        for source, line, path in find_snippet_imports():
            if not any(path.endswith(suffix) for suffix in resolvable):
                errors.append(
                    f"{source}:{line}: the embedded documentation imports '{path}', which has no entry "
                    "in the DOC_SNIPPETS tables; add the snippet body to both "
                    "src/Client/TerminalMarkdownRenderer.cpp and programs/server/docs.html so the "
                    "built-in help surfaces render it instead of dropping it"
                )

        for suffix in sorted(set(cpp) & set(html)):
            mdx_path = os.path.join(SNIPPETS_DIR, suffix)
            if not os.path.exists(mdx_path):
                errors.append(f"docs/snippets/{suffix}: referenced by DOC_SNIPPETS but the file is missing")
                continue
            with open(mdx_path, encoding="utf-8") as f:
                expected = normalize_snippet_body(f.read())
            if cpp[suffix] != expected:
                errors.append(
                    f"docs/snippets/{suffix}: content embedded in src/Client/TerminalMarkdownRenderer.cpp "
                    "is out of sync with the snippet source (regenerate the DOC_SNIPPETS entry)"
                )
            if html[suffix] != expected:
                errors.append(
                    f"docs/snippets/{suffix}: content embedded in programs/server/docs.html "
                    "is out of sync with the snippet source (regenerate the DOC_SNIPPETS entry)"
                )

    for e in errors:
        print(e)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
