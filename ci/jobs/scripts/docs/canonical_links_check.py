#!/usr/bin/env python3
"""Reject repository-authored links that point at redirect sources.

Redirects preserve old inbound URLs, but links authored inside the English docs
must point directly at canonical pages. GT localizes an absolute internal URL by
inserting the target locale. If the English URL is only valid because of a
redirect, the localized URL usually has neither a page nor a matching redirect
and becomes a 404.

The check covers Markdown/MDX pages included by the regular lychee pass plus the
shared JavaScript/JSX snippets translated by GT. Code examples and MDX comments
are masked before scanning. It also covers documentation embedded in C++ source,
where Markdown links to ``clickhouse.com/docs`` must be root-relative; bare URLs
used by runtime help remain absolute but must use canonical routes. ``--fix``
rewrites redirect sources to their final internal destinations; redirects
themselves remain unchanged for external and historical traffic.
"""

import argparse
import json
import os
import re
import sys

try:
    from .lychee_check import dump_inputs
except ImportError:
    # The CI command executes this file directly from the docs root.
    from lychee_check import dump_inputs


LOCALE_DIRS = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}

# Markdown links, JSX attributes, navigation data such as
# ``{ label: "Settings", href: "/operations/settings" }``. GT rewrites the
# first two forms, while ClickHouse's locale component fixer covers the latter;
# all of them must start from a canonical English route.
MARKDOWN_LINK = re.compile(r"\]\(\s*(?P<url>/[^\s)]+)")
PUBLIC_MARKDOWN_LINK = re.compile(
    r"\]\(\s*(?P<url>https://clickhouse\.com/docs(?:/en)?/[^\s)]+)"
)
NAVIGATION_URL = re.compile(
    r"(?<![A-Za-z0-9_])(?:(?:['\"])?(?:href|to)(?:['\"])?)[ \t]*[:=][ \t]*"
    r"\{?[ \t]*(?P<quote>['\"`])(?P<url>/[^'\"`\s$]+)(?P=quote)"
)
# Template literals whose static prefix is an internal route. These are used by
# shared navigation snippets such as QuickStartsGrid and KBExplorer. Capture the
# prefix through its separator so a rewrite keeps ``/${...}`` intact.
TEMPLATE_URL = re.compile(
    r"`(?P<url>/[A-Za-z0-9][A-Za-z0-9/_.#-]*)\$\{"
)
BARE_DOCS_URL = re.compile(
    r"(?P<url>https://clickhouse\.com/docs(?:/en)?/"
    r"[^\s'\"`)<>\\]*[A-Za-z0-9_/#?=&%+~-])"
)

MDX_COMMENT = re.compile(r"\{/\*.*?\*/\}", re.DOTALL)
FENCED_CODE = re.compile(
    r"^[ \t]{0,3}```.*?^[ \t]{0,3}```[^\n]*$", re.DOTALL | re.MULTILINE
)
INLINE_CODE = re.compile(r"`[^`\n]+`")


def mask_match(match):
    """Hide non-rendered content without changing offsets or line numbers."""
    return "".join("\n" if c == "\n" else " " for c in match.group(0))


def mask_blocks_and_comments(text):
    text = MDX_COMMENT.sub(mask_match, text)
    return FENCED_CODE.sub(mask_match, text)


def mask_non_links(text):
    return INLINE_CODE.sub(mask_match, mask_blocks_and_comments(text))


def split_path_suffix(url):
    indexes = [i for marker in ("?", "#") if (i := url.find(marker)) >= 0]
    if not indexes:
        return url, ""
    index = min(indexes)
    return url[:index], url[index:]


def load_redirects(docs_root):
    path = os.path.join(docs_root, "_site", "redirects.json")
    with open(path, encoding="utf-8") as f:
        entries = json.load(f)

    redirects = {}
    for entry in entries:
        source = (entry.get("source") or "").strip()
        destination = (entry.get("destination") or "").strip()
        source_path, _ = split_path_suffix(source)
        first_segment = source_path.lstrip("/").split("/", 1)[0]
        if (
            not source_path.startswith("/")
            or not destination.startswith("/")
            or ":" in source_path
            or ":" in destination
            or first_segment in LOCALE_DIRS
        ):
            continue
        redirects[source_path.rstrip("/") or "/"] = destination
    return redirects


def canonicalize_url(url, redirects, make_relative=False):
    """Return the final redirect destination, preserving caller fragments."""
    public_prefix = ""
    internal_url = url
    for prefix in ("https://clickhouse.com/docs/en", "https://clickhouse.com/docs"):
        if url.startswith(f"{prefix}/"):
            public_prefix = "https://clickhouse.com/docs"
            internal_url = url[len(prefix) :]
            break

    original_path, original_suffix = split_path_suffix(internal_url)
    lookup = original_path.rstrip("/") or "/"
    for extension in (".mdx", ".md"):
        if lookup.endswith(extension):
            lookup = lookup[: -len(extension)]
            break
    if lookup in redirects:
        destination = redirects[lookup]
        seen = {lookup}
        while True:
            destination_path, destination_suffix = split_path_suffix(destination)
            next_lookup = destination_path.rstrip("/") or "/"
            if next_lookup not in redirects:
                break
            if next_lookup in seen:
                return None
            seen.add(next_lookup)
            next_destination = redirects[next_lookup]
            # A fragment/query deliberately attached by a redirect belongs to that
            # redirect. Otherwise carry the suffix forward to the final page.
            if destination_suffix and not split_path_suffix(next_destination)[1]:
                next_destination += destination_suffix
            destination = next_destination
    elif public_prefix and make_relative:
        destination = lookup
    else:
        return None

    if original_suffix and not split_path_suffix(destination)[1]:
        destination += original_suffix
    return destination if make_relative else public_prefix + destination


def find_aliases_in_text(
    text, redirects, include_public_urls=False, include_templates=True
):
    masked = mask_non_links(text)
    template_masked = mask_blocks_and_comments(text)
    aliases = []
    occupied = set()
    patterns = [
        (MARKDOWN_LINK, False, False, masked),
        (NAVIGATION_URL, False, False, masked),
    ]
    if include_templates:
        patterns.append((TEMPLATE_URL, False, True, template_masked))
    if include_public_urls:
        patterns.extend(
            (
                (PUBLIC_MARKDOWN_LINK, True, False, masked),
                (BARE_DOCS_URL, False, False, masked),
            )
        )
    for pattern, make_relative, preserve_separator, content in patterns:
        for match in pattern.finditer(content):
            start, end = match.span("url")
            if (start, end) in occupied:
                continue
            occupied.add((start, end))
            original = text[start:end]
            canonical = canonicalize_url(
                original, redirects, make_relative=make_relative
            )
            if (
                preserve_separator
                and original.endswith("/")
                and canonical
                and not canonical.endswith("/")
            ):
                canonical += "/"
            if canonical and canonical != original:
                aliases.append((start, end, original, canonical))
    return sorted(aliases)


def source_files(docs_root, repo_root):
    files = {
        os.path.join(docs_root, rel)
        for rel in dump_inputs(docs_root)
        if rel.endswith((".md", ".mdx"))
    }

    # lychee does not inspect JS/JSX, but GT translates shared snippets and its
    # static URL handling must receive canonical paths there as well.
    snippets = os.path.join(docs_root, "snippets")
    for root, dirs, names in os.walk(snippets):
        if root == snippets:
            dirs[:] = [d for d in dirs if d not in LOCALE_DIRS]
        for name in names:
            if name.endswith((".js", ".jsx")):
                files.add(os.path.join(root, name))

    # A growing portion of the reference documentation is embedded in the C++
    # registration code and exported verbatim. These links are English source
    # links too, even though their containing files live outside ``docs``.
    source_root = os.path.join(repo_root, "src")
    for root, _dirs, names in os.walk(source_root):
        for name in names:
            if name.endswith((".cpp", ".h")):
                files.add(os.path.join(root, name))
    return sorted(files)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("docs_root", nargs="?", default=".")
    parser.add_argument(
        "--fix", action="store_true", help="Rewrite aliases to canonical routes."
    )
    args = parser.parse_args(argv)

    docs_root = os.path.abspath(args.docs_root)
    repo_root = os.path.dirname(docs_root)
    redirects = load_redirects(docs_root)
    violations = []
    fixed = 0

    for path in source_files(docs_root, repo_root):
        rel = os.path.relpath(path, repo_root)
        with open(path, encoding="utf-8", errors="replace") as f:
            text = f.read()
        aliases = find_aliases_in_text(
            text,
            redirects,
            include_public_urls=rel.startswith("src/"),
            include_templates=rel.endswith((".js", ".jsx")),
        )
        if not aliases:
            continue

        for start, _end, original, canonical in aliases:
            line = text.count("\n", 0, start) + 1
            column = start - text.rfind("\n", 0, start)
            violations.append((rel, line, column, original, canonical))

        if args.fix:
            for start, end, _original, canonical in reversed(aliases):
                text = text[:start] + canonical + text[end:]
                fixed += 1
            with open(path, "w", encoding="utf-8") as f:
                f.write(text)

    if args.fix:
        print(f"fixed: {fixed}")
    remaining = 0 if args.fix else len(violations)
    print(f"noncanonical internal links: {remaining}")
    for rel, line, column, original, canonical in violations[:80]:
        print(f"  {rel}:{line}:{column}  {original} -> {canonical}")
    if len(violations) > 80:
        print(f"  ... and {len(violations) - 80} more")
    return 0 if remaining == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
