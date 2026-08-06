#!/usr/bin/env python3
"""Generate the ClickHouse Cloud unsupported-features documentation list.

The list is derived from English documentation pages that render a
`CloudNotSupportedBadge`, including badges rendered through imported MDX
snippets. The destination uses dedicated markers so this generator owns only
the list and not the surrounding hand-written compatibility guide.
"""

import argparse
import dataclasses
import re
from pathlib import Path


TARGET = Path("products/cloud/guides/cloud-compatibility.mdx")
START_MARKER = "{/* CLOUD_NOT_SUPPORTED_FEATURES_START */}"
END_MARKER = "{/* CLOUD_NOT_SUPPORTED_FEATURES_END */}"
LOCALES = {"ar", "es", "fr", "ja", "ko", "pt-BR", "ru", "zh"}

BADGE_RE = re.compile(r"<CloudNotSupportedBadge\b[^>]*?/?>")
HEADING_RE = re.compile(
    r"^#{1,6}\s+(.+?)(?:\s+\{#([^}]+)\})\s*$", re.MULTILINE
)
TITLE_RE = re.compile(r"^title:\s*(['\"]?)(.*?)\1\s*$", re.MULTILINE)
SNIPPET_IMPORT_RE = re.compile(
    r"^import\s+([A-Za-z_$][\w$]*)\s+from\s+"
    r"['\"](/snippets/[^'\"]+\.mdx)['\"];?\s*$",
    re.MULTILINE,
)
MARKDOWN_LINK_RE = re.compile(r"\[([^]]+)]\([^)]+\)")
HTML_TAG_RE = re.compile(r"<[^>]+>")


@dataclasses.dataclass(frozen=True, order=True)
class Feature:
    label: str
    url: str


def _page_url(relative_path: Path) -> str:
    path = relative_path.with_suffix("").as_posix()
    if path.endswith("/index"):
        path = path[: -len("/index")]
    return f"/{path}"


def _page_title(text: str, relative_path: Path) -> str:
    match = TITLE_RE.search(text)
    if match:
        return match.group(2).strip()
    return relative_path.stem.replace("-", " ").replace("_", " ").title()


def _clean_label(label: str) -> str:
    label = MARKDOWN_LINK_RE.sub(r"\1", label)
    label = HTML_TAG_RE.sub("", label)
    label = label.replace("**", "").replace("__", "")
    return label.replace("[", r"\[").replace("]", r"\]").strip()


def _expand_mdx_snippets(
    text: str, docs_root: Path, stack: tuple[Path, ...] = ()
) -> str:
    imports = {
        component: docs_root / source.removeprefix("/")
        for component, source in SNIPPET_IMPORT_RE.findall(text)
    }
    for component, snippet_path in imports.items():
        if snippet_path in stack:
            chain = " -> ".join(str(path) for path in (*stack, snippet_path))
            raise ValueError(f"recursive MDX snippet import: {chain}")
        if not snippet_path.is_file():
            raise FileNotFoundError(f"MDX snippet does not exist: {snippet_path}")
        snippet = _expand_mdx_snippets(
            snippet_path.read_text(encoding="utf-8"),
            docs_root,
            (*stack, snippet_path),
        )
        invocation = re.compile(rf"<{re.escape(component)}\b[^>]*?\s*/>")
        text = invocation.sub(lambda _match: snippet, text)
    return text


def _features_in_page(path: Path, docs_root: Path) -> list[Feature]:
    relative_path = path.relative_to(docs_root)
    source = path.read_text(encoding="utf-8")
    expanded = _expand_mdx_snippets(source, docs_root)
    page_title = _clean_label(_page_title(source, relative_path))
    page_url = _page_url(relative_path)
    headings = list(HEADING_RE.finditer(expanded))
    features = []

    for badge in BADGE_RE.finditer(expanded):
        preceding = [heading for heading in headings if heading.start() < badge.start()]
        if preceding:
            heading = preceding[-1]
            heading_title = _clean_label(heading.group(1))
            label = (
                heading_title
                if heading_title.casefold() == page_title.casefold()
                else f"{page_title}: {heading_title}"
            )
            anchor = heading.group(2)
            url = f"{page_url}#{anchor}" if anchor else page_url
        else:
            label = page_title
            url = page_url
        features.append(Feature(label=label, url=url))

    return features


def collect_features(docs_root: Path) -> list[Feature]:
    features = set()
    for path in docs_root.rglob("*.mdx"):
        relative_path = path.relative_to(docs_root)
        if relative_path.parts[0] in LOCALES or relative_path.parts[0] == "snippets":
            continue
        if path.name.startswith("_") or relative_path == TARGET:
            continue
        features.update(_features_in_page(path, docs_root))
    return sorted(features, key=lambda feature: (feature.label.casefold(), feature.url))


def render_feature_list(features: list[Feature]) -> str:
    if not features:
        raise ValueError("no Cloud-not-supported features found")
    return "\n".join(f"- [{feature.label}]({feature.url})" for feature in features)


def updated_target_content(docs_root: Path) -> tuple[str, int]:
    target = docs_root / TARGET
    content = target.read_text(encoding="utf-8")
    if content.count(START_MARKER) != 1 or content.count(END_MARKER) != 1:
        raise ValueError(
            f"{target} must contain exactly one pair of unsupported-feature markers"
        )
    start, remainder = content.split(START_MARKER, 1)
    _old_list, end = remainder.split(END_MARKER, 1)
    features = collect_features(docs_root)
    generated = render_feature_list(features)
    updated = f"{start}{START_MARKER}\n{generated}\n{END_MARKER}{end}"
    return updated, len(features)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--docs-dir", type=Path, default=Path("docs"))
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args()

    target = args.docs_dir / TARGET
    updated, count = updated_target_content(args.docs_dir)
    current = target.read_text(encoding="utf-8")
    if updated == current:
        print(f"Cloud unsupported-feature list is current ({count} entries)")
        return 0
    if args.write:
        target.write_text(updated, encoding="utf-8")
        print(f"Updated Cloud unsupported-feature list ({count} entries)")
        return 0
    print(
        "Cloud unsupported-feature list is out of date; rerun with --write",
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
