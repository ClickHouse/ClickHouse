#!/usr/bin/env python3
"""
Script to automatically extract quick-start metadata and regenerate the
quickstarts-data.jsx modules

This script scans the docs/get-started/quickstarts/ directory for .mdx files,
extracts metadata from their frontmatter, and writes it to
snippets/components/QuickStartsGrid/quickstarts-data.jsx, which home.mdx
imports. It does the same for every locale tree (docs/<locale>/get-started/
quickstarts/ -> snippets/<locale>/components/QuickStartsGrid/
quickstarts-data.jsx), so the cards pick up the translated titles and
descriptions from the locale pages' frontmatter. The data lives in a snippets
module rather than inline in home.mdx because the translation pipeline
translates snippet modules but not `export const` literals inside pages
(same layout as KBExplorer's kb-data.jsx).

Every quickstart page is omitted from its locale's sidebar, so the script also
normalizes the non-translatable `searchable: true` frontmatter flag across the
English and localized page trees.

Usage:
    python scripts/update_quickstarts.py
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

LOCALES = ('ar', 'es', 'fr', 'ja', 'ko', 'pt-BR', 'ru', 'zh')

CLOUD_SETUP_CARD = {
    'id': 'create-your-first-service-on-cloud',
    'title': 'ClickHouse Cloud quick start',
    'description': (
        'Quick start guide for ClickHouse Cloud'
    ),
    'useCases': ['All'],
    'products': ['Cloud'],
}

CLOUD_SETUP_CARD_TRANSLATIONS = {
    'ar': {
        'title': 'البدء السريع مع ClickHouse Cloud',
        'description': 'دليل البدء السريع لـ ClickHouse Cloud',
    },
    'es': {
        'title': 'Inicio rápido de ClickHouse Cloud',
        'description': 'Guía de inicio rápido para ClickHouse Cloud',
    },
    'fr': {
        'title': 'Démarrage rapide de ClickHouse Cloud',
        'description': 'Guide de démarrage rapide pour ClickHouse Cloud',
    },
    'ja': {
        'title': 'ClickHouse Cloud クイックスタート',
        'description': 'ClickHouse Cloud のクイックスタートガイド',
    },
    'ko': {
        'title': 'ClickHouse Cloud 빠른 시작',
        'description': 'ClickHouse Cloud 빠른 시작 가이드',
    },
    'pt-BR': {
        'title': 'Início rápido do ClickHouse Cloud',
        'description': 'Guia de início rápido do ClickHouse Cloud',
    },
    'ru': {
        'title': 'Быстрый старт с ClickHouse Cloud',
        'description': 'Руководство по быстрому старту с ClickHouse Cloud',
    },
    'zh': {
        'title': 'ClickHouse Cloud 快速入门',
        'description': 'ClickHouse Cloud 快速入门指南',
    },
}


def add_cloud_setup_card(quickstarts: List[Dict[str, Any]],
                         locale: Optional[str] = None) -> None:
    """Add the Cloud setup card without keeping a duplicate quickstart page."""
    prefix = f'/{locale}' if locale else ''
    href = f'{prefix}/get-started/setup/cloud'

    for quickstart in quickstarts:
        if quickstart['id'] == CLOUD_SETUP_CARD['id']:
            raise ValueError(
                f"Remove the legacy {CLOUD_SETUP_CARD['id']}.mdx page; its "
                "explorer card is generated from update_quickstarts.py"
            )

    translated = CLOUD_SETUP_CARD_TRANSLATIONS.get(locale, {})
    card = {**CLOUD_SETUP_CARD, **translated}
    quickstarts.append({**card, 'href': href})
    quickstarts.sort(key=lambda quickstart: quickstart['id'])


def unquote_scalar(value: str) -> str:
    """Unquote a single YAML scalar, honoring the escaping the docs frontmatter
    actually uses.

    Inside a single-quoted scalar a doubled '' is the one literal apostrophe
    YAML allows ('l''immobilier' -> l'immobilier). For double-quoted scalars,
    support the quote and backslash escapes used by the existing frontmatter and
    reject every other escape rather than diverging from Mintlify's YAML parser.
    A real YAML parser would be ideal, but PyYAML is not available in the docs CI
    image, so handle this deliberately limited subset by hand.
    """
    value = value.strip()
    if value.startswith(("'", '"')):
        if len(value) < 2 or value[-1] != value[0]:
            raise ValueError(
                "multiline or unterminated quoted YAML scalars are not "
                "supported; keep the value on one physical line"
            )
        if value[0] == "'":
            return value[1:-1].replace("''", "'")
        inner = value[1:-1]
        decoded = []
        index = 0
        while index < len(inner):
            if inner[index] != '\\':
                decoded.append(inner[index])
                index += 1
                continue
            if index + 1 >= len(inner) or inner[index + 1] not in ('"', '\\'):
                escape = inner[index:index + 2]
                raise ValueError(
                    f"unsupported YAML escape {escape!r}; only escaped quotes "
                    "and backslashes are supported"
                )
            decoded.append(inner[index + 1])
            index += 2
        return ''.join(decoded)
    return value


def parse_frontmatter(content: str) -> Dict[str, Any]:
    """
    Parse the supported single-line subset of YAML frontmatter.

    Args:
        content: The full content of the MDX file

    Returns:
        Dictionary containing the frontmatter fields
    """
    # Match frontmatter between --- delimiters
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if not match:
        return {}

    frontmatter_text = match.group(1)
    frontmatter = {}

    # Parse only the single-line scalar and inline-array forms this generator
    # understands. Reject other valid YAML forms instead of silently reducing
    # them to a different value than Mintlify's YAML parser renders.
    for line_number, raw_line in enumerate(frontmatter_text.split('\n'), 1):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue

        if raw_line[0].isspace():
            raise ValueError(
                f"unsupported indented YAML value on frontmatter line "
                f"{line_number}; use a single-line scalar or inline array"
            )

        if ':' not in line:
            raise ValueError(
                f"unsupported YAML syntax on frontmatter line {line_number}: "
                f"{line!r}"
            )

        # Handle key: value pairs
        key, value = line.split(':', 1)
        key = key.strip()
        value = value.strip()
        if not key or not value:
            raise ValueError(
                f"empty or nested YAML value for {key!r} on frontmatter "
                f"line {line_number}; use a single-line scalar or inline array"
            )
        if key in frontmatter:
            raise ValueError(f"duplicate frontmatter field {key!r}")
        if value.startswith(('|', '>')):
            raise ValueError(
                f"block YAML scalar for {key!r} is not supported; keep the "
                "value on one physical line"
            )

        # Handle arrays like [item1, item2]. Brackets are unquoted, so this
        # is checked before unquoting the scalar form below.
        if value.startswith('[') and value.endswith(']'):
            array_content = value[1:-1]
            items = [unquote_scalar(item)
                    for item in array_content.split(',')]
            frontmatter[key] = [item for item in items if item]
        else:
            frontmatter[key] = unquote_scalar(value)

    return frontmatter

def slugify_tag(value: str) -> str:
    """
    Normalize a frontmatter tag value to the stable slug the QuickStartsGrid
    filter options match against ('AI/ML' -> 'ai-ml'). Slugs survive the
    translation pipeline (which localizes display labels but leaves
    identifier-like strings alone), so filtering keeps working on locale pages.
    """
    slug = re.sub(r'[^a-z0-9]+', '-', value.lower()).strip('-')
    # 'OSS' means self-managed; the grid exposes only the latter as an option.
    return {'oss': 'self-managed'}.get(slug, slug)

def extract_quickstart_data(file_path: Path, base_dir: Path) -> Dict[str, Any]:
    """
    Extract quick-start metadata from an MDX file.

    Args:
        file_path: Path to the MDX file
        base_dir: Base directory for generating relative paths

    Returns:
        Dictionary containing the quick-start data
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    frontmatter = parse_frontmatter(content)

    # Generate ID from filename (remove .mdx extension)
    file_id = file_path.stem

    # Generate href from the path relative to the project root
    rel_path = file_path.relative_to(base_dir)
    href = '/' + str(rel_path.parent / file_path.stem).replace('\\', '/')

    # Extract fields with defaults
    quickstart = {
        'id': file_id,
        'title': frontmatter.get('title', file_id.replace('-', ' ').title()),
        'description': frontmatter.get('description', ''),
        'href': href,
        'useCases': frontmatter.get('useCases', []),
        'products': frontmatter.get('products', []),
    }

    # Add icon if present
    if 'icon' in frontmatter:
        quickstart['icon'] = frontmatter['icon']

    return quickstart

def find_quickstart_files(quickstarts_dir: Path) -> List[Path]:
    """
    Find all quick-start MD/MDX files (excluding home.mdx).

    Args:
        quickstarts_dir: Path to the quickstarts directory

    Returns:
        List of Path objects for quick-start files
    """
    files = []

    # Find both .md and .mdx files
    for pattern in ['**/*.mdx', '**/*.md']:
        for file_path in quickstarts_dir.glob(pattern):
            # Skip home.mdx, the README, and anything underscore-prefixed
            # (templates like _TEMPLATE.mdx and helper directories)
            if file_path.name in ('home.mdx', 'README.md'):
                continue
            if any(part.startswith('_') for part in file_path.relative_to(quickstarts_dir).parts):
                continue
            files.append(file_path)

    # Remove duplicates and sort
    files = sorted(set(files))

    return files

def generate_badges(use_cases: List[str], products: List[str]) -> str:
    """
    Generate Badge components for use cases and products.

    Args:
        use_cases: List of use case tags
        products: List of product tags

    Returns:
        Badge components as a string
    """
    # First line: muted text back-link to the quickstarts home. Its markup and
    # styling live in a single shared partial,
    # get-started/quickstarts/_back-to-quickstarts.mdx, which every quickstart
    # page imports as <BackToQuickstarts /> (Mintlify resolves the import to the
    # localized copy per locale). Keeping the link in one partial avoids
    # duplicating the styling across every page and locale; the page's import of
    # the partial is what makes this tag resolve.
    first_line = '<BackToQuickstarts />'

    # Second line: all other badges
    second_line_badges = []

    # Expand 'All' to all use cases for badge display
    all_use_cases = ['Real-Time Analytics', 'Data Warehousing', 'Observability', 'AI/ML']
    display_use_cases = all_use_cases if 'All' in use_cases else use_cases

    # Add use case badges (blue)
    for use_case in display_use_cases:
        # Capitalize properly
        display_text = use_case.title() if use_case.lower() != 'ai/ml' else 'AI/ML'
        second_line_badges.append(f'<Badge size="lg" color="blue">{display_text}</Badge>')

    # Add product badges (orange). Brand/acronym names must not be title-cased
    # ('OSS'.title() == 'Oss').
    product_labels = {'oss': 'OSS', 'chdb': 'chDB', 'clickpipes': 'ClickPipes', 'clickstack': 'ClickStack'}
    for product in products:
        display_text = product_labels.get(product.lower(), product.title())
        second_line_badges.append(f'<Badge size="lg" color="orange">{display_text}</Badge>')

    # Combine with line break and add margin
    second_line = '\n'.join(second_line_badges)

    return f'{first_line}\n<div className="mt-2 flex flex-wrap gap-2">\n{second_line}\n</div>'

def update_quickstart_page(file_path: Path, use_cases: List[str], products: List[str],
                           update_badges: bool) -> Optional[str]:
    """
    Normalize required frontmatter and optionally refresh the badge section.

    Returns the updated page content, or None when the badge block is already
    up to date and `searchable: true` is already present. Nothing is written
    here: the caller stages the content and flushes it only after the whole
    scan has succeeded, so a failure on a later page cannot leave the tree
    half-regenerated.

    Args:
        file_path: Path to the quick-start MDX file
        use_cases: List of use case tags
        products: List of product tags
        update_badges: Whether to refresh the English badge block
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        original = f.read()

    frontmatter_match = re.match(r'^---\s*\n(.*?)\n---\s*\n', original, re.DOTALL)
    if not frontmatter_match:
        raise ValueError("no frontmatter block; cannot set `searchable: true`")

    frontmatter = frontmatter_match.group(1)
    searchable_lines = re.findall(r'^searchable:\s*.*$', frontmatter, re.MULTILINE)
    if len(searchable_lines) > 1:
        raise ValueError("multiple `searchable` frontmatter fields")

    if searchable_lines:
        frontmatter = re.sub(
            r'^searchable:\s*.*$', 'searchable: true', frontmatter,
            count=1, flags=re.MULTILINE)
    else:
        frontmatter, replacements = re.subn(
            r'(^sidebarTitle:.*$)', r'\1\nsearchable: true', frontmatter,
            count=1, flags=re.MULTILINE)
        if not replacements:
            frontmatter = f'searchable: true\n{frontmatter}'

    content = (
        original[:frontmatter_match.start(1)]
        + frontmatter
        + original[frontmatter_match.end(1):]
    )

    if not update_badges:
        return content if content != original else None

    # Generate badges
    badges = generate_badges(use_cases, products)

    # Pattern to match the autogenerated section
    pattern = r'\{/\* AUTOGENERATED_START \*/\}.*?\{/\* AUTOGENERATED_END \*/\}'

    # Without the markers the badge block cannot be updated and the page would
    # silently keep stale badges, so this is an error, not a warning.
    if not re.search(pattern, content, re.DOTALL):
        raise ValueError(
            "no {/* AUTOGENERATED_START */}/{/* AUTOGENERATED_END */} markers "
            "found; cannot update the badge block (copy the marker pair from "
            "_TEMPLATE.mdx)"
        )

    # Replace the content between markers
    replacement = f'{{/* AUTOGENERATED_START */}}\n{badges}\n{{/* AUTOGENERATED_END */}}'
    updated = re.sub(pattern, replacement, content, flags=re.DOTALL)
    return updated if updated != original else None

def render_data_module(quickstarts: List[Dict[str, Any]]) -> str:
    """Render the quickstarts-data.jsx module body.

    Tag values are emitted as slugs (the form the grid's filter options match
    against); the in-page badges keep the raw frontmatter values.

    The data is emitted as JSON, which is valid JS object-literal syntax, so
    titles/descriptions with apostrophes, backticks, smart quotes, or non-Latin
    text need no special escaping. ensure_ascii=False keeps non-ASCII readable.
    """
    slugged = [
        {**qs,
         'useCases': [slugify_tag(v) for v in qs.get('useCases', [])],
         'products': [slugify_tag(v) for v in qs.get('products', [])]}
        for qs in quickstarts
    ]
    body = json.dumps(slugged, indent=2, ensure_ascii=False)
    return (
        "// AUTO-GENERATED by _site/scripts/update_quickstarts.py — do not edit by hand.\n"
        "// Re-run the script to refresh the quick-start card data.\n"
        f"export const quickStartsData = {body};\n"
    )

def build_quickstarts(quickstarts_dir: Path, project_root: Path,
                      update_badges: bool,
                      staged: Dict[Path, str]) -> Tuple[List[Dict[str, Any]], int]:
    """Extract quick-start data from every page in quickstarts_dir.

    Returns the extracted data and the number of pages that failed. Failed
    pages are reported but do not stop the scan, so one run surfaces every
    problem; the caller must treat a non-zero failure count as an error
    rather than shipping data that silently omits the failed pages.

    Refreshed badge blocks are added to `staged` (path -> new content), not
    written: the caller flushes them only after every scan has succeeded.
    """
    files = find_quickstart_files(quickstarts_dir)
    quickstarts = []
    failures = 0
    seen_ids = {}
    for file_path in files:
        try:
            data = extract_quickstart_data(file_path, project_root)

            # Ids are the filename stem, so the recursive scan lets two pages in
            # different subdirectories collide (foo/setup.mdx and bar/setup.mdx
            # both -> 'setup'). A collision would silently drop one page from the
            # id->page map used for cross-locale tag inheritance, and emit
            # duplicate ids for React keys and featured lookup, so fail closed
            # (the non-zero failure count below blocks every write).
            prior = seen_ids.get(data['id'])
            if prior is not None:
                raise ValueError(
                    f"duplicate quickstart id {data['id']!r}; also produced by "
                    f"{prior.relative_to(quickstarts_dir)}. Ids are the filename "
                    "stem and must be unique across the tree — rename one file."
                )
            seen_ids[data['id']] = file_path

            updated = update_quickstart_page(
                file_path, data['useCases'], data['products'], update_badges)
            if updated is not None:
                staged[file_path] = updated

            quickstarts.append(data)
            print(f"  ✓ {file_path.name}: {data['title']}")
        except Exception as e:
            print(f"  ✗ {file_path.name}: Error - {e}")
            failures += 1
    return quickstarts, failures

def main():
    """Main function to run the script."""
    # Get the project root directory (this script lives in _site/scripts/)
    project_root = Path(__file__).resolve().parents[2]

    quickstarts_dir = project_root / 'get-started' / 'quickstarts'
    if not quickstarts_dir.exists():
        print(f"Error: Quick-starts directory not found: {quickstarts_dir}")
        return 1

    # Every write (badge blocks and data modules) is staged here and flushed
    # only after all trees have scanned cleanly, so a failure anywhere leaves
    # the tree untouched instead of half-regenerated.
    staged = {}

    # English tree: extract data and refresh the in-page badge blocks.
    print(f"Scanning for quick-start files in {quickstarts_dir}...")
    quickstarts, failures = build_quickstarts(quickstarts_dir, project_root,
                                              update_badges=True, staged=staged)
    if not quickstarts:
        print("No valid quick-start data extracted")
        return 1
    add_cloud_setup_card(quickstarts)

    output_path = (project_root / 'snippets' / 'components' / 'QuickStartsGrid'
                   / 'quickstarts-data.jsx')
    staged[output_path] = render_data_module(quickstarts)
    print(f"✓ Staged {len(quickstarts)} quick-start(s) for {output_path}")

    # Locale trees: same extraction against the translated pages, so titles and
    # descriptions come out localized and hrefs come out locale-prefixed
    # (extract_quickstart_data derives the href from the path relative to the
    # project root). Badges are left to the translation pipeline, while the
    # non-translatable searchable flag is normalized here.
    for locale in LOCALES:
        locale_dir = project_root / locale / 'get-started' / 'quickstarts'
        if not locale_dir.exists():
            print(f"  - {locale}: no quickstarts directory, skipped")
            continue
        print(f"\nScanning {locale_dir}...")
        locale_quickstarts, locale_failures = build_quickstarts(
            locale_dir, project_root, update_badges=False, staged=staged)
        failures += locale_failures
        if not locale_quickstarts:
            print(f"  - {locale}: no valid quick-start data, skipped")
            continue
        add_cloud_setup_card(locale_quickstarts, locale)
        # Keep useCases/products canonical English: the grid filters match data
        # values against its option lists by string equality, and the
        # translation pipeline translates frontmatter tag values inconsistently.
        english_by_id = {qs['id']: qs for qs in quickstarts}
        for entry in locale_quickstarts:
            english = english_by_id.get(entry['id'])
            if english is None:
                # A localized page whose id has no English counterpart cannot
                # inherit canonical tags, so it would keep its own translated
                # useCases/products — which slugify_tag collapses to "" for
                # non-Latin text, shipping a card with an unfilterable tag.
                # Fail closed rather than emit broken explorer data (the
                # non-zero failure count below blocks all writes).
                print(f"  ✗ {locale}/{entry['id']}: no English quickstart with "
                      "this id; the localized tags cannot be made canonical. "
                      "Rename this page to match its English counterpart, or "
                      "add the missing English page.")
                failures += 1
                continue
            entry['useCases'] = english['useCases']
            entry['products'] = english['products']
        locale_output = (project_root / 'snippets' / locale / 'components'
                         / 'QuickStartsGrid' / 'quickstarts-data.jsx')
        staged[locale_output] = render_data_module(locale_quickstarts)
        print(f"✓ Staged {len(locale_quickstarts)} quick-start(s) for {locale_output}")

    if failures:
        print(f"\n✗ {failures} page(s) failed; nothing was written — "
              "fix the errors above and re-run.")
        return 1

    for path, content in staged.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding='utf-8')
    print(f"\n✓ Wrote {len(staged)} file(s)")
    return 0

if __name__ == '__main__':
    exit(main())
