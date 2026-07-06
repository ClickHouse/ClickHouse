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

Usage:
    python scripts/update_quickstarts.py
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

def parse_frontmatter(content: str) -> Dict[str, Any]:
    """
    Parse YAML frontmatter from MDX file content.

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

    # Parse simple YAML key-value pairs and arrays
    for line in frontmatter_text.split('\n'):
        line = line.strip()
        if not line or line.startswith('#'):
            continue

        # Handle key: value pairs
        if ':' in line:
            key, value = line.split(':', 1)
            key = key.strip()
            value = value.strip()

            # Remove quotes from strings
            value = value.strip('"').strip("'")

            # Handle arrays like [item1, item2]
            if value.startswith('[') and value.endswith(']'):
                # Parse array
                array_content = value[1:-1]
                items = [item.strip().strip('"').strip("'")
                        for item in array_content.split(',')]
                frontmatter[key] = [item for item in items if item]
            else:
                frontmatter[key] = value

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
    # First line: muted text back-link (arrow icon + label), styled to match the
    # homepage links. The href is root-relative and the onClick prepends the
    # /docs base path on the production deploy (a bare relative href like
    # "home" breaks under the subpath mount).
    first_line = (
        '<a href="/get-started/quickstarts/home" '
        "onClick={(e) => { e.preventDefault(); window.location.href = (window.location.pathname.startsWith('/docs') ? '/docs' : '') + '/get-started/quickstarts/home'; }} "
        'className="inline-flex items-center gap-1.5 text-sm text-gray-500 dark:text-zinc-500 hover:text-gray-900 dark:hover:text-[#fdff75] transition-colors font-normal no-underline">'
        '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0"><path d="M19 12H5" /><path d="M12 19l-7-7 7-7" /></svg>'
        'All quickstarts</a>'
    )

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

def update_quickstart_badges(file_path: Path, use_cases: List[str], products: List[str]) -> Optional[str]:
    """
    Refresh the autogenerated badges section of a quick-start MDX file.

    Returns the updated page content, or None when the badge block is already
    up to date. Nothing is written here: the caller stages the content and
    flushes it only after the whole scan has succeeded, so a failure on a
    later page cannot leave the tree half-regenerated.

    Args:
        file_path: Path to the quick-start MDX file
        use_cases: List of use case tags
        products: List of product tags
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

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
    return updated if updated != content else None

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
    for file_path in files:
        try:
            data = extract_quickstart_data(file_path, project_root)

            if update_badges:
                updated = update_quickstart_badges(file_path, data['useCases'], data['products'])
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

    output_path = (project_root / 'snippets' / 'components' / 'QuickStartsGrid'
                   / 'quickstarts-data.jsx')
    staged[output_path] = render_data_module(quickstarts)
    print(f"✓ Staged {len(quickstarts)} quick-start(s) for {output_path}")

    # Locale trees: same extraction against the translated pages, so titles and
    # descriptions come out localized and hrefs come out locale-prefixed
    # (extract_quickstart_data derives the href from the path relative to the
    # project root). Badges are left to the translation pipeline.
    locales = ['ar', 'es', 'fr', 'ja', 'ko', 'pt-BR', 'ru', 'zh']
    for locale in locales:
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
        # Keep useCases/products canonical English: the grid filters match data
        # values against its option lists by string equality, and the
        # translation pipeline translates frontmatter tag values inconsistently.
        english_by_id = {qs['id']: qs for qs in quickstarts}
        for entry in locale_quickstarts:
            english = english_by_id.get(entry['id'])
            if english:
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
