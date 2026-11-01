#!/usr/bin/env python3
"""
Generate the knowledge-base search index consumed by the KB explorer.

This script scans resources/support-center/knowledge-base/ for article .mdx
files, extracts metadata from their frontmatter, and writes the index to a
generated JS module:

    snippets/components/KBExplorer/kb-data.jsx  ->  export const kbIndex = {...}

resources/support-center/home.mdx imports that module and passes it to
<KBExplorer index={kbIndex} />. We embed the index via a static import rather
than fetch it at runtime because mint dev does not serve raw .json files. The
module must be .jsx (not .json or .js): Mintlify only bundles .jsx snippet
imports as ES modules — a .js import is loaded as a classic script and throws
"Unexpected token 'export'", and JSON can't be statically imported here.

Usage:
    python _site/scripts/update_kb.py
"""

import re
import json
from pathlib import Path
from typing import Dict, Any, Tuple

# Directory slug -> human label, matching the sidebar groups in
# resources/support-center/navigation.json. Unlisted dirs fall back to a
# title-cased version of the slug.
CATEGORY_LABELS = {
    'performance-optimization': 'Performance & optimization',
    'cloud-services': 'Cloud',
    'data-import-export': 'Data import & export',
    'configuration-settings': 'Configuration & settings',
    'integrations': 'Integrations & client libraries',
    'troubleshooting': 'Troubleshooting & errors',
    'tables-schema': 'Tables & schema',
    'queries-sql': 'Queries & SQL',
    'materialized-views': 'Materialized views & projections',
    'monitoring-debugging': 'Monitoring & debugging',
    'security': 'Security & access control',
    'data-management': 'Data management',
    'setup-installation': 'Setup & installation',
    'general-faqs': 'General & FAQs',
}

def parse_frontmatter(content: str) -> Dict[str, Any]:
    """Parse simple YAML frontmatter (key: value and inline [arrays])."""
    match = re.match(r'^---\s*\n(.*?)\n---\s*\n', content, re.DOTALL)
    if not match:
        return {}

    frontmatter: Dict[str, Any] = {}
    for line in match.group(1).split('\n'):
        line = line.strip()
        if not line or line.startswith('#') or ':' not in line:
            continue

        key, value = line.split(':', 1)
        key = key.strip()
        value = value.strip()

        if value.startswith('[') and value.endswith(']'):
            items = [item.strip().strip('"').strip("'")
                     for item in value[1:-1].split(',')]
            frontmatter[key] = [item for item in items if item]
        else:
            frontmatter[key] = value.strip('"').strip("'")

    return frontmatter


def is_article(frontmatter: Dict[str, Any], file_path: Path) -> bool:
    """Exclude landing/index pages — they aren't individual articles."""
    if frontmatter.get('doc_type') == 'landing-page':
        return False
    if file_path.stem == 'index' or file_path.stem.endswith('-index'):
        return False
    return bool(frontmatter.get('title'))


def extract_article(file_path: Path, kb_dir: Path) -> Dict[str, Any]:
    """Extract a single article's index entry from its MDX file."""
    content = file_path.read_text(encoding='utf-8')
    frontmatter = parse_frontmatter(content)

    rel_path = file_path.relative_to(kb_dir)
    category_slug = rel_path.parts[0]
    article_id = str(rel_path.with_suffix('')).replace('\\', '/')

    tags = frontmatter.get('tags', [])
    if isinstance(tags, str):
        tags = [tags] if tags else []

    return {
        'id': article_id,
        'title': frontmatter.get('title', ''),
        'description': frontmatter.get('description', ''),
        'href': '/resources/support-center/knowledge-base/' + article_id,
        'category': CATEGORY_LABELS.get(
            category_slug,
            category_slug.replace('-', ' ').title()
        ),
        'tags': tags,
    }


def build_index(kb_dir: Path) -> Tuple[Dict[str, Any], int]:
    files = []
    for file_path in kb_dir.glob('**/*.mdx'):
        # Skip files in underscore-prefixed directories (assets/partials).
        rel_parts = file_path.relative_to(kb_dir).parts[:-1]
        if any(part.startswith('_') for part in rel_parts):
            continue
        files.append(file_path)

    articles = []
    skipped = 0
    for file_path in sorted(set(files)):
        frontmatter = parse_frontmatter(file_path.read_text(encoding='utf-8'))
        if not is_article(frontmatter, file_path):
            skipped += 1
            continue
        articles.append(extract_article(file_path, kb_dir))

    articles.sort(key=lambda a: a['title'].lower())

    return {
        'categories': sorted({a['category'] for a in articles}),
        'tags': sorted({tag for a in articles for tag in a['tags']}),
        'articles': articles,
    }, skipped


def render_module(index: Dict[str, Any]) -> str:
    """Render the kb-data.jsx module body.

    The index is emitted as JSON, which is valid JS object-literal syntax, so
    titles/descriptions with apostrophes, backticks, smart quotes, or non-Latin
    text need no special escaping. ensure_ascii=False keeps non-ASCII readable.
    """
    body = json.dumps(index, indent=2, ensure_ascii=False)
    return (
        "// AUTO-GENERATED by _site/scripts/update_kb.py — do not edit by hand.\n"
        "// Re-run the script to refresh the knowledge-base index.\n"
        f"export const kbIndex = {body};\n"
    )


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    kb_dir = project_root / 'resources' / 'support-center' / 'knowledge-base'
    output_path = (project_root / 'snippets' / 'components' / 'KBExplorer'
                   / 'kb-data.jsx')

    if not kb_dir.exists():
        print(f"Error: knowledge-base directory not found: {kb_dir}")
        return 1

    index, skipped = build_index(kb_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_module(index), encoding='utf-8')

    print(f"✓ Wrote {len(index['articles'])} articles to {output_path}")
    print(f"  {len(index['categories'])} categories, {len(index['tags'])} tags "
          f"({skipped} non-article pages skipped)")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())