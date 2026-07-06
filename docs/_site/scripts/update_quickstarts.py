#!/usr/bin/env python3
"""
Script to automatically extract quick-start metadata and update home.mdx

This script scans the docs/get-started/quickstarts/ directory for .mdx files,
extracts metadata from their frontmatter, and updates the quickStartsData array
in home.mdx.

Usage:
    python scripts/update_quickstarts.py
"""

import re
import json
from pathlib import Path
from typing import Dict, List, Any

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
            # Skip home.mdx and files in underscore-prefixed directories
            if file_path.name == 'home.mdx':
                continue
            if any(part.startswith('_') for part in file_path.relative_to(quickstarts_dir).parts[:-1]):
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
    # homepage links (e.g. "Read the quickstart" / "Set up docs MCP server").
    first_line = '<a href="home" className="inline-flex items-center gap-1.5 text-sm text-gray-500 dark:text-zinc-500 hover:text-gray-900 dark:hover:text-[#fdff75] transition-colors font-normal no-underline"><svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" className="shrink-0"><path d="M19 12H5" /><path d="M12 19l-7-7 7-7" /></svg>All quickstarts</a>'

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

    # Add product badges (orange)
    for product in products:
        # Capitalize properly
        display_text = product.title()
        second_line_badges.append(f'<Badge size="lg" color="orange">{display_text}</Badge>')

    # Combine with line break and add margin
    second_line = '\n'.join(second_line_badges)

    return f'{first_line}\n<div className="mt-2 flex flex-wrap gap-2">\n{second_line}\n</div>'

def update_quickstart_badges(file_path: Path, use_cases: List[str], products: List[str]) -> None:
    """
    Update the autogenerated badges section in a quick-start MDX file.

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

    # Check if markers exist
    if not re.search(pattern, content, re.DOTALL):
        print(f"    Warning: No AUTOGENERATED markers found in {file_path.name}")
        return

    # Replace the content between markers
    replacement = f'{{/* AUTOGENERATED_START */}}\n{badges}\n{{/* AUTOGENERATED_END */}}'
    content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)

def generate_javascript_array(quickstarts: List[Dict[str, Any]]) -> str:
    """
    Generate JavaScript array code from quick-start data.

    Args:
        quickstarts: List of quick-start dictionaries

    Returns:
        JavaScript code as a string
    """
    lines = ['export const quickStartsData = [']

    for qs in quickstarts:
        lines.append('  {')

        # Add each field
        for key, value in qs.items():
            if isinstance(value, list):
                # Format arrays
                array_items = ', '.join(f"'{item}'" for item in value)
                lines.append(f"    {key}: [{array_items}],")
            elif isinstance(value, str):
                # Escape single quotes in strings
                escaped_value = value.replace("'", "\\'")
                lines.append(f"    {key}: '{escaped_value}',")
            else:
                lines.append(f"    {key}: {json.dumps(value)},")

        lines.append('  },')

    lines.append('];')

    return '\n'.join(lines)

def update_home_mdx(home_path: Path, quickstarts_data: str) -> None:
    """
    Update the home.mdx file with new quickstarts data.

    Args:
        home_path: Path to home.mdx
        quickstarts_data: JavaScript array code as a string
    """
    with open(home_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Find the quickStartsData array and replace it
    # Match from "export const quickStartsData = [" to the closing "];"
    pattern = r'export const quickStartsData = \[.*?\];'

    if not re.search(pattern, content, re.DOTALL):
        print("Warning: Could not find quickStartsData array in home.mdx")
        print("Adding it before the QuickStartsGrid component...")

        # Find the component usage and add data before it
        component_pattern = r'(<QuickStartsGrid)'
        if re.search(component_pattern, content):
            content = re.sub(
                component_pattern,
                f'{quickstarts_data}\n\n{{/* Render the QuickStartsGrid component */}}\n\\1',
                content
            )
        else:
            print("Error: Could not find QuickStartsGrid component in home.mdx")
            return
    else:
        # Replace existing data
        content = re.sub(pattern, quickstarts_data, content, flags=re.DOTALL)

    with open(home_path, 'w', encoding='utf-8') as f:
        f.write(content)

    print(f"✓ Updated {home_path}")

def main():
    """Main function to run the script."""
    # Get the project root directory (this script lives in _site/scripts/)
    project_root = Path(__file__).resolve().parents[2]

    # Paths
    quickstarts_dir = project_root / 'get-started' / 'quickstarts'
    home_path = quickstarts_dir / 'home.mdx'

    if not quickstarts_dir.exists():
        print(f"Error: Quick-starts directory not found: {quickstarts_dir}")
        return 1

    if not home_path.exists():
        print(f"Error: home.mdx not found: {home_path}")
        return 1

    print(f"Scanning for quick-start files in {quickstarts_dir}...")

    # Find all quick-start files
    files = find_quickstart_files(quickstarts_dir)

    if not files:
        print("No quick-start files found (excluding home.mdx)")
        return 0

    print(f"Found {len(files)} quick-start file(s)")

    # Extract metadata from each file and update badges
    quickstarts = []
    for file_path in files:
        try:
            data = extract_quickstart_data(file_path, project_root)
            quickstarts.append(data)
            print(f"  ✓ {file_path.name}: {data['title']}")

            # Update badges in the file
            update_quickstart_badges(file_path, data['useCases'], data['products'])
        except Exception as e:
            print(f"  ✗ {file_path.name}: Error - {e}")

    if not quickstarts:
        print("No valid quick-start data extracted")
        return 1

    # Generate JavaScript array
    print("\nGenerating JavaScript array...")
    js_array = generate_javascript_array(quickstarts)

    # Update home.mdx
    print(f"\nUpdating {home_path}...")
    update_home_mdx(home_path, js_array)

    print(f"\n✓ Successfully updated home.mdx with {len(quickstarts)} quick-start(s)")
    print(f"✓ Updated badges in {len(quickstarts)} quick-start file(s)")
    return 0

if __name__ == '__main__':
    exit(main())
