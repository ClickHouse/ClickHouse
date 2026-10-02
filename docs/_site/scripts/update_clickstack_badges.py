"""Regenerate page availability from the ClickStack navigation routes."""
import json
import re
from pathlib import Path

root = Path(__file__).resolve().parents[2]

def pages(value):
    if isinstance(value, str) and value.startswith('clickstack/'):
        yield value
    elif isinstance(value, list):
        for item in value:
            yield from pages(item)
    elif isinstance(value, dict):
        for key in ('pages', 'anchors', 'dropdowns'):
            yield from pages(value.get(key, []))

groups = {}
for page in sorted(set(pages(json.loads((root / 'clickstack/navigation.json').read_text())))):
    source = root / (page + '.mdx')
    if not source.is_file():
        raise FileNotFoundError(source)
    # Mintlify serves navigation file paths; legacy slug frontmatter is not a route.
    slug = '/' + re.sub(r'/index$', '', page)
    relative = page.removeprefix('clickstack/')
    product = 'Open Source'
    for prefix, label in [('observability/', 'Fully managed'), ('self-configured/', 'Self Configured')]:
        if relative.startswith(prefix):
            relative = relative.removeprefix(prefix)
            product = label
            break
    key = re.sub(r'(^|/)index$', '', relative).rstrip('/')
    if key == 'getting-started/oss':
        key = 'getting-started'
    groups.setdefault(key, {})[product] = slug

mapping = {}
order = ['Open Source', 'Self Configured', 'Fully managed']
for variants in groups.values():
    badges = [[label, variants[label]] for label in order if label in variants]
    for slug in variants.values():
        mapping[slug] = badges

path = root / '_site/customizations/clickstack-availability.js'
s = path.read_text()
s = re.sub(r'  // BEGIN PAGE MAP.*?  // END PAGE MAP', '  // BEGIN PAGE MAP\n  var pages = ' + json.dumps(mapping, sort_keys=True, indent=2) + ';\n  // END PAGE MAP', s, flags=re.S)
path.write_text(s)
print(f'Generated availability for {len(mapping)} pages.')
