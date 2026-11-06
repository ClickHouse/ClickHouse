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
        for key in ('pages', 'anchors', 'dropdowns', 'root'):
            yield from pages(value.get(key, []))

groups = {}
for page in sorted(set(pages(json.loads((root / 'clickstack/navigation.json').read_text())))):
    source = root / (page + '.mdx')
    if not source.is_file():
        raise FileNotFoundError(source)
    # Mintlify serves navigation file paths; legacy slug frontmatter is not a route.
    slug = '/' + re.sub(r'/index$', '', page)
    # Product landing pages already identify the offering in their title.
    if slug in ('/clickstack', '/clickstack/observability'):
        continue
    relative = page.removeprefix('clickstack/')
    product = 'Open Source'
    for prefix, label in [('observability/', 'ClickHouse Observability'), ('self-configured/', 'Self configured')]:
        if relative.startswith(prefix):
            relative = relative.removeprefix(prefix)
            product = label
            break
    key = re.sub(r'(^|/)index$', '', relative).rstrip('/')
    # Demo Days label availability per feature rather than per page.
    if re.match(r'demo-days/\d{4}/', key):
        continue
    if key == 'getting-started/oss':
        key = 'getting-started'
    groups.setdefault(key, {})[product] = slug

mapping = {}
for variants in groups.values():
    for slug in variants.values():
        badges = []
        if 'Open Source' in variants:
            badges.append(['Open Source', variants['Open Source']])
        observability = variants.get('ClickHouse Observability') or variants.get('Self configured')
        # Self-configured deployments belong to ClickHouse Observability.
        # Keep its pill active on the current self-configured guide.
        if slug == variants.get('Self configured'):
            observability = slug
        if observability:
            badges.append(['ClickHouse Observability', observability])
        mapping[slug] = badges

# Read feature anchors from both product editions, including shared newer pages.
exceptions = json.loads((Path(__file__).with_name('demo-days-availability.json')).read_text())
sections = {}
for source in sorted((root / 'clickstack').glob('**/demo-days/2026/*.mdx')):
    page = '/' + source.relative_to(root).with_suffix('').as_posix()
    date = source.stem
    oss = f'/clickstack/demo-days/2026/{date}'
    observability = f'/clickstack/observability/demo-days/2026/{date}'
    if not (root / (observability.lstrip('/') + '.mdx')).exists():
        observability = oss
    anchors = re.findall(r'^## .*?\{#([^}]+)\}$', source.read_text(), re.M)
    only = exceptions.get(date, [])
    missing = set(only) - set(anchors)
    if missing:
        raise ValueError(f'{source}: missing feature anchors {missing}')
    sections[page] = {
        'variants': [['Open Source', oss], ['ClickHouse Observability', observability]],
        'observabilityOnly': only,
    }

path = root / '_site/customizations/clickstack-availability.js'
s = path.read_text()
s = re.sub(r'  // BEGIN PAGE MAP.*?  // END PAGE MAP', '  // BEGIN PAGE MAP\n  var pages = ' + json.dumps(mapping, sort_keys=True, indent=2) + ';\n  // END PAGE MAP', s, flags=re.S)
s = re.sub(r'  // BEGIN SECTION MAP.*?  // END SECTION MAP', '  // BEGIN SECTION MAP\n  var sections = ' + json.dumps(sections, sort_keys=True, indent=2) + ';\n  // END SECTION MAP', s, flags=re.S)
path.write_text(s)
print(f'Generated availability for {len(mapping)} pages.')
