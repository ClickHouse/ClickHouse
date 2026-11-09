#!/usr/bin/env python3
"""Record all include directives per file in ClickHouse's pinned sources.

No source archive is extracted or written to disk. A receipt is installed
only after its downloaded compressed bytes match contrib_repositories.json.
This tool does not modify source checkouts or other cached audit results.
For a complete receipt directory consumed by native_cc_inputs.py:

  python3 bazel/initial_migration/scan_include_adjacency.py \
      --root . --output /tmp/clickhouse-include-receipts

--owner collects specified archives, --root-only collects just root files.
--root-revision defaults to HEAD; every scanned root file must still match
that revision. Optional --inventory files check the archive file lists against
earlier glob inventories. They are not needed to download and scan sources.

Receipt v3:
  name, owner, sha256, commit: names and verified source identity.
  inventory: sorted real source files visible after repositories.bzl's
    BUILD/.bazelignore removals; excludes the newly installed BUILD.bazel.
  files: mapping every scanned filename to all directives in encounter order.
    Each directive is [physical_line, directive, syntax, operand].
    directive includes the introducer # / % / . so ASM and C stay distinct.
    syntax is quote, angle, apostrophe, or computed. No computed operands
    are discarded or capped. quote/angle/apostrophe operands contain the
    filename without delimiters. computed operands contain the expression.
  unscanned: filename lists grouped by reason. Callers must not treat these
    files as having no includes. Missing/removed/broken links aren't inventory.
  source_links: resolved link filename -> real file in inventory.
  stats: receipt counts and digest data.
  For root receipts, stats.inventory_scope must be "git_tracked_regular":
    every regular Git file at commit, without generated Bazel metadata.
"""
import argparse
import bisect
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import gzip
import hashlib
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys
import tarfile
import time
import urllib.request

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.repository_inputs import (
    ROOT, TEXTUAL_SOURCE_SUFFIXES, archive_link_status,
)

VERSION = 3
SKIP = {'clickhouse_llvm_project', 'clickhouse_rust_vendor'}
ROOT_METADATA = {
    'BUILD', 'BUILD.bazel', 'REPO.bazel', 'WORKSPACE', 'WORKSPACE.bazel',
    'MODULE.bazel', 'MODULE.bazel.lock', '.bazelignore',
}
TU = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM',
      '.m', '.mm', '.cu')
SCAN_SUFFIXES = tuple(set(TU + TEXTUAL_SOURCE_SUFFIXES + (
    '.S.in', '.s.in', '.asm.in', '.hpp.in', '.h.in', '.i', '.ii',
    '.tbi', '.md.in', '.rc', '.pch', '.unx', '.cl',
)))
ASM_SUFFIXES = ('.s', '.asm', '.inc', '.s.in', '.asm.in')
# During preprocessing header-name tokens take priority over comments inside
# the angle brackets. Names such as <Interpreters//HashJoin/JoinUsedFlags.h>
# are literal names, not the start of a // comment.
GAP = r'(?:[ \t\f\v]|/\*[^\r\n]*?\*/)*'
CPP_COMMENTS = re.compile(
    rf'(?P<header>(?m:^{GAP}[#%]{GAP}(?:include_next|include|import|embed)\b{GAP}<[^>\r\n]*>))'
    r'|(?P<comment>/\*.*?\*/|//[^\n]*)|"(?:\\.|[^"\\\r\n])*"|\'(?:\\.|[^\'\\\r\n])*\'', re.S,
)
DIRECTIVE = re.compile(
    r'(?m)^[ \t\f\v]*(?P<intro>[#%])[ \t]*(?P<kind>include_next|include|import|embed)\b(?P<operand>[^\r\n]*)'
)
ASSEMBLER = re.compile(
    r'(?mi)^[ \t]*(?P<intro>\.)(?P<kind>include|incbin)\b(?P<operand>[^\r\n]*)'
)
OPERAND = re.compile(r"""^[ \t]*(?:"([^"\n]+)"|<([^>\n]+)>|'([^'\n]+)')(.*)$""")
CONTINUATIONS = re.compile(r'\\\r?\n')


class HashedInput:
    def __init__(self, source):
        self.source = source
        self.digest = hashlib.sha256()
        self.bytes = 0

    def read(self, size=-1):
        data = self.source.read(size)
        self.digest.update(data)
        self.bytes += len(data)
        return data


def classify(filename):
    name = PurePosixPath(filename).name
    if name.endswith(SCAN_SUFFIXES):
        return 'source_suffix'
    if '.' not in name.lstrip('.'):
        return 'extensionless'
    return 'unrecognized_suffix'


def scrub_comments(match):
    text = match[0]
    if match.lastgroup == 'comment':
        return ''.join('\n' if char == '\n' else ' ' for char in text)
    if match.lastgroup == 'header':
        prefix, delimiter, header = text.partition('<')
        return CPP_COMMENTS.sub(scrub_comments, prefix) + delimiter + header
    return text


def includes(raw, filename):
    if b'\x00' in raw:
        return None
    raw_text = raw.decode('utf-8', errors='replace')
    # Preprocessing joins logical lines before handling comments. Track
    # consumed physical newlines so diagnostics still name original lines.
    offsets = []
    consumed = 0
    for match in CONTINUATIONS.finditer(raw_text):
        offsets.append(match.start() - consumed)
        consumed += len(match[0])
    text = CONTINUATIONS.sub('', raw_text)
    text = CPP_COMMENTS.sub(scrub_comments, text)
    expressions = (DIRECTIVE, ASSEMBLER) if filename.lower().endswith(ASM_SUFFIXES) else (DIRECTIVE,)
    matches = sorted((m for expression in expressions for m in expression.finditer(text)),
                     key=lambda match: match.start())
    if not matches:
        return []
    result = []
    # Number of physical newlines before the given logical line.
    previous_pos, previous_line = 0, 0
    for match in matches:
        previous_line += text.count('\n', previous_pos, match.start())
        previous_pos = match.start()
        line = previous_line + bisect.bisect_right(offsets, match.start()) + 1
        expression = match['operand'].strip()
        value = OPERAND.match(expression)
        if value is None:
            syntax, operand = 'computed', expression
        else:
            (syntax, operand), = [(kind, s) for kind, s in
                                  zip(('quote', 'angle', 'apostrophe'), value.groups()[:3])
                                  if s is not None]
        result.append([line, match['intro'] + match['kind'].lower(), syntax, operand])
    return result


def link_target(filename, members, removed):
    path = filename
    seen = set()
    for _ in range(64):
        if path.startswith('/') or path == '..' or path.startswith('../') or path in seen:
            return None
        seen.add(path)
        parts = path.split('/')
        for count in range(1, len(parts) + 1):
            prefix = '/'.join(parts[:count])
            if prefix in removed or prefix not in members:
                return None
            kind, target = members[prefix]
            if kind in {'symlink', 'hardlink'}:
                if kind == 'symlink':
                    target = posixpath.join(posixpath.dirname(prefix), target)
                path = posixpath.normpath(posixpath.join(target, *parts[count:]))
                break
            if count != len(parts) and kind != 'directory':
                return None
        else:
            return path if members[path][0] == 'file' else None
    return None


def out_path(outdir, spec):
    return outdir / f"{spec['name']}-v{VERSION}-{spec['sha256']}.json.gz"


def verified_inventory(paths, repositories):
    inventory = {}
    for file in paths:
        opener = gzip.open if file.suffix == '.gz' else open
        with opener(file, 'rt') as source:
            for owner, contents in json.load(source).items():
                if contents['sha256'] != repositories[owner]['sha256']:
                    raise ValueError('Inventory SHA differs from repository pin: ' + owner)
                inventory[owner] = frozenset(contents['files']) - {'BUILD.bazel'}
    return inventory


def patch_directives(root, name, facts):
    if name != 'clickhouse_librdkafka':
        return
    patch = (root / 'bazel/patches/librdkafka-config-include.patch').read_text()
    expected = {'src/rd.h': 1, 'src/tinycthread.h': 1}
    if patch.count('-#include "../config.h"') != 2 or patch.count('+#include "config.h"') != 2:
        raise ValueError('librdkafka patch changed; update scan_include_adjacency')
    for filename, count in expected.items():
        updated = 0
        for directive in facts[filename]:
            if directive[1:] == ['#include', 'quote', '../config.h']:
                directive[3] = 'config.h'
                updated += 1
        if updated != count:
            raise ValueError(f'{name}:{filename} expected {count} ../config.h includes, found {updated}')


def scan(root, owner, spec, outdir, inventories, offline=False):
    output = out_path(outdir, spec)
    if output.exists():
        with gzip.open(output, 'rt') as source:
            result = json.load(source)
        if (result.get('version') == VERSION and result.get('sha256') == spec['sha256']
                and result.get('owner') == owner and result.get('name') == spec['name']
                and result.get('commit') == spec['commit']):
            return {'owner': owner, 'path': str(output), 'cached': True, **result['stats']}
        raise ValueError('Invalid existing receipt: ' + str(output))
    if offline:
        return {'owner': owner, 'missing': True}
    start = time.monotonic()
    request = urllib.request.Request(spec['url'], headers={'User-Agent': 'ClickHouse-include-adjacency-audit'})
    facts = {}
    members = {}
    categories = defaultdict(list)
    scanned_bytes = 0
    removed = set(spec['remove_build_files']) | {'.bazelignore', 'BUILD.bazel'}
    prefix = spec['strip_prefix'] + '/'
    with urllib.request.urlopen(request, timeout=130) as response:
        stream = HashedInput(response)
        with tarfile.open(fileobj=stream, mode='r|gz') as archive:
            for member in archive:
                if member.name.rstrip('/') == spec['strip_prefix']:
                    continue
                if not member.name.startswith(prefix):
                    raise ValueError(f"{owner}: unexpected archive member {member.name}")
                filename = member.name.removeprefix(prefix).rstrip('/')
                if filename.startswith('../') or filename.startswith('/'):
                    raise ValueError(f"{owner}: unsafe archive member {member.name}")
                kind = ('file' if member.isfile() else 'directory' if member.isdir()
                        else 'symlink' if member.issym() else 'hardlink' if member.islnk()
                        else 'nonregular')
                target = member.linkname
                if kind == 'hardlink' and target.startswith(prefix):
                    target = target.removeprefix(prefix)
                members[filename] = kind, target
                if kind != 'file' or filename in removed:
                    continue
                if PurePosixPath(filename).name in {'BUILD', 'BUILD.bazel'}:
                    raise ValueError(f"{owner}: {filename} missing from remove_build_files")
                classified = classify(filename)
                if classified != 'unrecognized_suffix':
                    raw = archive.extractfile(member).read()
                    extracted = includes(raw, filename)
                    if extracted is None:
                        categories['binary'].append(filename)
                    else:
                        facts[filename] = extracted
                        scanned_bytes += len(raw)
                else:
                    categories[classified].append(filename)
                # Streaming tarfile keeps TarInfo objects after iteration
                # unless cleared; our members contain only names and link data.
                archive.members.clear()
        while stream.read(1024 * 1024):
            pass
    digest = stream.digest.hexdigest()
    if digest != spec['sha256']:
        raise ValueError(f"{owner}: compressed SHA256 {digest}, expected {spec['sha256']}")
    statuses = archive_link_status(members, removed)
    inventory = sorted(name for name, (kind, _) in members.items()
                       if name not in removed and (kind == 'file' or statuses.get(name) == 'file'))
    inventory_differences = {}
    if owner in inventories and frozenset(inventory) != inventories[owner]:
        extra = sorted(frozenset(inventory) - inventories[owner])
        missing = sorted(inventories[owner] - frozenset(inventory))
        # Older glob inventories sometimes included symlinks to directories
        # or dangling links. Verify those against all pinned tar members and
        # repositories.bzl removals instead of treating them as source files.
        confirmed_nonfiles = {
            filename: statuses[filename] for filename in missing
            if filename in statuses and statuses[filename] != 'file'
        }
        unexplained = sorted(set(missing) - set(confirmed_nonfiles))
        if extra or unexplained:
            raise ValueError(f'{owner}: inventory differed: new={extra[:20]}, lost={unexplained[:20]}')
        inventory_differences = confirmed_nonfiles
    source_links = {}
    for name, status in statuses.items():
        if status != 'file' or name in removed:
            continue
        target = link_target(name, members, removed)
        if not target:
            raise ValueError(f'{owner}: inconsistent link resolution for {name}')
        source_links[name] = target
        if target in facts:
            facts[name] = facts[target]
        else:
            categories['link_target_not_scanned'].append(name)
    patch_directives(root, spec['name'], facts)
    counts = Counter((d[1], d[2]) for directives in facts.values() for d in directives)
    stats = {
        'inventory_files': len(inventory), 'scanned_files': len(facts),
        'scanned_bytes': scanned_bytes, 'compressed_bytes': stream.bytes,
        'directives': sum(counts.values()), 'computed': sum(n for (_, fmt), n in counts.items() if fmt == 'computed'),
        'count_by_directive': {' '.join(key): n for key, n in sorted(counts.items())},
        'linked_files': len(source_links),
        'inventory_checked': owner in inventories,
        'elapsed_seconds': round(time.monotonic() - start, 3),
    }
    unscanned = {key: sorted(names) for key, names in categories.items()}
    all_unscanned = frozenset(name for names in unscanned.values() for name in names)
    if set(facts) & all_unscanned or (set(facts) | all_unscanned) != set(inventory):
        raise ValueError(owner + ': files plus unscanned do not partition inventory')
    result = {
        'version': VERSION, 'name': spec['name'], 'owner': owner,
        'sha256': digest, 'commit': spec['commit'], 'source_url': spec['url'],
        'inventory': inventory, 'files': facts, 'source_links': source_links,
        'unscanned': unscanned, 'stats': stats,
        'old_inventory_nonfiles': inventory_differences,
    }
    temporary = output.with_suffix('.incomplete.gz')
    with gzip.open(temporary, 'wt', compresslevel=3) as destination:
        json.dump(result, destination, separators=(',', ':'))
    temporary.replace(output)
    return {'owner': owner, 'path': str(output), 'cached': False, **stats}


def scan_root(root, revision, outdir):
    """Record tracked root sources after checking bytes against a Git commit."""
    commit = subprocess.check_output(
        ['git', 'rev-parse', revision + '^{commit}'], cwd=root, text=True,
    ).strip()
    output = outdir / f'clickhouse-root-v{VERSION}-{commit}.json.gz'
    if output.exists():
        with gzip.open(output, 'rt') as source:
            result = json.load(source)
        if (result.get('version') == VERSION and result.get('commit') == commit
                and result['stats'].get('inventory_scope') == 'git_tracked_regular'):
            return {'owner': '', 'path': str(output), 'cached': True, **result['stats']}
        raise ValueError(f'{output}: root receipt does not cover all Git files; use a fresh --output directory')
    root_tree = {}
    tree = subprocess.check_output(['git', 'ls-tree', '-rz', '--full-tree', commit], cwd=root)
    for entry in tree.split(b'\0'):
        if not entry:
            continue
        left, filename = entry.split(b'\t', 1)
        mode, kind, blob = left.decode().split()
        name = filename.decode()
        # gitlinks (160000) are pinned external repositories; their headers
        # come from the corresponding http_archive receipts. Exclude symlink
        # blobs too: following one could read another repository's checkout.
        if kind == 'blob' and mode in {'100644', '100755'} and PurePosixPath(name).name not in ROOT_METADATA:
            root_tree[name] = blob
    categories = defaultdict(list)
    files = {}
    inventory = []
    verified_blobs = {}
    scanned_bytes = 0
    for filename, blob in root_tree.items():
        inventory.append(filename)
        if classify(filename) == 'unrecognized_suffix':
            categories['unrecognized_suffix'].append(filename)
            continue
        raw = (root / filename).read_bytes()
        actual = hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest()
        if actual != blob:
            raise ValueError(f'{filename}: current Git blob {actual} does not match {commit} blob {blob}')
        data = includes(raw, filename)
        if data is None:
            categories['binary'].append(filename)
        else:
            files[filename] = data
            verified_blobs[filename] = blob
            scanned_bytes += len(raw)
    stats = {
        'inventory_files': len(inventory), 'scanned_files': len(files),
        'scanned_bytes': scanned_bytes,
        'directives': sum(len(incs) for incs in files.values()),
        'computed': sum(inc[2] == 'computed' for incs in files.values() for inc in incs),
        'inventory_checked': True, 'inventory_scope': 'git_tracked_regular',
        'linked_files': 0,
    }
    result = {
        'version': VERSION, 'name': '', 'owner': '', 'sha256': None,
        'commit': commit, 'source_url': 'git:' + str(root),
        'inventory': sorted(inventory), 'files': files, 'source_links': {},
        'unscanned': {key: sorted(names) for key, names in categories.items()},
        'stats': stats, 'verified_blobs': verified_blobs,
    }
    temporary = output.with_suffix('.incomplete.gz')
    with gzip.open(temporary, 'wt', compresslevel=3) as destination:
        json.dump(result, destination, separators=(',', ':'))
    temporary.replace(output)
    return {'owner': '', 'path': str(output), 'cached': False, **stats}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', required=True, type=Path,
                        help='Directory for uncommitted receipt files.')
    parser.add_argument('--inventory', action='append', default=[], type=Path,
                        help='Optional JSON or JSON.gz pinned-file inventory; may be repeated.')
    parser.add_argument('--jobs', type=int, default=4)
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument('--owner', action='append', default=[],
                       help='Only scan specified contrib/name or clickhouse_name archives; may be repeated.')
    scope.add_argument('--root-only', action='store_true', help='Only scan root sources.')
    parser.add_argument('--offline', action='store_true')
    parser.add_argument('--root-revision', default='HEAD',
                        help='Git revision that scanned root source files must match (default: HEAD).')
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error('--jobs must be at least 1')
    args.root = args.root.resolve()
    args.output.mkdir(parents=True, exist_ok=True)
    status = []
    if not args.owner:
        record = scan_root(args.root, args.root_revision, args.output)
        status.append(record)
        print(json.dumps(record, separators=(',', ':')), flush=True)
    if args.root_only:
        return
    repositories = json.loads((args.root / 'bazel/rules/contrib_repositories.json').read_text())
    inventory = verified_inventory(args.inventory, repositories)
    selected = [(owner, spec) for owner, spec in repositories.items()
                if spec['name'] not in SKIP and not spec.get('module')
                and (not args.owner or owner in args.owner or spec['name'] in args.owner)]
    if args.owner and len(selected) != len(set(args.owner)):
        raise SystemExit('Not all requested owners are eligible or recognized')
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = {pool.submit(scan, args.root, owner, spec, args.output, inventory, args.offline): owner
                   for owner, spec in selected}
        for future in as_completed(futures):
            owner = futures[future]
            try:
                record = future.result()
                status.append(record)
                print(json.dumps(record, separators=(',', ':')), flush=True)
            except Exception as error:
                record = {'owner': owner, 'error': repr(error)}
                status.append(record)
                print(json.dumps(record, separators=(',', ':')), flush=True)
    summary = {
        'version': VERSION, 'selected': len(selected), 'receipts': status,
        'errors': sum('error' in r for r in status),
        'missing': sum(bool(r.get('missing')) for r in status),
        'total_compressed_bytes': sum(r.get('compressed_bytes', 0) for r in status),
        'total_directives': sum(r.get('directives', 0) for r in status),
        'total_scanned_files': sum(r.get('scanned_files', 0) for r in status),
    }
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps({key: value for key, value in summary.items() if key != 'receipts'}))
    if summary['errors'] or summary['missing']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
