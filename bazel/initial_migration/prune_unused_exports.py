#!/usr/bin/env python3
"""Remove source exports with no remaining BUILD, Starlark, or metadata consumer."""

import argparse
import ast
from collections import Counter, defaultdict
from copy import deepcopy
import json
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, ast_key
from initial_migration.repository_inputs import ROOT


def prune_unused_exports(root=ROOT, write=False, *, read_only=()):
    # Read quoted strings instead of splitting at spaces or brackets. Some
    # actual pinned source labels contain both.
    paths = subprocess.check_output([
        'rg', '--files', '--hidden', '-g', '*.bzl', '-g', '*.BUILD.bazel',
        '-g', '*.MODULE.bazel', '-g', 'MODULE.bazel', '-g', 'BUILD',
        '-g', 'BUILD.bazel', '-g', 'bazel/*.json', '-g', 'bazel/**/*.json',
        '-g', '!MODULE.bazel.lock',
    ], cwd=root, text=True).splitlines()
    required = defaultdict(set)
    stems = defaultdict(set)
    prefix = re.compile(r'(@clickhouse_[A-Za-z0-9_.+\-]+)//:')
    location = re.compile(r'\$\((?:locations?|execpaths?|rootpaths?)\s+([^)]*)\)')

    def record(value, dynamic=False):
        match = prefix.match(value)
        if not match:
            return False
        name = value[match.end():]
        if dynamic:
            # '%' and '{' introduce formatting placeholders; '$' occurs in
            # shell expansions of source labels.
            name = re.split(r'[{$%]', name, maxsplit=1)[0]
            stems[match[1]].add(name)
        else:
            required[match[1]].add(name)
        return True

    def scan_string(value):
        if record(value):
            if any(char in value for char in '{%$'):
                record(value, True)
        ranges = []
        for match in location.finditer(value):
            record(match[1])
            ranges.append((match.start(), match.end()))
        for match in prefix.finditer(value):
            if match.start() and not any(start <= match.start() < end for start, end in ranges):
                # Dynamic shell command, repository is known but filenames
                # can't be proven from this string.
                stems[match[1]].add('')

    def scan_json(value):
        if isinstance(value, str):
            scan_string(value)
        elif isinstance(value, list):
            for item in value:
                scan_json(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                scan_json(key)
                scan_json(item)

    for name in paths:
        path = root / name
        text = path.read_text()
        if path.suffix == '.json':
            scan_json(json.loads(text))
            continue
        for node in ast.walk(ast.parse(text, filename=name)):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                scan_string(node.value)
            elif (isinstance(node, ast.BinOp) and isinstance(node.left, ast.Constant)
                  and isinstance(node.left.value, str)):
                record(node.left.value, True)
            elif (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                  and node.func.attr == 'format' and isinstance(node.func.value, ast.Constant)
                  and isinstance(node.func.value.value, str)):
                record(node.func.value.value, True)
    stats = Counter()
    for path in sorted((root / 'bazel/dependencies').glob('*.BUILD.bazel')):
        if path in read_only:
            continue
        repo = '@' + path.name.removesuffix('.BUILD.bazel')
        src = Source(path.read_text())
        edits = []
        for statement in src.module.body:
            if not (isinstance(statement, ast.Expr) and isinstance(statement.value, ast.Call)
                    and getattr(statement.value.func, 'id', '') == 'exports_files'):
                continue
            call = statement.value
            if len(call.args) != 1 or not isinstance(call.args[0], ast.List):
                continue
            try:
                original = ast.literal_eval(call.args[0])
            except (ValueError, TypeError):
                continue
            if not all(isinstance(name, str) for name in original):
                continue
            kept = [name for name in original if name in required[repo] or
                    any(name.startswith(stem) for stem in stems[repo])]
            if kept == original:
                continue
            stats['removed_exports'] += len(original) - len(kept)
            stats[repo] += len(original) - len(kept)
            if kept:
                edits.append((*src.span(call.args[0]), json.dumps(kept, indent=4)))
            else:
                edits.append((*src.span(statement), ''))
        if edits:
            stats['files'] += 1
            text = src.rewrite(edits)
            ast.parse(text, filename=str(path))
            if write:
                path.write_text(text)
    return stats


def prune_native_exports(root=ROOT, write=False, *, only=None):
    """Prune native exports only when no foreign BUILD, macro, or metadata uses them."""
    from initial_migration.first_party_packages import Build, CPUS, call_name, git_inventory, native_directory

    listing = subprocess.check_output([
        'git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
        '--', '*.bzl', '*.BUILD.bazel', '*.MODULE.bazel', '*.json',
        'BUILD', 'BUILD.bazel', '**/BUILD', '**/BUILD.bazel', 'MODULE.bazel', 'REPO.bazel',
    ], cwd=root).decode().split('\0')
    paths = sorted({name for name in listing if name and (root / name).is_file() and (
        (root / name).name in {'BUILD', 'BUILD.bazel', 'MODULE.bazel', 'REPO.bazel'}
        or name.endswith(('.bzl', '.BUILD.bazel', '.MODULE.bazel'))
        or name.startswith('bazel/') and name.endswith('.json') and not name.endswith('MODULE.bazel.lock')
    )})
    packages = {str(Path(name).parent).removeprefix('.') for name in paths
                if Path(name).name in {'BUILD', 'BUILD.bazel'}}
    documents = {}
    sources = {}
    exports = defaultdict(list)
    required = set()
    target_stems = defaultdict(set)
    package_stems = set()
    native_inventory = git_inventory(root)
    # External overlays have their own // namespace. They must use @clickhouse
    # for main-repository labels; .bzl macros can run outside their own package.
    label = re.compile(
        r'(?P<repo>@clickhouse|@{1,2}[^\s\[\]{}"\'()+:;/]*)?//'
        r'(?P<pkg>[^\s\[\]{}"\'()+:;/$]*?(?:/[^\s\[\]{}"\'()+:;/$]*)*):'
        r'(?P<name>[^\s\[\]{}"\'(),;$%]*)')

    def record(value, filename, dynamic=False):
        external = filename.endswith(('.BUILD.bazel', '.MODULE.bazel')) or filename.startswith('bazel/dependencies/')
        package = str(Path(filename).parent).removeprefix('.')
        if dynamic or any(marker in value for marker in ('{', '%', '$')):
            stem = re.match(r'^(@clickhouse|@)?//([^:\s{%$]+)(?:[{%$]|$)', value)
            if stem and (stem[1] or not external):
                package_stems.add(stem[2])
        for match in label.finditer(value):
            repo, owner, name = match.group('repo', 'pkg', 'name')
            if repo not in {None, '@clickhouse', '@'} or repo is None and external:
                continue
            if filename in documents and owner == package:
                continue
            if dynamic or value[match.end():match.end() + 1] in {'{', '%', '$'}:
                target_stems[owner].add(name)
            else:
                required.add('//' + owner + ':' + name)

    def scan_json(value, filename):
        if isinstance(value, str):
            record(value, filename)
        elif isinstance(value, dict):
            for key, child in value.items():
                scan_json(key, filename)
                scan_json(child, filename)
        elif isinstance(value, list):
            for child in value:
                scan_json(child, filename)

    for filename in paths:
        path = root / filename
        package = str(path.parent.relative_to(root)).removeprefix('.')
        text = path.read_text()
        if path.suffix == '.json':
            scan_json(json.loads(text), filename)
            continue
        if path.name in {'BUILD', 'BUILD.bazel'} and (not package or native_directory(package)):
            doc = Build(root, path, native_inventory, packages, text)
            documents[filename] = doc
            source = doc.source
        else:
            source = Source(text)
        sources[filename] = source
        exported_nodes = set()
        for statement in source.module.body:
            if not isinstance(statement, ast.Expr) or call_name(statement.value) != 'exports_files':
                continue
            call = statement.value
            exported_nodes.update(map(id, ast.walk(call)))
            if filename not in documents:
                continue
            # Only native exports understood by Build.values may be rewritten.
            # Preserve dynamic exports instead of interpreting a partial value.
            if len(call.args) != 1:
                continue
            expanded = documents[filename].expand(call.args[0])
            if any(isinstance(node, ast.Call) and call_name(node) not in {'glob', 'select'}
                   or isinstance(node, ast.Name) and node.id not in {'glob', 'select'}
                   for node in ast.walk(expanded)):
                continue
            choices = [documents[filename].values(expanded, cpu) for cpu in CPUS]
            if choices[0] != choices[1]:
                continue
            labels = [(value if value.startswith('//') else '//' + package + ':' + value.removeprefix(':'))
                      for value in choices[0]]
            exports[filename].append((statement, labels))
        parents = {id(child): parent for parent in ast.walk(source.module) for child in ast.iter_child_nodes(parent)}
        for node in ast.walk(source.module):
            if id(node) in exported_nodes or not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                continue
            parent = parents.get(id(node))
            dynamic = (isinstance(parent, ast.BinOp) and isinstance(parent.op, (ast.Add, ast.Mod))
                       or isinstance(parent, ast.Attribute) and parent.attr == 'format')
            record(node.value, filename, dynamic)

    stats = Counter()
    for filename, declarations in exports.items():
        source = sources[filename]
        edits = []
        for statement, labels in declarations:
            kept = []
            for exported in labels:
                owner, name = exported[2:].split(':', 1)
                if ((only is not None and exported not in only) or
                        exported in required or any(name.startswith(stem) for stem in target_stems[owner])
                        or any(owner.startswith(stem) for stem in package_stems)):
                    kept.append(exported)
            stats['retained_exports'] += len(kept)
            if len(kept) == len(labels):
                continue
            count = len(labels) - len(kept)
            stats['removed_exports'] += count
            stats[filename] += count
            if kept:
                edits.append((*source.span(statement.value.args[0]),
                              json.dumps([label.split(':', 1)[1] for label in kept], indent=4)))
            else:
                edits.append((*source.span(statement), ''))
        if not edits:
            continue
        text = source.rewrite(edits)
        original, updated = deepcopy(source.module), ast.parse(text, filename=filename)
        for module in (original, updated):
            module.body = [statement for statement in module.body
                           if not (isinstance(statement, ast.Expr)
                                   and call_name(statement.value) == 'exports_files')]
        if ast_key(original) != ast_key(updated):
            raise ValueError('Pruning changed a non-exports declaration in ' + filename)
        stats['files'] += 1
        if write:
            (root / filename).write_text(text)
    return stats


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write', action='store_true')
    parser.add_argument('--native', action='store_true', help='Prune first-party instead of external exports')
    args = parser.parse_args()
    prune = prune_native_exports if args.native else prune_unused_exports
    print(json.dumps(prune(args.root, args.write), indent=2))


if __name__ == '__main__':
    main()
