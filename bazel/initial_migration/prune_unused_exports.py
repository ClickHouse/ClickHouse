#!/usr/bin/env python3
"""Remove source exports with no remaining BUILD, Starlark, or metadata consumer."""

import argparse
import ast
from collections import Counter, defaultdict
import json
from pathlib import Path
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source
from initial_migration.repository_inputs import ROOT


def prune_unused_exports(root=ROOT, write=False):
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


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    print(json.dumps(prune_unused_exports(args.root, args.write), indent=2))


if __name__ == '__main__':
    main()
