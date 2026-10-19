#!/usr/bin/env python3
"""Compact options and source lists in ClickHouse external BUILD overlays.

Keep cmake_link.bzl source ordinals: only replace a source run when a glob
expands to those exact files, in that order, from the pinned repository.
compact() accepts package_sources(owner) for generator use. The standalone
command accepts --facts from scan_include_adjacency.py; without --facts it
only factors literal lists and does not introduce source globs. --write
formats changed BUILD files and verifies both CPU configurations before
replacing any original.
"""
import argparse
import ast
from bisect import bisect_left
from collections import Counter, defaultdict
import copy
from difflib import SequenceMatcher
import gzip
import json
from pathlib import Path
import posixpath
import re
import sys
import subprocess

ROOT_DEFAULT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DEFAULT / 'bazel'))
from initial_migration.compact_build import (  # noqa: E402
    Source, ast_key, render_list, safe_expression, select_dictionary,
)
from initial_migration.simplify_globs import GlobFiles, pattern_expression  # noqa: E402

ATTRIBUTES = ('copts', 'local_includes', 'includes', 'srcs')
NATIVE = {'cc_library', 'ch_cc_library', 'cc_binary', 'cc_test',
          'cmake_runtime_objects', 'nasm_objects'}
CPUS = ('@platforms//cpu:x86_64', '@platforms//cpu:aarch64')
SOURCE_SUFFIXES = {'.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM'}


def function(node):
    return node.func.id if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) else None


def attrs(rule):
    return {attr.arg: attr.value for attr in rule.keywords}


class PackageFiles(GlobFiles):
    """The same Bazel glob match as GlobFiles, indexed by literal prefix."""

    def __init__(self, filenames):
        super().__init__(filenames)
        self.sorted_files = sorted(self.files)

    def matching(self, pattern):
        if pattern in self.matches:
            return self.matches[pattern]
        if '*' not in pattern:
            found = frozenset({pattern} & self.files)
        else:
            expression = pattern_expression(pattern)
            prefix = pattern[:pattern.index('*')]
            begin = bisect_left(self.sorted_files, prefix)
            end = bisect_left(self.sorted_files, prefix + '\uffff')
            found = frozenset(path for path in self.sorted_files[begin:end]
                              if expression.fullmatch(path))
        self.matches[pattern] = found
        return found


def load_package_files(facts_dir, owner, spec):
    if facts_dir is None:
        return None
    receipt = facts_dir / (spec['name'] + '-v3-' + spec['sha256'] + '.json.gz')
    if not receipt.exists():
        return None
    with gzip.open(receipt, 'rt') as stream:
        data = json.load(stream)
    for key, value in (('version', 3), ('owner', owner), ('name', spec['name']),
                       ('commit', spec['commit']), ('sha256', spec['sha256'])):
        if data.get(key) != value:
            raise ValueError(f'{receipt}: expected {key}={value}, found {data.get(key)!r}')
    if not data.get('stats', {}).get('inventory_checked'):
        return None
    # scan_include_adjacency validated every nested BUILD against the removal list.
    inventory = set(data['inventory'])
    remaining_builds = {s for s in inventory if posixpath.basename(s) in {'BUILD', 'BUILD.bazel'}}
    if remaining_builds - {'BUILD.bazel'}:
        raise ValueError(f'{receipt}: remaining nested BUILD files: {sorted(remaining_builds)}')
    return PackageFiles(inventory)


def eligible_constants(source):
    """Only constants whose every read is in the selected native attributes."""
    allowed_reads = defaultdict(list)
    for _, rule in source.rules():
        if function(rule) in NATIVE:
            name = attrs(rule).get('name')
            if not isinstance(name, ast.Constant):
                continue
            for attr in rule.keywords:
                if attr.arg not in ATTRIBUTES:
                    continue
                for node in ast.walk(attr.value):
                    if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
                        allowed_reads[node.id].append((node, name.value, attr.arg))
    all_reads = defaultdict(list)
    for node in ast.walk(source.module):
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            all_reads[node.id].append(node)
    result = {}
    for statement in source.module.body:
        if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                and isinstance(statement.targets[0], ast.Name)):
            name = statement.targets[0].id
            for field in ATTRIBUTES:
                if (name.startswith('_GENERATED_' + field.upper() + '_') and all_reads[name]
                        and {id(item) for item in all_reads[name]}
                        == {id(item[0]) for item in allowed_reads[name]}
                        and all(item[2] == field for item in allowed_reads[name])):
                    result[name] = (statement, field, allowed_reads[name])
    return result


def native_values(source):
    for name, (statement, field, _) in eligible_constants(source).items():
        yield field, statement.value, 0, name
    for _, rule in source.rules():
        if function(rule) not in NATIVE:
            continue
        for attr in rule.keywords:
            if attr.arg in ATTRIBUTES:
                yield attr.arg, attr.value, attr.col_offset, attrs(rule)['name'].value


def factor(text, stats):
    source = Source(text)
    edits = []
    for field, value, indent, _ in native_values(source):
        for node in ast.walk(value):
            choices = select_dictionary(node)
            if choices is None or not safe_expression(node):
                continue
            if (len(choices.keys) != 2
                    or any(not isinstance(key, ast.Constant) for key in choices.keys)
                    or {key.value for key in choices.keys} != set(CPUS)):
                continue
            if '#' in source.text(node):
                continue
            x, a = (branch.elts for branch in choices.values)
            if any(function(nested) == 'select'
                   for element in [*x, *a] for nested in ast.walk(element)):
                continue
            keyed_x, keyed_a = [ast_key(n) for n in x], [ast_key(n) for n in a]
            common = SequenceMatcher(a=keyed_x, b=keyed_a, autojunk=False).get_matching_blocks()
            # For an interior block a second select costs more than a few items.
            common = [block for block in common if block.size and (
                block.a == block.b == 0
                or (block.a + block.size == len(x) and block.b + block.size == len(a))
                or block.size >= 3)]
            if not common:
                continue
            parts = []
            previous_x = previous_a = 0
            selects = 0

            def add_select(part_x, part_a):
                rows = [
                    ' ' * (indent + 4) + source.text(key) + ': ' +
                    render_list(source, values, indent + 4) + ',\n'
                    for key, values in zip(choices.keys, (part_x, part_a))
                ]
                parts.append('select({\n' + ''.join(rows) + ' ' * indent + '})')

            for block in common:
                if block.a > previous_x or block.b > previous_a:
                    add_select(x[previous_x:block.a], a[previous_a:block.b])
                    selects += 1
                parts.append(render_list(source, x[block.a:block.a + block.size], indent))
                previous_x, previous_a = block.a + block.size, block.b + block.size
            if previous_x < len(x) or previous_a < len(a) or not selects:
                # Retain CPU-selection domain when all branches equal: neither
                # CPU matching must still fail as in the original select().
                add_select(x[previous_x:], a[previous_a:])
            replacement = '(' + ' + '.join(parts) + ')'
            if len(replacement.encode()) >= source.span(node)[1] - source.span(node)[0]:
                continue
            edits.append((*source.span(node), replacement))
            stats[field + '_selects_factored'] += 1
            stats[field + '_shared_items'] += sum(block.size for block in common)
    return source.rewrite(edits)


def join_lists(text, stats):
    source = Source(text)
    edits = []
    class Join(ast.NodeVisitor):
        def __init__(self, field, indent):
            self.field = field
            self.indent = indent

        def visit_BinOp(self, node):
            if (isinstance(node.op, ast.Add) and isinstance(node.left, ast.List)
                    and isinstance(node.right, ast.List)
                    and safe_expression(node) and '#' not in source.text(node)):
                edits.append((*source.span(node),
                              render_list(source, node.left.elts + node.right.elts, self.indent)))
                stats[self.field + '_adjacent_lists_joined'] += 1
            else:
                self.generic_visit(node)
    for field, value, indent, _ in native_values(source):
        Join(field, indent).visit(value)
    return source.rewrite(edits)


def readable_names(text, spec_name, stats):
    source = Source(text)
    constants = eligible_constants(source)
    reserved = {node.id for node in ast.walk(source.module) if isinstance(node, ast.Name)}
    edits = []
    renamed = {}
    for old, (statement, field, uses) in constants.items():
        if len(uses) == 1:
            # Single-use support expression: one definition obscures its consumer.
            if not safe_expression(statement.value):
                continue
            edits.extend([(*source.span(statement), ''),
                          (*source.span(uses[0][0]), '(' + source.text(statement.value) + ')')])
            stats[field + '_constants_inlined'] += 1
            renamed[old] = None
            continue
        owners = [re.sub(r'__compile_\d+$', '', rule).strip('_') for _, rule, _ in uses]
        first = re.split(r'[^a-zA-Z0-9]+', owners[0])
        common = []
        for position, token in enumerate(first):
            if all(len(parts := re.split(r'[^a-zA-Z0-9]+', owner)) > position
                   and parts[position] == token for owner in owners):
                common.append(token)
            else:
                break
        # Avoid invented numeric identifiers. For truly unrelated consumers,
        # repository scope is useful only if its unnumbered name is available.
        scope = '_'.join(common) if common else spec_name.removeprefix('clickhouse_')
        if not scope or scope.isdigit():
            continue
        new = '_' + scope.upper() + '_' + field.upper()
        if new in reserved:
            stats[field + '_name_collision_kept'] += 1
            continue
        edits.append((*source.span(statement.targets[0]), new))
        edits.extend((*source.span(use), new) for use, _, _ in uses)
        renamed[old] = new
        reserved.add(new)
        stats[field + '_constants_named'] += 1
    return source.rewrite(edits), renamed


def source_globs(text, files, stats, examples):
    source = Source(text)
    rule_names = {kw.value.value for _, rule in source.rules() for kw in rule.keywords
                  if kw.arg == 'name' and isinstance(kw.value, ast.Constant)
                  and isinstance(kw.value.value, str)}
    edits = []

    def source_file(node):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            return None
        if node.value.startswith(('@', '//')):
            return None
        path = node.value.removeprefix(':')
        if (path not in files.files or path in rule_names
                or posixpath.splitext(path)[1] not in SOURCE_SUFFIXES):
            return None
        return path

    def patterns(path):
        directory, base = posixpath.split(path)
        prefix = directory + '/' if directory else ''
        stem, suffix = posixpath.splitext(base)
        yield prefix + '*' + suffix
        # A recursive glob replaces only a contiguous sorted file list and only
        # if no test or other uncompiled source exists beneath this directory.
        yield prefix + '**/*' + suffix
        for m in re.finditer(r'[_\-.]', stem):
            yield prefix + stem[:m.end()] + '*' + suffix
            yield prefix + '*' + stem[m.start():] + suffix

    def visit(node, indent, rule_name):
        if isinstance(node, ast.List):
            if '#' in source.text(node):
                return
            values = tuple(source_file(n) for n in node.elts)
            parts = []
            emitted = index = 0
            while index < len(values):
                filename = values[index]
                if filename is None:
                    index += 1
                    continue
                matches = []
                for pattern in set(patterns(filename)):
                    found = files.result((pattern,), ())
                    count = len(found)
                    if count >= 3 and count <= len(values) - index and values[index:index+count] == found:
                        matches.append((count, -len(pattern), pattern))
                if not matches:
                    index += 1
                    continue
                count, _, pattern = max(matches)
                if index > emitted:
                    parts.append(render_list(source, node.elts[emitted:index], indent))
                parts.append('glob(' + json.dumps([pattern]) + ')')
                stats['source_globs_added'] += 1
                stats['source_files_in_new_globs'] += count
                if len(examples) < 25:
                    examples.append({'rule': rule_name, 'pattern': pattern, 'sources': count})
                index += count
                emitted = index
            if parts:
                if emitted < len(values):
                    parts.append(render_list(source, node.elts[emitted:], indent))
                replacement = parts[0] if len(parts) == 1 else '(' + ' + '.join(parts) + ')'
                edits.append((*source.span(node), replacement))
        elif isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            visit(node.left, indent, rule_name)
            visit(node.right, indent, rule_name)
        elif function(node) == 'select' and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
            for branch in node.args[0].values:
                visit(branch, indent + 4, rule_name)

    for field, value, indent, rule_name in native_values(source):
        if field == 'srcs':
            visit(value, indent, rule_name)
    return source.rewrite(edits)


class Verify(ast.NodeTransformer):
    """Expand target fields per CPU, keep every other statement for comparison."""
    def __init__(self, cpu, files, bindings):
        self.cpu = cpu
        self.files = files
        self.binding_names = bindings
        self.bindings = {}
        self.field = None
        self.tu_counts = Counter()
        self.tu_rules = defaultdict(set)
        self.rules_verified = 0

    def visit_Module(self, node):
        result = []
        for statement in node.body:
            if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                    and isinstance(statement.targets[0], ast.Name)
                    and statement.targets[0].id in self.binding_names):
                self.bindings[statement.targets[0].id] = statement.value
            else:
                result.append(self.visit(statement))
        node.body = result
        return node

    def visit_Name(self, node):
        if isinstance(node.ctx, ast.Load) and node.id in self.bindings:
            return self.visit(copy.deepcopy(self.bindings[node.id]))
        return node

    def visit_Constant(self, node):
        # Local source labels :foo.cc and foo.cc refer to the same package file.
        if (self.field == 'srcs' and self.files is not None and isinstance(node.value, str)
                and node.value.startswith(':') and node.value[1:] in self.files.files):
            return ast.Constant(node.value[1:])
        return node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if self.field in ATTRIBUTES and isinstance(node.op, ast.Add):
            if isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                return ast.List(elts=node.left.elts + node.right.elts, ctx=ast.Load())
        return node

    def visit_Call(self, node):
        name = function(node)
        if name in NATIVE:
            self.rules_verified += 1
            for kw in node.keywords:
                old = self.field
                self.field = kw.arg if kw.arg in ATTRIBUTES else None
                kw.value = self.visit(kw.value)
                self.field = old
            rule_name = attrs(node)['name'].value
            srcs = attrs(node).get('srcs')
            if isinstance(srcs, ast.List):
                counts = Counter(n.value.removeprefix(':') for n in srcs.elts
                                 if isinstance(n, ast.Constant) and isinstance(n.value, str)
                                 and posixpath.splitext(n.value)[1] in SOURCE_SUFFIXES)
                for filename, count in counts.items():
                    if count > 1:
                        self.tu_counts[(rule_name, filename)] += count - 1
                    self.tu_rules[filename].add(rule_name)
            return node
        node = self.generic_visit(node)
        if (name == 'select' and self.field in ATTRIBUTES and len(node.args) == 1
                and not node.keywords and isinstance(node.args[0], ast.Dict)
                and all(isinstance(key, ast.Constant) for key in node.args[0].keys)):
            choice = {key.value: value for key, value in zip(node.args[0].keys, node.args[0].values)}
            if self.cpu in choice:
                return choice[self.cpu]
            if set(choice) <= set(CPUS) | {'//conditions:default'} and '//conditions:default' in choice:
                return choice['//conditions:default']
        if (name == 'glob' and self.field == 'srcs' and self.files is not None
                and len(node.args) == 1):
            options = {kw.arg: kw.value for kw in node.keywords}
            if set(options) <= {'exclude', 'allow_empty', 'exclude_directories'}:
                try:
                    patterns = ast.literal_eval(node.args[0])
                    exclude = ast.literal_eval(options['exclude']) if 'exclude' in options else []
                    excluded_dirs = ast.literal_eval(options['exclude_directories']) if 'exclude_directories' in options else True
                except (ValueError, TypeError):
                    return node
                if (excluded_dirs and isinstance(patterns, list) and isinstance(exclude, list)
                        and all(isinstance(p, str) for p in [*patterns, *exclude])):
                    return ast.List(elts=[ast.Constant(p) for p in self.files.result(patterns, exclude)],
                                    ctx=ast.Load())
        return node


def prove(before, after, files, renamed, path, stats):
    original_names = set(eligible_constants(Source(before)))
    names = original_names | {name for name in renamed.values() if name}
    for cpu in CPUS:
        old = Verify(cpu, files, names)
        new = Verify(cpu, files, names)
        old_tree = old.visit(ast.parse(before))
        new_tree = new.visit(ast.parse(after))
        old_key, new_key = ast_key(old_tree), ast_key(new_tree)
        if old_key != new_key:
            for a, b in zip(old_tree.body, new_tree.body):
                if ast_key(a) != ast_key(b):
                    print('BEFORE', ast.unparse(a)[:1500], file=sys.stderr)
                    print('AFTER', ast.unparse(b)[:1500], file=sys.stderr)
                    break
            raise ValueError(f'{path}, {cpu}: expanded BUILD changed')
        stats['native_rules_verified_two_cpus'] += old.rules_verified
        stats['extra_occurrences_in_one_tu_list_two_cpus'] += sum(old.tu_counts.values())
        stats['filenames_compiled_by_multiple_rules_two_cpus'] += sum(len(rules) > 1 for rules in old.tu_rules.values())


def compact(text, spec_name, files=None, stats=None, examples=None):
    """Compact one overlay; files is the immutable Bazel-package file inventory."""
    stats = Counter() if stats is None else stats
    examples = [] if examples is None else examples
    if files is not None and not isinstance(files, PackageFiles):
        files = PackageFiles(files)
    after = factor(text, stats)
    while True:
        joined = join_lists(after, stats)
        if joined == after:
            break
        after = joined
    if files is not None:
        after = source_globs(after, files, stats, examples)
    after, renamed = readable_names(after, spec_name, stats)
    prove(text, after, files, renamed, spec_name, stats)
    return after, renamed


def format_and_prove(root, before, after, spec_name, files, renamed):
    if before == after:
        return after
    formatted = subprocess.run(
        ['buildifier', '-mode=fix', '-type=build', '-config=' + str(root / '.buildifier.json')],
        input=after, text=True, stdout=subprocess.PIPE, check=True,
    ).stdout
    prove(before, formatted, files, renamed, spec_name, Counter())
    return formatted


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT_DEFAULT)
    parser.add_argument('--facts', type=Path, help='immutable scan_include_adjacency.py receipts')
    parser.add_argument('--preview-dir', type=Path)
    parser.add_argument('--report', type=Path)
    parser.add_argument('--files', nargs='*')
    parser.add_argument('--exclude', nargs='*', default=[])
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    if args.preview_dir and (args.preview_dir.resolve() == args.root.resolve()
                             or args.root.resolve() in args.preview_dir.resolve().parents):
        parser.error('--preview-dir must be outside the checked-out repository')
    repositories = json.loads((args.root / 'bazel/rules/contrib_repositories.json').read_text())
    specs = {value['name']: (owner, value) for owner, value in repositories.items()}
    stats = Counter()
    report = {}
    changes = []
    paths = sorted((args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))
    for path in paths:
        spec_name = path.name.removesuffix('.BUILD.bazel')
        if (args.files and spec_name not in args.files) or spec_name in args.exclude:
            continue
        if spec_name not in specs:
            stats['overlays_without_contrib_spec'] += 1
            continue
        owner, spec = specs[spec_name]
        files = load_package_files(args.facts, owner, spec)
        if files is None:
            stats['overlays_without_verified_pinned_receipt'] += 1
        counts = Counter()
        examples = []
        before = path.read_text()
        after, renamed = compact(before, spec_name, files, counts, examples)
        after = format_and_prove(args.root, before, after, spec_name, files, renamed)
        counts['bytes_removed'] += len(before.encode()) - len(after.encode())
        counts['overlays_checked'] += 1
        if after != before:
            counts['overlays_changed'] += 1
            changes.append((path, before, after))
            report[spec_name] = {'counts': dict(counts), 'renamed': renamed, 'new_globs': examples}
        stats.update(counts)
    if args.write:
        # The receipt, source-order and complete BUILD-statement checks for
        # both CPUs must pass for every file before changing the checkout.
        for path, before, _ in changes:
            if path.read_text() != before:
                raise RuntimeError(f'{path} changed while compact_external_options.py ran')
        for path, _, after in changes:
            path.write_text(after)
    if args.preview_dir:
        args.preview_dir.mkdir(parents=True, exist_ok=True)
        for path, _, after in changes:
            (args.preview_dir / path.name).write_text(after)
    final = {'total': dict(stats), 'files': report}
    if args.report:
        args.report.write_text(json.dumps(final, indent=2) + '\n')
    print(json.dumps(stats, indent=2))
    print('Changed overlays:', ', '.join(report))
    if not args.write:
        print('Verified. No repository files written.')


if __name__ == '__main__':
    main()
