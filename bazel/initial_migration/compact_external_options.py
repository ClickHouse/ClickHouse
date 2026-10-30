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
import io
import json
from pathlib import Path
import posixpath
import re
import sys
import subprocess
import tokenize

ROOT_DEFAULT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT_DEFAULT / 'bazel'))
from initial_migration.compact_build import (  # noqa: E402
    Source, ast_key, render_list, safe_expression, select_dictionary,
)
from initial_migration.simplify_globs import GlobFiles, pattern_expression  # noqa: E402

ATTRIBUTES = ('copts', 'local_includes', 'includes', 'srcs')
HEADER_ATTRIBUTES = (*ATTRIBUTES, 'hdrs', 'textual_hdrs')
CC_COMPILERS = {'cc_library', 'ch_cc_library', 'cc_binary', 'cc_test'}
NATIVE = CC_COMPILERS | {'llvm_runtime_objects', 'nasm_objects'}
CPUS = ('@platforms//cpu:x86_64', '@platforms//cpu:aarch64')
SOURCE_SUFFIXES = {'.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM'}


# ClickHouse's CMake source groups. Each glob must expand to the existing
# consecutive srcs in order; unmatched source groups keep their captured list.
ORDERED_COMPILE_GLOBS = {
    'clickhouse_cassandra': [
        (['src/*.cpp'], ['src/get_time-mac.cpp', 'src/get_time-win.cpp']),
    ],
    'clickhouse_google_benchmark': [
        (['src/*.cc'], ['src/benchmark_main.cc']),
    ],
    'clickhouse_grpc': [
        (['src/core/credentials/**/*.cc'], []),
        (['src/core/resolver/**/*.cc'], []),
    ],
    'clickhouse_s2geometry': [
        (['src/s2/*.cc', 'src/s2/base/*.cc', 'src/s2/util/**/*.cc'],
         ['src/s2/**/*_test.cc', 'src/s2/*_testing.cc', 'src/s2/gmock_matchers.cc']),
    ],
    'clickhouse_vectorscan': [
        (['src/*.c'], ['src/dispatcher.c']),
        (['src/*/*.c'], []),
        (['src/' + directory + '/*.cpp' for directory in (
            'hwlm', 'nfa', 'nfagraph', 'parser', 'rose', 'smallwrite', 'som', 'util',
        )], ['src/*/*dump*.cpp']),
    ],
}


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
    # The scanner verifies the compressed archive SHA256 before publishing a
    # v3 receipt. inventory_checked only records comparison with an older
    # inventory, which is not available for every pinned archive.
    stats = data.get('stats', {})
    inventory = set(data['inventory'])
    scanned = set(data['files'])
    unscanned = {item for items in data['unscanned'].values() for item in items}
    if (scanned & unscanned or scanned | unscanned != inventory
            or stats.get('inventory_files') != len(inventory)
            or stats.get('compressed_bytes', 0) <= 0):
        raise ValueError(f'{receipt}: source inventory receipt inconsistent')
    # scan_include_adjacency validated every nested BUILD against the removal list.
    remaining_builds = {s for s in inventory if posixpath.basename(s) in {'BUILD', 'BUILD.bazel'}}
    if remaining_builds - {'BUILD.bazel'}:
        raise ValueError(f'{receipt}: remaining nested BUILD files: {sorted(remaining_builds)}')
    inventory.add('BUILD.bazel')
    return PackageFiles(inventory)


def eligible_constants(source, attributes=ATTRIBUTES, kinds=NATIVE):
    """Only constants whose every read is in the selected native attributes."""
    allowed_reads = defaultdict(list)
    for _, rule in source.rules():
        if function(rule) in kinds:
            name = attrs(rule).get('name')
            if not isinstance(name, ast.Constant):
                continue
            for attr in rule.keywords:
                if attr.arg not in attributes:
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
            for field in attributes:
                if (name.startswith('_GENERATED_' + field.upper() + '_') and all_reads[name]
                        and {id(item) for item in all_reads[name]}
                        == {id(item[0]) for item in allowed_reads[name]}
                        and all(item[2] == field for item in allowed_reads[name])):
                    result[name] = (statement, field, allowed_reads[name])
    return result


def native_values(source, attributes=ATTRIBUTES, kinds=NATIVE):
    for name, (statement, field, _) in eligible_constants(source, attributes, kinds).items():
        yield field, statement.value, 0, name
    for _, rule in source.rules():
        if function(rule) not in kinds:
            continue
        for attr in rule.keywords:
            if attr.arg in attributes:
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


def string_value(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


def dictionary_pairs(value):
    if not isinstance(value, ast.Dict):
        return None
    keys = [string_value(key) for key in value.keys]
    # Neither repeated keys nor dict unpacking has an unambiguous literal
    # insertion order to use in the template proof.
    if None in keys or len(keys) != len(set(keys)):
        return None
    return list(zip(value.keys, value.values))


def factor_substitutions(text, spec_name, stats):
    """Share literal initial substitutions without moving replacements."""
    source = Source(text)
    reserved = {n.id for n in ast.walk(source.module) if isinstance(n, ast.Name)}
    comments = [item for item in tokenize.generate_tokens(io.StringIO(text).readline)
                if item.type == tokenize.COMMENT]
    edits = []
    names = set()

    def token(value):
        return re.sub(r'[^a-zA-Z0-9]+', '_', value).strip('_').upper()

    def dictionary(values, indent):
        pad = ' ' * indent
        if not values:
            return '{}'
        return ('{\n' + ''.join(pad + '    ' + source.text(key) + ': '
                               + source.text(value) + ',\n' for key, value in values)
                + pad + '}')

    for statement, rule in source.rules():
        if function(rule) != 'expand_template':
            continue
        fields = attrs(rule)
        name, output = string_value(fields.get('name')), string_value(fields.get('out'))
        selected = fields.get('substitutions')
        if (function(selected) != 'select' or len(selected.args) != 1 or selected.keywords
                or name is None or output is None or 'template' not in fields):
            continue
        choices = dictionary_pairs(selected.args[0])
        if choices is None or len(choices) != 2 or {string_value(k) for k, _ in choices} != set(CPUS):
            continue
        branches = [dictionary_pairs(choice) for _, choice in choices]
        if any(branch is None for branch in branches):
            continue
        common = 0
        while common < min(map(len, branches)):
            pairs = [branch[common] for branch in branches]
            if not all(ast_key(key) == ast_key(pairs[0][0])
                       and ast_key(value) == ast_key(pairs[0][1]) for key, value in pairs):
                break
            # Do not hoist calls from configured values to file-loading time.
            if string_value(pairs[0][1]) is None:
                break
            common += 1
        # Three entries produce as many BUILD lines as the original branches.
        if common < 4 or any(selected.lineno <= c.start[0] <= selected.end_lineno for c in comments):
            continue
        owner = spec_name.removeprefix('clickhouse_')
        parent, filename = posixpath.split(output)
        parent = posixpath.basename(parent)
        if token(parent) in {token(owner), token(owner + '-cmake'), 'INCLUDE', 'INCLUDE_PUBLIC'}:
            parent = ''
        constant = '_' + '_'.join(token(p) for p in (owner, parent, filename, 'substitutions') if p)
        if constant in reserved:
            continue
        indent = next(kw.col_offset for kw in rule.keywords if kw.arg == 'substitutions')
        declaration = constant + ' = ' + dictionary(branches[0][:common], 0) + '\n\n'
        rows = []
        for (key, _), branch in zip(choices, branches):
            rows.append(' ' * (indent + 4) + source.text(key) + ': ' + constant
                        + ' | ' + dictionary(branch[common:], indent + 4) + ',\n')
        replacement = 'select({\n' + ''.join(rows) + ' ' * indent + '})'
        start, end = source.span(selected)
        if end - start - len(replacement.encode()) - len(declaration.encode()) < 64:
            continue
        start_rule, _ = source.span(statement)
        edits.extend(((start_rule, start_rule, declaration), (start, end, replacement)))
        names.add(constant)
        reserved.add(constant)
        stats['template_selects_factored'] += 1
        stats['template_initial_substitutions_shared'] += common
    return source.rewrite(edits), names


class OrderedTemplates(ast.NodeTransformer):
    """Expand only introduced constants and compare ordered substitutions."""
    def __init__(self, cpu, introduced):
        self.cpu = cpu
        self.introduced = introduced
        self.definitions = {}
        self.in_substitutions = False
        self.ordered_entries = {}

    def visit_Module(self, node):
        statements = []
        for statement in node.body:
            if (isinstance(statement, ast.Assign) and len(statement.targets) == 1
                    and isinstance(statement.targets[0], ast.Name)
                    and statement.targets[0].id in self.introduced):
                self.definitions[statement.targets[0].id] = statement.value
            else:
                statements.append(self.visit(statement))
        node.body = statements
        return node

    def visit_Name(self, node):
        if (self.in_substitutions and isinstance(node.ctx, ast.Load)
                and node.id in self.definitions):
            return self.visit(copy.deepcopy(self.definitions[node.id]))
        return node

    def visit_Call(self, node):
        if function(node) == 'expand_template':
            name = string_value(attrs(node).get('name'))
            for kw in node.keywords:
                if kw.arg == 'substitutions':
                    self.in_substitutions = True
                    kw.value = self.visit(kw.value)
                    self.in_substitutions = False
                    entries = dictionary_pairs(kw.value)
                    if entries is not None:
                        self.ordered_entries[name] = [
                            (string_value(key), ast_key(value)) for key, value in entries
                        ]
            return node
        node = self.generic_visit(node)
        if (self.in_substitutions and function(node) == 'select'
                and len(node.args) == 1 and not node.keywords):
            pairs = dictionary_pairs(node.args[0])
            if (pairs is not None and self.cpu in CPUS
                    and {string_value(k) for k, _ in pairs} == set(CPUS)):
                return next(value for key, value in pairs if string_value(key) == self.cpu)
        return node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if self.in_substitutions and isinstance(node.op, ast.BitOr):
            left, right = dictionary_pairs(node.left), dictionary_pairs(node.right)
            if left is not None and right is not None:
                left_keys = {string_value(k) for k, _ in left}
                if left_keys & {string_value(k) for k, _ in right}:
                    raise ValueError('Template dictionaries overlap; insertion order not proven')
                return ast.Dict(keys=[k for k, _ in left + right],
                                values=[v for _, v in left + right])
        return node


def source_globs(text, files, stats, examples, *, headers=False, source_tests=False, groups=()):
    """Replace contiguous exact-order files; header mode never moves attributes."""
    source = Source(text)
    if headers:
        # Use the native package header classes; do not treat .proto, .rl or
        # other generator inputs as headers merely because they appear in srcs.
        from initial_migration.first_party_packages import TEXTUAL
        from initial_migration.repository_inputs import CC_HEADER_SUFFIXES
        suffixes = set(CC_HEADER_SUFFIXES) | set(TEXTUAL)
        attributes = HEADER_ATTRIBUTES
        kinds = CC_COMPILERS
        fields = {'srcs', 'hdrs', 'textual_hdrs'}
    else:
        suffixes = SOURCE_SUFFIXES
        attributes = ATTRIBUTES
        kinds = NATIVE
        fields = {'srcs'}
    rule_names = {kw.value.value for _, rule in source.rules() for kw in rule.keywords
                  if kw.arg == 'name' and isinstance(kw.value, ast.Constant)
                  and isinstance(kw.value.value, str)}
    ordered_groups = [
        (files.result(patterns, exclude), tuple(patterns), tuple(exclude))
        for patterns, exclude in groups if all(files.matching(p) for p in patterns)
    ]
    edits = []

    def source_file(node):
        if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
            return None
        if node.value.startswith(('@', '//')):
            return None
        path = node.value.removeprefix(':')
        if (path not in files.files or path in rule_names
                or posixpath.splitext(path)[1] not in suffixes):
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
        if headers:
            ancestor = posixpath.dirname(directory)
            while ancestor:
                yield ancestor + '/**/*' + suffix
                ancestor = posixpath.dirname(ancestor)
            yield '**/*' + suffix
        else:
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
                matches = [
                    (len(names), -sum(map(len, includes + exclude)), includes, exclude)
                    for names, includes, exclude in ordered_groups
                    if names and values[index:index + len(names)] == names
                ]
                for pattern in set(patterns(filename)):
                    excluded = ()
                    if source_tests and not headers:
                        suffix = posixpath.splitext(filename)[1]
                        tests = tuple('**/' + name + suffix for name in (
                            '*_test', '*_benchmark', 'test_*',
                        ))
                        matched = files.matching(pattern)
                        excluded = tuple(p for p in tests if matched & files.matching(p))
                    found = files.result((pattern,), excluded)
                    count = len(found)
                    # An excluded glob takes more lines than a plain glob.
                    minimum = 5 + len(excluded) if excluded else 3
                    if count >= minimum and count <= len(values) - index and values[index:index+count] == found:
                        if headers:
                            # Prefer include/Poco/Net/*.h over **/*.h when
                            # both currently name the same files.
                            matches.append((count, pattern.split('*', 1)[0].count('/'),
                                            '**' not in pattern, -len(pattern), pattern))
                        else:
                            expression_size = len(pattern) + sum(map(len, excluded))
                            matches.append((count, -expression_size, (pattern,), excluded))
                if not matches:
                    index += 1
                    continue
                if headers:
                    count, *_, pattern = max(matches)
                    includes, excluded = (pattern,), ()
                else:
                    count, _, includes, excluded = max(matches)
                if index > emitted:
                    parts.append(render_list(source, node.elts[emitted:index], indent))
                options = ', exclude = ' + json.dumps(excluded) if excluded else ''
                parts.append('glob(' + json.dumps(includes) + options + ')')
                category = 'header' if headers else 'source'
                stats[category + '_globs_added'] += 1
                stats[category + '_files_in_new_globs'] += count
                if len(examples) < 25:
                    examples.append({'rule': rule_name, 'pattern': includes[0] if len(includes) == 1 else list(includes), 'sources': count})
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

    for field, value, indent, rule_name in native_values(source, attributes, kinds):
        if field in fields:
            visit(value, indent, rule_name)
    result = source.rewrite(edits)
    if headers:
        # Header globs are evaluated when the BUILD loads, including files
        # where the header pass did not replace any literal list.
        prove(text, result, files, {}, 'native header globs', stats, headers=True)
    return result


def compact_source_glob_runs(text, files, stats, examples, groups=()):
    """Reconsider adjacent srcs lists and existing globs in source ordinal order."""
    source = Source(text)
    edits = []

    def source_files(node):
        if isinstance(node, ast.List):
            return tuple(source.text(item) for item in node.elts)
        if function(node) != 'glob' or len(node.args) != 1:
            return None
        options = attrs(node)
        if set(options) - {'exclude', 'allow_empty', 'exclude_directories'}:
            return None
        try:
            patterns = ast.literal_eval(node.args[0])
            excluded = ast.literal_eval(options['exclude']) if 'exclude' in options else []
            allow_empty = ast.literal_eval(options['allow_empty']) if 'allow_empty' in options else False
            exclude_dirs = ast.literal_eval(options['exclude_directories']) if 'exclude_directories' in options else True
        except (ValueError, TypeError):
            return None
        if (not exclude_dirs or not isinstance(patterns, list) or not isinstance(excluded, list)
                or not all(isinstance(p, str) for p in [*patterns, *excluded])):
            return None
        filenames = files.result(patterns, excluded)
        if not allow_empty and (not filenames or any(not files.matching(p) for p in patterns)):
            raise ValueError(f'strict source glob has no match: {patterns}, exclude = {excluded}')
        if any(posixpath.splitext(path)[1] not in SOURCE_SUFFIXES for path in filenames):
            return None
        return tuple(json.dumps(path) for path in filenames)

    def parts(node):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return parts(node.left) + parts(node.right)
        return [node]

    def visit(node, indent, rule_name):
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            inputs = parts(node)
            pending, output = [], []
            changed = False

            def emit():
                nonlocal changed
                original = ' + '.join(source.text(p) for p, _ in pending)
                if (len(pending) > 1 and any(function(p) == 'glob' for p, _ in pending)
                        and '#' not in original):
                    filenames = [name for _, values in pending for name in values]
                    literal = '[\n' + ''.join(
                        ' ' * (indent + 4) + value + ',\n' for value in filenames
                    ) + ' ' * indent + ']'
                    fragment = f'cc_library(name = {json.dumps(rule_name)},\n    srcs = {literal},\n)\n'
                    result = source_globs(fragment, files, Counter(), [], source_tests=True, groups=groups)
                    compacted = Source(result)
                    call = next(compacted.rules())[1]
                    replacement = compacted.text(attrs(call)['srcs'])
                    old_key = ast_key(ast.parse(original, mode='eval'))
                    new_key = ast_key(ast.parse(replacement, mode='eval'))
                    if old_key != new_key and len(original.encode()) - len(replacement.encode()) >= 64:
                        output.append(replacement)
                        stats['joined_srcs_groups'] += 1
                        changed = True
                        pending.clear()
                        return
                output.extend(source.text(p) for p, _ in pending)
                pending.clear()

            for item in inputs:
                filenames = source_files(item)
                if filenames is None:
                    emit()
                    output.append(source.text(item))
                else:
                    pending.append((item, filenames))
            emit()
            if changed:
                edits.append((*source.span(node), '(' + ' + '.join(output) + ')'))
            else:
                for item in inputs:
                    if function(item) == 'select':
                        visit(item, indent, rule_name)
        elif function(node) == 'select' and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
            for value in node.args[0].values:
                visit(value, indent + 4, rule_name)

    for field, value, indent, rule_name in native_values(source):
        if field == 'srcs':
            visit(value, indent, rule_name)
    return source.rewrite(edits)


def external_source_globs(text, files, stats, examples, groups=()):
    """Compact external TU lists, retaining every original file's ordinal."""
    after = text
    while True:
        before = after
        after = source_globs(before, files, stats, examples, source_tests=True, groups=groups)
        after = compact_source_glob_runs(after, files, stats, examples, groups)
        if after == before:
            return after


class Verify(ast.NodeTransformer):
    """Expand target fields per CPU, keep every other statement for comparison."""
    def __init__(self, cpu, files, bindings, *, headers=False):
        self.cpu = cpu
        self.files = files
        self.binding_names = bindings
        self.bindings = {}
        self.field = None
        self.attributes = HEADER_ATTRIBUTES if headers else ATTRIBUTES
        self.kinds = CC_COMPILERS if headers else NATIVE
        self.file_fields = {'srcs', 'hdrs', 'textual_hdrs'} if headers else {'srcs'}
        self.check_header_globs = headers
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
        if (self.field in self.file_fields and self.files is not None and isinstance(node.value, str)
                and node.value.startswith(':') and node.value[1:] in self.files.files):
            return ast.Constant(node.value[1:])
        return node

    def visit_BinOp(self, node):
        node = self.generic_visit(node)
        if self.field in self.attributes and isinstance(node.op, ast.Add):
            if isinstance(node.left, ast.List) and isinstance(node.right, ast.List):
                return ast.List(elts=node.left.elts + node.right.elts, ctx=ast.Load())
        return node

    def visit_Call(self, node):
        name = function(node)
        if name in self.kinds:
            self.rules_verified += 1
            for kw in node.keywords:
                old = self.field
                self.field = kw.arg if kw.arg in self.attributes else None
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
        if (name == 'select' and self.field in self.attributes and len(node.args) == 1
                and not node.keywords and isinstance(node.args[0], ast.Dict)
                and all(isinstance(key, ast.Constant) for key in node.args[0].keys)):
            choice = {key.value: value for key, value in zip(node.args[0].keys, node.args[0].values)}
            if self.cpu in choice:
                return choice[self.cpu]
            if set(choice) <= set(CPUS) | {'//conditions:default'} and '//conditions:default' in choice:
                return choice['//conditions:default']
        if (name == 'glob' and self.field in self.file_fields and self.files is not None
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
                    matched = self.files.result(patterns, exclude)
                    if self.check_header_globs:
                        allow_empty = ast.literal_eval(options['allow_empty']) if 'allow_empty' in options else False
                        if not allow_empty:
                            empty_patterns = [pattern for pattern in patterns if not self.files.matching(pattern)]
                            if empty_patterns or not matched:
                                raise ValueError('strict glob has no match: ' + repr(empty_patterns or patterns))
                    return ast.List(elts=[ast.Constant(p) for p in matched], ctx=ast.Load())
        return node


def prove(before, after, files, renamed, path, stats, *, headers=False):
    attributes = HEADER_ATTRIBUTES if headers else ATTRIBUTES
    kinds = CC_COMPILERS if headers else NATIVE
    original_names = set(eligible_constants(Source(before), attributes, kinds))
    # New template names map to themselves; readable_names() only renames
    # _GENERATED_ native attributes, so it never produces a self-mapping.
    template_names = {key for key, value in renamed.items() if key == value}
    names = original_names | {name for name in renamed.values() if name and name not in template_names}
    cpus = (*CPUS, '//conditions:default')
    for cpu in cpus:
        old_templates = OrderedTemplates(cpu, template_names)
        new_templates = OrderedTemplates(cpu, template_names)
        old = Verify(cpu, files, names, headers=headers)
        new = Verify(cpu, files, names, headers=headers)
        old_tree = old.visit(old_templates.visit(ast.parse(before)))
        new_tree = new.visit(new_templates.visit(ast.parse(after)))
        if old_templates.ordered_entries != new_templates.ordered_entries:
            raise ValueError(f'{path}, {cpu}: ordered template substitutions changed')
        old_key, new_key = ast_key(old_tree), ast_key(new_tree)
        if old_key != new_key:
            for a, b in zip(old_tree.body, new_tree.body):
                if ast_key(a) != ast_key(b):
                    print('BEFORE', ast.unparse(a)[:1500], file=sys.stderr)
                    print('AFTER', ast.unparse(b)[:1500], file=sys.stderr)
                    break
            raise ValueError(f'{path}, {cpu}: expanded BUILD changed')
        count_key = 'three_configs'
        stats['native_rules_verified_' + count_key] += old.rules_verified
        stats['template_ordered_items_verified_' + count_key] += sum(
            len(items) for items in old_templates.ordered_entries.values())
        stats['extra_occurrences_in_one_tu_list_' + count_key] += sum(old.tu_counts.values())
        stats['filenames_compiled_by_multiple_rules_' + count_key] += sum(
            len(rules) > 1 for rules in old.tu_rules.values())


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
        after = external_source_globs(after, files, stats, examples, ORDERED_COMPILE_GLOBS.get(spec_name, ()))
    after, renamed = readable_names(after, spec_name, stats)
    after, template_names = factor_substitutions(after, spec_name, stats)
    renamed.update({name: name for name in template_names})
    prove(text, after, files, renamed, spec_name, stats)
    return after, renamed


def format_and_prove(root, before, after, spec_name, files, renamed, *, headers=False):
    if before == after:
        return after
    formatted = subprocess.run(
        ['buildifier', '-mode=fix', '-type=build', '-config=' + str(root / '.buildifier.json')],
        input=after, text=True, stdout=subprocess.PIPE, check=True,
    ).stdout
    prove(before, formatted, files, renamed, spec_name, Counter(), headers=headers)
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
