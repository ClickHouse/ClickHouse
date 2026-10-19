"""Simplify generated globs against the source files in their Bazel package."""

import ast
from collections import Counter
from functools import cache
import json
import re

from initial_migration.compact_build import Source
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES


@cache
def pattern_expression(pattern):
    """Bazel glob supports * within a component and ** as a full component."""
    if not pattern or pattern.startswith('/') or pattern.endswith('/'):
        raise ValueError('Invalid Bazel glob: ' + pattern)
    parts = pattern.split('/')
    for part in parts:
        if not part or ('**' in part and part != '**'):
            raise ValueError('Invalid Bazel glob: ' + pattern)
    expression = ''
    for i, part in enumerate(parts):
        final = i == len(parts) - 1
        if part == '**':
            expression += '.*' if final else '(?:[^/]+/)*'
        else:
            expression += '[^/]*'.join(re.escape(piece) for piece in part.split('*'))
            if not final:
                expression += '/'
    return re.compile('^' + expression + '$')


class GlobFiles:
    def __init__(self, files):
        self.files = frozenset(files)
        self.matches = {}

    def matching(self, pattern):
        if pattern not in self.matches:
            if '*' not in pattern:
                self.matches[pattern] = frozenset({pattern} & self.files)
            else:
                expression = pattern_expression(pattern)
                self.matches[pattern] = frozenset(name for name in self.files if expression.fullmatch(name))
        return self.matches[pattern]

    def union(self, patterns):
        return frozenset().union(*(self.matching(pattern) for pattern in patterns))

    def result(self, patterns, excluded):
        return tuple(sorted(self.union(patterns) - self.union(excluded)))


def _literal_strings(node):
    try:
        value = ast.literal_eval(node)
    except (ValueError, TypeError):
        return None
    return value if isinstance(value, (list, tuple)) and all(isinstance(item, str) for item in value) else None


def _function(node, name):
    return isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == name


def simplify_globs(text, files, stats=None):
    """Preserve each glob's sorted files while removing empty include patterns.

    Bazel disallows empty glob patterns under the pinned configuration. Always
    require the package file inventory before expanding or simplifying globs.
    Globs with dynamic patterns or options remain untouched.
    """
    if files is None:
        raise ValueError('Cannot simplify globs without their pinned package files')
    stats = Counter() if stats is None else stats
    source = Source(text)
    globs = GlobFiles(files)
    edits = []
    for node in ast.walk(source.module):
        if not (_function(node, 'glob') or _function(node, 'header_glob')):
            continue
        stats['globs'] += 1
        if len(node.args) != 1:
            stats['dynamic_globs'] += 1
            continue
        patterns = _literal_strings(node.args[0])
        options = {kw.arg: kw.value for kw in node.keywords}
        excluded = _literal_strings(options['exclude']) if 'exclude' in options else []
        if patterns is None or excluded is None or any(
                key not in {'exclude', 'allow_empty', 'exclude_directories'} for key in options):
            stats['dynamic_globs'] += 1
            continue
        if 'exclude_directories' in options and not (
                isinstance(options['exclude_directories'], ast.Constant)
                and options['exclude_directories'].value in (True, 1)):
            # Directory-returning globs need a separate directory inventory.
            stats['directory_globs'] += 1
            continue
        header_glob = _function(node, 'header_glob')
        if header_glob:
            patterns = [root + '/*' + suffix for root in patterns for suffix in CC_HEADER_SUFFIXES]
            stats['header_macros'] += 1
        original_patterns, original_excluded = patterns, excluded
        original_files = globs.result(patterns, excluded)
        patterns = [pattern for pattern in patterns if globs.matching(pattern)]
        included = globs.union(patterns)
        excluded = [pattern for pattern in excluded if included & globs.matching(pattern)]
        if globs.result(patterns, excluded) != original_files:
            raise ValueError('Glob changed its sorted file result at line ' + str(node.lineno))
        stats['verified_globs'] += 1
        stats['empty_patterns_removed'] += len(original_patterns) - len(patterns)
        stats['unmatched_excludes_removed'] += len(original_excluded) - len(excluded)
        allow_empty = options.get('allow_empty')
        if isinstance(allow_empty, ast.Constant) and allow_empty.value is True:
            # An inventory-proven nonempty glob needs no allow_empty override.
            stats['default_allow_empty_removed'] += 1
            allow_empty = None
        parts = [json.dumps(patterns)]
        if excluded:
            parts.append('exclude = ' + json.dumps(excluded))
        if allow_empty is not None:
            parts.append('allow_empty = ' + source.text(allow_empty))
        if 'exclude_directories' in options:
            parts.append('exclude_directories = ' + source.text(options['exclude_directories']))
        # Preserve an empty fixed input set as an ordinary list. Neither an
        # empty pattern nor a glob whose excludes remove all files is valid.
        replacement = 'glob(' + ', '.join(parts) + ')' if original_files else '[]'
        if replacement != source.text(node):
            edits.append((*source.span(node), replacement))
    if stats['header_macros']:
        for statement in source.module.body:
            if not isinstance(statement, ast.Expr) or not _function(statement.value, 'load'):
                continue
            load = statement.value
            if not any(isinstance(arg, ast.Constant) and arg.value == 'header_glob' for arg in load.args[1:]):
                continue
            kept = [arg for arg in load.args if not isinstance(arg, ast.Constant) or arg.value != 'header_glob']
            if len(kept) == 1 and not load.keywords:
                replacement = ''
            else:
                parts = [source.text(arg) for arg in kept]
                parts.extend(kw.arg + ' = ' + source.text(kw.value) for kw in load.keywords)
                replacement = 'load(' + ', '.join(parts) + ')'
            edits.append((*source.span(statement), replacement))
    return source.rewrite(edits)
