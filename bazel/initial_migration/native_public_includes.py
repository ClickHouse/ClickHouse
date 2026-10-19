#!/usr/bin/env python3
"""Publish pinned include directories through existing native dependencies.

Only a consumer's existing public deps or its own implementation_deps can
supply an include. Publish a new include on a source owner only when that
owner already publishes headers in the same pinned directory. This does not
change dependencies, source lists, own private includes, or generated roots.
"""

import argparse
import ast
from collections import Counter, defaultdict
from functools import cache
import json
from pathlib import Path
import posixpath
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, ast_key
from initial_migration.native_cc_inputs import Sources, SPECIAL_REPOS
from initial_migration.prune_inherited_headers import (
    ARCHES, CC_COMPILERS, CC_PROVIDERS, Document, Graph, label,
)
from initial_migration.repository_inputs import REPOSITORIES, ROOT


def directory(node, doc, constants=()):
    """Interpret only the repository path expressions emitted by migration."""
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name) and node.id in doc.constants and node.id not in constants:
        return directory(doc.constants[node.id], doc, (*constants, node.id))
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left, right = directory(node.left, doc, constants), directory(node.right, doc, constants)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.Call) and not node.keywords:
        if getattr(node.func, 'id', '') == 'repository_path' and len(node.args) == 1:
            return directory(node.args[0], doc, constants)
        if (getattr(node.func, 'id', '') == 'repository_local_include' and len(node.args) in {1, 2}
                and (len(node.args) == 1 or directory(node.args[1], doc, constants) == doc.owner)):
            return directory(node.args[0], doc, constants)
    return None


def list_elements(node, doc, cpu, constants=()):
    """Return guaranteed list elements for one CPU, keeping path expressions."""
    if isinstance(node, ast.Name) and node.id in doc.constants and node.id not in constants:
        return list_elements(doc.constants[node.id], doc, cpu, (*constants, node.id))
    if isinstance(node, (ast.List, ast.Tuple)):
        return node.elts
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return list_elements(node.left, doc, cpu, constants) + list_elements(node.right, doc, cpu, constants)
    if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
            and len(node.args) == 1 and not node.keywords and isinstance(node.args[0], ast.Dict)):
        choices = node.args[0]
        if not all(isinstance(key, ast.Constant) and isinstance(key.value, str) for key in choices.keys):
            return []
        table = {key.value: value for key, value in zip(choices.keys, choices.values)}
        if set(table) <= {'@platforms//cpu:' + arch for arch in ARCHES} | {'//conditions:default'}:
            branch = table.get('@platforms//cpu:' + cpu, table.get('//conditions:default'))
            return list_elements(branch, doc, cpu, constants)
        branches = [list_elements(value, doc, cpu, constants) for value in table.values()]
        if branches:
            common = set.intersection(*(set(ast_key(item) for item in branch) for branch in branches))
            return [item for item in branches[0] if ast_key(item) in common]
    return []


class IncludeGraph(Graph):
    def __init__(self, documents):
        super().__init__(documents)
        self.includes = defaultdict(set)
        self.repos = {owner: '@' + spec['name'] for owner, spec in REPOSITORIES.items()}

    @cache
    def visible(self, target, cpu):
        """CcInfo visible during target compilation, including own private deps."""
        pending = list(self.dependencies(target, cpu))
        if target in self.rules:
            doc, kind, attrs = self.rules[target]
            if kind in CC_COMPILERS:
                pending.extend(doc.labels(attrs.get('implementation_deps'), cpu))
        seen = {target}
        while pending:
            dependency = pending.pop()
            if dependency in seen:
                continue
            seen.add(dependency)
            if self.rules.get(dependency, (None, None, None))[1] in CC_PROVIDERS | {'alias'}:
                # Dependencies' implementation_deps are private to their compilation.
                pending.extend(self.dependencies(dependency, cpu))
        return frozenset(seen - {target})

    @cache
    def own_public_files(self, target, cpu):
        """Source headers owned by target's repository, including its public deps."""
        doc, kind, _ = self.rules[target]
        prefix = doc.repo + '//:'
        files = self.own_headers(target, cpu) | self.inherited(target, cpu)
        return frozenset(file for file in files if file.startswith(prefix))

    @cache
    def original_includes(self, target, cpu):
        if target not in self.rules:
            return frozenset()
        doc, kind, attrs = self.rules[target]
        if kind not in CC_PROVIDERS:
            return frozenset()
        result = set()
        for node in list_elements(attrs.get('includes'), doc, cpu):
            value = directory(node, doc)
            if value is None or value.startswith('$(GENDIR)/'):
                continue
            if isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'repository_local_include':
                result.add(posixpath.normpath(value))
            else:
                result.add(posixpath.normpath(posixpath.join(doc.owner or '', value)))
        return frozenset(result)

    def provider(self, target, value, cpu):
        """Return an existing reachable owner and exact relative source include."""
        doc, _, _ = self.rules[target]
        original = directory(value, doc)
        if original is None or original.startswith('$(GENDIR)/'):
            return None
        absolute = posixpath.normpath(original)
        owner = '/'.join(absolute.split('/')[:2])
        repo = self.repos.get(owner)
        if repo == doc.repo:
            # Even if another target in this package publishes the same
            # directory, this task does not change its own private includes.
            return None
        visible = self.visible(target, cpu)
        # This also handles previously reviewed root includes such as
        # crypto's contrib/openssl-cmake/common/include.
        existing = [ref for ref in visible if absolute in self.original_includes(ref, cpu)]
        if existing:
            return min(existing), None
        if repo is None or repo == doc.repo or repo in SPECIAL_REPOS:
            return None
        relative = absolute.removeprefix(owner).lstrip('/') or '.'
        # A directory in the archive named config/ or gen/ is not evidence
        # that a generated include directory is declared at the same location.
        if relative.startswith(('config/', 'gen/')) or relative in {'config', 'gen'}:
            return None
        prefix = repo + '//:' + (relative.rstrip('/') + '/' if relative != '.' else '')
        choices = []
        for ref in visible:
            if ref not in self.rules:
                continue
            provider_doc, kind, attrs = self.rules[ref]
            if provider_doc.repo != repo or kind not in CC_PROVIDERS:
                continue
            name = ref.partition('//:')[2]
            # address_sorting also captures all grpc headers but does not own
            # the grpc API. Its glob cannot establish public include ownership.
            if (repo == '@clickhouse_grpc' and relative == 'include'
                    and name not in {'grpc', 'grpc++', 'grpc_unsecure', 'grpc++_unsecure'}):
                continue
            published = self.own_public_files(ref, cpu)
            if not any(header.startswith(prefix) for header in published):
                continue
            # Prefer named header APIs and logical library wrappers over
            # generated compile groups. The provider must already be visible.
            choices.append((not name.endswith('_headers'), '__compile_' in name, len(published), ref))
        if not choices:
            return None
        provider = min(choices)[-1]
        return provider, relative


class IncludeRewrite:
    def __init__(self, doc, graph):
        self.doc, self.graph = doc, graph
        self.sites = {}
        self.visited = set()
        self.stats = Counter()

    def walk(self, node, target, arches, allowed, constants=()):
        if node is None:
            return
        if isinstance(node, ast.Name) and node.id in self.doc.constants and node.id not in constants:
            key = (node.id, target, arches, allowed)
            if key not in self.visited:
                self.visited.add(key)
                self.walk(self.doc.constants[node.id], target, arches, allowed, (*constants, node.id))
            return
        if isinstance(node, (ast.List, ast.Tuple)):
            for item in node.elts:
                if (isinstance(item, ast.Call)
                        and getattr(item.func, 'id', '') == 'repository_local_include'):
                    site = self.sites.setdefault(id(item), {
                        'list': node, 'item': item, 'eligible': True, 'providers': set(), 'uses': set(),
                    })
                    if not allowed or not arches or target is None:
                        site['eligible'] = False
                        continue
                    for cpu in arches:
                        site['uses'].add((target, cpu, posixpath.normpath(directory(item, self.doc) or '')))
                        supplied = self.graph.provider(target, item, cpu)
                        if supplied is None:
                            site['eligible'] = False
                        elif supplied[1] is not None:
                            site['providers'].add((supplied[0], cpu, supplied[1]))
                else:
                    self.walk(item, target, arches, allowed, constants)
            return
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            self.walk(node.left, target, arches, allowed, constants)
            self.walk(node.right, target, arches, allowed, constants)
            return
        if (isinstance(node, ast.Call) and getattr(node.func, 'id', '') == 'select'
                and len(node.args) == 1 and isinstance(node.args[0], ast.Dict)):
            options = node.args[0]
            keys = {key.value for key in options.keys if isinstance(key, ast.Constant)}
            known = len(keys) == len(options.keys) and keys <= {
                '@platforms//cpu:' + cpu for cpu in ARCHES
            } | {'//conditions:default'}
            for key, branch in zip(options.keys, options.values):
                selected = arches
                if known:
                    selected = tuple(cpu for cpu in arches
                                     if key.value == '@platforms//cpu:' + cpu
                                     or key.value == '//conditions:default'
                                     and '@platforms//cpu:' + cpu not in keys)
                self.walk(branch, target, selected, allowed, constants)
            return
        # Unknown function arguments cannot establish Bazel include semantics.
        for child in ast.iter_child_nodes(node):
            self.walk(child, target, arches, False, constants)

    def collect(self):
        for stmt in self.doc.source.module.body:
            if isinstance(stmt, ast.Assign):
                continue
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                call = stmt.value
                attrs = {kw.arg: kw.value for kw in call.keywords}
                name, language = attrs.get('name'), attrs.get('language')
                native = (getattr(call.func, 'id', '') in {'ch_cc_library', 'cc_library', 'cc_binary', 'cc_test'}
                          and isinstance(name, ast.Constant) and isinstance(name.value, str)
                          and (language is None or isinstance(language, ast.Constant)
                               and language.value in {'C', 'CXX'}))
                target = label(':' + name.value, self.doc.repo) if native else None
                for key, value in attrs.items():
                    self.walk(value, target, ARCHES, native and key == 'local_includes')
                for arg in call.args:
                    self.walk(arg, target, ARCHES, False)
            else:
                self.walk(stmt, None, ARCHES, False)
        for site in self.sites.values():
            if site['eligible']:
                self.stats['consumer_include_expressions'] += 1
                for provider, cpu, relative in site['providers']:
                    self.graph.includes[provider, cpu].add(relative)
            else:
                self.stats['private_generated_or_not_reachable'] += 1

    def text(self):
        source = self.doc.source
        edits = []
        for site in self.sites.values():
            if not site['eligible']:
                continue
            node, item = site['list'], site['item']
            first, last = source.span(item)
            index = node.elts.index(item)
            limit = (source.span(node.elts[index + 1])[0] if index + 1 < len(node.elts)
                     else source.span(node)[1] - 1)
            comma = source.bytes[last:limit].find(b',')
            if comma >= 0:
                last += comma + 1
            edits.append((first, last, ''))
        for _, call in source.rules():
            attrs = {kw.arg: kw.value for kw in call.keywords}
            name = attrs.get('name')
            if not isinstance(name, ast.Constant) or not isinstance(name.value, str):
                continue
            target = label(':' + name.value, self.doc.repo)
            values = {'@platforms//cpu:' + cpu: sorted(self.graph.includes[target, cpu])
                      for cpu in ARCHES}
            if not any(values.values()):
                continue
            first, second = values.values()
            addition = (json.dumps(first, indent=4) if first == second
                        else 'select(' + json.dumps(values, indent=4) + ')')
            if 'includes' in attrs:
                node = attrs['includes']
                edits.append((*source.span(node), '(' + source.text(node) + ') + ' + addition))
            else:
                at = source.span(call)[1] - 1
                edits.append((at, at, '\n    includes = ' + addition + ',\n'))
            self.stats['source_owner_targets'] += 1
        return source.rewrite(edits)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--receipts', type=Path, required=True)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    sources = Sources(args.receipts, args.root)
    if args.write:
        sources.require_complete()
    paths = [args.root / 'BUILD.bazel',
             *sorted((args.root / 'bazel/dependencies').glob('*.BUILD.bazel'))]
    documents = [Document(path, sources.inventories) for path in paths]
    graph = IncludeGraph(documents)
    rewrites = {}
    for doc in documents:
        if doc.repo == '@clickhouse' or doc.repo in SPECIAL_REPOS:
            continue
        rewrite = IncludeRewrite(doc, graph)
        rewrite.collect()
        rewrites[doc.repo] = rewrite
    pending = {}
    total = Counter()
    for repo, rewrite in rewrites.items():
        doc = rewrite.doc
        after = rewrite.text()
        total.update(rewrite.stats)
        if after != doc.source.bytes.decode():
            # buildifier does not sort source lists under this repository's
            # checked-in configuration. No changes to source ordering allowed.
            formatted = subprocess.run(
                ['buildifier', '-config=' + str(args.root / '.buildifier.json'), '-type=build'],
                input=after, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True,
            ).stdout
            original_rules = {
                (getattr(call.func, 'id', ''), ast_key({kw.arg: kw.value for kw in call.keywords}['name'])):
                {kw.arg: ast_key(kw.value) for kw in call.keywords
                 if kw.arg not in {'includes', 'local_includes'}}
                for _, call in doc.source.rules()
            }
            formatted_rules = {
                (getattr(call.func, 'id', ''), ast_key({kw.arg: kw.value for kw in call.keywords}['name'])):
                {kw.arg: ast_key(kw.value) for kw in call.keywords
                 if kw.arg not in {'includes', 'local_includes'}}
                for _, call in Source(formatted).rules()
            }
            if original_rules != formatted_rules:
                raise ValueError('Unexpected non-include attribute change: ' + str(doc.path))
            pending[doc.path] = formatted
    print('Includes:', dict(total), 'files:', len(pending))
    print('Published:', json.dumps({
        target + ' ' + cpu: sorted(dirs)
        for (target, cpu), dirs in sorted(graph.includes.items()) if dirs
    }, indent=2, sort_keys=True))
    # Reconstruct CcInfo include visibility from the formatted result rather
    # than trusting the planned graph additions. Every removed expression must
    # now be supplied at that exact directory on each applicable architecture.
    changed_documents = [
        Document(doc.path, sources.inventories,
                 text=pending.get(doc.path, doc.source.bytes.decode()))
        for doc in documents
    ]
    actual_graph = IncludeGraph(changed_documents)
    for rewrite in rewrites.values():
        for site in rewrite.sites.values():
            if not site['eligible']:
                continue
            for target, cpu, exact_directory in site['uses']:
                if not any(exact_directory in actual_graph.original_includes(provider, cpu)
                           for provider in actual_graph.visible(target, cpu)):
                    raise ValueError(f'{target} {cpu}: removed {exact_directory} is not published by deps')
    if args.write:
        for doc in documents:
            if doc.path in pending and doc.path.read_bytes() != doc.source.bytes:
                raise ValueError('BUILD changed during the audit: ' + str(doc.path))
        for path, formatted in pending.items():
            path.write_text(formatted)


if __name__ == '__main__':
    main()
