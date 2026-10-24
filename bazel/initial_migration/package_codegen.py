"""Emit the Arrow Flight and Kerberos generators in the packages that compile their outputs."""

import ast
import json
import re

from initial_migration.build_file import ARCHES
from initial_migration.repository_inputs import comparison_root, configuration_owner
from initial_migration.compact_build import Source
from initial_migration.configured_codegen_outputs import data_assignments, dictionary
from initial_migration.configured_external_outputs import collapse_equal_cpu_selects

# Match CMake output directories, not source target names or action hashes.
PACKAGES = {
    'contrib/arrow-cmake/cpp/src/arrow/flight/': ('clickhouse_arrow', 'ARROW_FLIGHT_CODEGEN_RULES'),
    'contrib/krb5-cmake/include_private/': ('clickhouse_krb5', 'KRB5_PRIVATE_CODEGEN_RULES'),
}


def partition(rules):
    remaining, pairs = {}, {}
    for name, attrs in rules.items():
        first = attrs['outs'][0].split('/', 2)
        owner = next((prefix for prefix in PACKAGES
                      if len(first) == 3 and first[0] == 'gen' and first[2].startswith(prefix)), None)
        if owner is None:
            remaining[name] = attrs
            continue
        repo, symbol = PACKAGES[owner]
        arch, output = first[1:]
        prefix = 'gen/' + arch + '/'
        if arch not in ARCHES or not all(p.startswith(prefix + owner) for p in attrs['outs']):
            raise ValueError('Mixed output packages or unrecognized architecture: ' + name)
        if set(attrs) != {'srcs', 'outs', 'tools', 'cmd'}:
            raise ValueError('Unrecognized package code generator: ' + name)
        cmd = attrs['cmd'].replace('$(BINDIR)/bazel/' + prefix, '$(RULEDIR)/config/')
        srcs = []
        for label in attrs['srcs']:
            if label.startswith('//bazel:' + prefix + owner):
                label = ':config/' + label.removeprefix('//bazel:' + prefix)
            elif repo == 'clickhouse_krb5' and label.startswith('@' + repo + '//:'):
                label = label.split('//:', 1)[1]
                cmd = cmd.replace('$$execroot/contrib/krb5/' + label + '"',
                                  '$$execroot/$(execpath :' + label + ')"')
            srcs.append(label)
        tools = []
        for label in attrs['tools']:
            # dependency_inputs() declares gen.pl and gen-map.pl in @clickhouse_krb5.
            if repo == 'clickhouse_krb5' and label in ('//:krb5_gen', '//:krb5_gen_map'):
                local = ':' + label.removeprefix('//:krb5_')
                cmd = cmd.replace('$(execpath ' + label + ')', '$(execpath ' + local + ')')
                label = local
            tools.append(label)
        rule = dict(srcs=srcs, outs=['config/' + p.removeprefix(prefix) for p in attrs['outs']],
                    tools=tools, cmd=cmd)
        variants = pairs.setdefault((symbol, output), {})
        if arch in variants:
            raise ValueError('Duplicate CMake code generator: ' + output)
        variants[arch] = rule
    groups = {symbol: {} for _, symbol in PACKAGES.values()}
    seen = set()
    for (symbol, output), variants in pairs.items():
        if set(variants) != set(ARCHES) or variants[ARCHES[0]] != variants[ARCHES[1]]:
            raise ValueError('CMake code generators differ across architectures: ' + output)
        attrs = variants[ARCHES[0]]
        name = 'generate_' + re.sub(r'[^a-zA-Z0-9_]+', '_', output)
        if name in remaining or name in groups[symbol] or seen.intersection(attrs['outs']):
            raise ValueError('Duplicate package code generator: ' + output)
        seen.update(attrs['outs'])
        groups[symbol][name] = attrs
    if not all(groups.values()):
        raise ValueError('No CMake code generators found for ' + ', '.join(k for k, v in groups.items() if not v))
    return remaining, groups


def package_source_map(rows, outputs):
    """Two physical CMake TUs and one Bazel TU share the same @build/contrib identity."""
    result = {}
    for row in rows:
        source, identity = row['from'], row['to']
        parts = identity.split('/', 2)
        arch = parts[1] if len(parts) == 3 and parts[1] in ARCHES else None
        path = parts[2] if arch else identity.removeprefix('@build/')
        if identity.startswith('@build/') and path in outputs:
            identity = '@build/' + path
            root = 'bazel-out/k8-opt/bin/'
            bazel = root + comparison_root(configuration_owner(path)) + '/config/' + path
            if source in {root + 'bazel/gen/' + prefix + path
                          for prefix in ('', *(a + '/' for a in ARCHES))}:
                source = bazel
            elif source != bazel and source not in {'build_bazel_reference_' + a + '/' + path for a in ARCHES}:
                raise ValueError('Unexpected generated CMake or Bazel source: ' + source)
        if source in result and result[source] != identity:
            raise ValueError('Conflicting generated source identity: ' + source)
        result[source] = identity
    return [{'from': path, 'to': target} for path, target in result.items()]


def publish_package_codegen(root, read_only=()):
    """Wire the emitted files to their declared repository labels and preserve maintained BUILD files."""
    path = root / 'bazel/rules/codegen_rules.bzl'
    decl = data_assignments(Source(path.read_text()))
    outputs = {}
    for repo, symbol in PACKAGES.values():
        for name, rule in dictionary(decl[symbol]).items():
            for out in ast.literal_eval(dictionary(rule)['outs']):
                if not out.startswith('config/') or out.removeprefix('config/') in outputs:
                    raise ValueError('Duplicate or invalid package output: ' + out)
                outputs[out.removeprefix('config/')] = repo
    pattern = re.compile(r'(?P<label>(?:@clickhouse)?//bazel:|:?)gen/(?:(?:'
                         + '|'.join(ARCHES) + r')/)?(?P<file>'
                         + '|'.join(re.escape(x) for x in sorted(outputs, key=len, reverse=True))
                         + r')(?![\w./+-])')
    paths = [root / 'BUILD.bazel', root / 'bazel/BUILD.bazel',
             *(root / 'bazel/dependencies').glob('*.BUILD.bazel')]
    changed = {}
    for p in paths:
        if not p.is_file():
            continue
        owner = p.name.removesuffix('.BUILD.bazel')
        text = p.read_text()
        def reference(match):
            repo = outputs[match['file']]
            if not match['label']:
                raise ValueError('Unqualified former //bazel output in ' + str(p) + ': ' + match[0])
            return (':' if owner == repo else '@' + repo + '//:') + 'config/' + match['file']
        updated = pattern.sub(reference, text)
        for repo, symbol in PACKAGES.values():
            if owner != repo:
                continue
            if symbol not in updated:
                pkg, = [x for x in ast.parse(updated).body if isinstance(x, ast.Expr)
                        and isinstance(x.value, ast.Call) and getattr(x.value.func, 'id', '') == 'package']
                source = Source(updated)
                end = source.span(pkg)[1]
                updated = source.rewrite([
                    (0, 0, 'load("@clickhouse//bazel/rules:codegen_actions.bzl", "declare_codegen")\n'
                     'load("@clickhouse//bazel/rules:codegen_rules.bzl", "' + symbol + '")\n'),
                    (end, end, '\n\n[declare_codegen(name = name, attrs = attrs)\n'
                     ' for name, attrs in ' + symbol + '.items()]\n'),
                ])
        if updated != text:
            if p in read_only:
                raise ValueError('Maintained BUILD file has stale generated labels: ' + str(p))
            changed[p] = collapse_equal_cpu_selects(updated)
    parity = root / 'bazel/initial_migration/any2bazel.json'
    data = json.loads(parity.read_text())
    new = package_source_map(data['ignore']['source_map'], outputs)
    if new != data['ignore']['source_map']:
        data['ignore']['source_map'] = new
        changed[parity] = json.dumps(data, indent=2) + '\n'
    for p, text in changed.items():
        p.write_text(text)
