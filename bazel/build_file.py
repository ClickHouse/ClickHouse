"""Render the configured native graph as BUILD rules in its owning repositories."""

from dataclasses import dataclass
import json
import re

from repository_inputs import REPOSITORIES, ROOT, execution_path, repository

ARCHES = ('x86_64', 'aarch64')
LABEL_ATTRIBUTES = {'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps'}


@dataclass
class Select:
    values: dict


@dataclass
class Glob:
    patterns: list
    exclude: tuple = ()


def select(values):
    choices = list(values.values())
    return choices[0] if all(value == choices[0] for value in choices) else Select(values)


def starlark(value, indent=0, paths=False, step=2):
    """Render data without Python's implicit adjacent string literals."""
    if isinstance(value, Select):
        return 'select(' + starlark(value.values, indent, paths, step) + ')'
    if isinstance(value, Glob):
        return 'glob(' + json.dumps(value.patterns) + (
            ', exclude = ' + json.dumps(value.exclude) if value.exclude else '') + ')'
    if isinstance(value, dict):
        entries = [json.dumps(k) + ': ' + starlark(v, indent + step, paths or (
            'shell' if k == 'cmd' else k in {
                'copts', 'c_flags', 'cxx_flags', 'link_flags', 'local_defines',
            }), step) for k, v in value.items()]
        brackets = '{}'
    elif isinstance(value, list):
        entries = [starlark(v, indent + step, paths, step) for v in value]
        brackets = '[]'
    else:
        if paths and isinstance(value, str):
            return execution_path(value, shell=paths == 'shell')
        return repr(value) if isinstance(value, bool) else json.dumps(value)
    if not entries:
        return brackets
    return brackets[0] + '\n' + ''.join(' ' * (indent + step) + e + ',\n' for e in entries) + ' ' * indent + brackets[1]


def target_owner(target):
    # Most ClickHouse wrappers are named <submodule>-cmake. Prefer that
    # explicit owner for libraries that combine sources from several repos.
    directory = target['paths']['source'].removesuffix('-cmake')
    owner = repository(directory)
    if owner:
        return owner
    sources = {
        repository(source['path'].removeprefix(str(ROOT) + '/'))
        for source in target.get('sources', []) if not source.get('isGenerated')
    } - {None}
    if len(sources) > 1:
        raise ValueError('Ambiguous source repository: ' + target['name'])
    return next(iter(sources)) if sources else None


def target_label(name, owners):
    owner = owners.get(name)
    return ('@' + REPOSITORIES[owner]['name'] + '//:' if owner else '//:') + name


def relocate_tools(value, owners):
    """Update root target references in generator inputs and location macros."""
    if isinstance(value, dict):
        return {key: relocate_tools(item, owners) for key, item in value.items()}
    if isinstance(value, list):
        return [relocate_tools(item, owners) for item in value]
    if isinstance(value, str):
        return re.sub(r'(?<![\w@/])//:([\w.+-]+)',
                      lambda m: target_label(m[1], owners) if m[1] in owners else m[0], value)
    return value


class BuildFile:
    def __init__(self, owner, owners):
        self.owner = owner
        self.owners = owners
        self.loads = {}
        self.rules = []
        self.external_inputs = set()
        self.local_sources = set()
        self.root_sources = set()

    def label(self, value):
        if value.startswith(':'):
            value = '//' + value
        if value.startswith('//:') and value[3:] in self.owners:
            value = target_label(value[3:], self.owners)
        if self.owner:
            prefix = '@' + REPOSITORIES[self.owner]['name'] + '//:'
            if value.startswith(prefix):
                self.local_sources.add(value)
                return ':' + value.removeprefix(prefix)
            if value.startswith('//:') and (ROOT / value[3:]).is_file():
                self.root_sources.add(value[3:])
            if value.startswith('//'):
                return '@clickhouse' + value
        if value.startswith('@'):
            self.external_inputs.add(value)
        return value

    def rule(self, kind, name, attrs):
        if kind in {'cc_library', 'cc_binary', 'cc_test'}:
            self.loads.setdefault('@rules_cc//cc:' + kind + '.bzl', set()).add(kind)
        elif kind == 'nasm_objects':
            self.loads.setdefault('@clickhouse//bazel:nasm.bzl', set()).add(kind)
        elif kind.startswith('perl_'):
            self.loads.setdefault('@rules_perl//perl:perl.bzl', set()).add(kind)
        elif kind in {'copy_file', 'expand_template', 'write_file'}:
            self.loads.setdefault('@bazel_skylib//rules:' + kind + '.bzl', set()).add(kind)
        lines = [kind + '(\n    name = ' + json.dumps(name) + ',\n']
        for key, value in attrs.items():
            if value == []:
                continue
            lines.append('    ' + key + ' = ' + starlark(
                value, 4, paths=key in {'copts', 'local_defines', 'linkopts'}, step=4) + ',\n')
        self.rules.append(''.join(lines) + ')\n')

    def target(self, name, variants):
        if all('alias' in variant for variant in variants.values()):
            actual, = {variant['alias'] for variant in variants.values()}
            self.rule('alias', name, {'actual': actual})
            return
        kind, = {v['kind'] for v in variants.values()}
        compatible = select({
            '@platforms//cpu:' + arch: [] if arch in variants else ['@platforms//:incompatible']
            for arch in ARCHES
        })
        group_count = max(len(v['groups']) for v in variants.values())
        binary_attrs = {}
        for index in range(group_count):
            groups = {arch: v['groups'][index] for arch, v in variants.items() if index < len(v['groups'])}
            attrs = {}
            for field in ['srcs', 'copts', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'local_defines', 'deps']:
                values = {}
                for arch in ARCHES:
                    value = groups.get(arch, {}).get(field, [])
                    values['@platforms//cpu:' + arch] = [self.label(v) for v in value] if field in LABEL_ATTRIBUTES else value
                attrs[field] = select(values)
            if kind == 'EXECUTABLE' and index == 0:
                binary_attrs = attrs
                # cc_binary has no hdrs attribute; preserve all declared inputs.
                binary_attrs['additional_compiler_inputs'] = select({
                    '@platforms//cpu:' + arch: [self.label(v) for field in
                        ['additional_compiler_inputs', 'hdrs', 'textual_hdrs']
                        for v in groups.get(arch, {}).get(field, [])]
                    for arch in ARCHES
                })
                del binary_attrs['hdrs'], binary_attrs['textual_hdrs']
                continue
            languages = {g['language'] for g in groups.values()}
            if 'ASM_NASM' in languages:
                if len(languages) != 1:
                    raise ValueError('Mixed compiler languages for ' + name)
                nasm_name = name + '__nasm_' + str(index)
                self.rule('nasm_objects', nasm_name, {
                    **{field: attrs[field] for field in ['srcs', 'hdrs', 'copts', 'local_defines', 'deps']},
                    'target_compatible_with': compatible,
                })
                attrs = {'srcs': [':' + nasm_name]}
            self.rule('cc_library', name + '__compile_' + str(index), {
                **attrs,
                'features': ['-fortify_source'],
                'target_compatible_with': compatible,
                'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
            })
        deps = select({
            '@platforms//cpu:' + arch: [self.label(d) for d in v['deps']] + [
                ':' + name + '__compile_' + str(i)
                for i in range(1 if kind == 'EXECUTABLE' else 0, group_count)
            ] + ([self.label(d) for d in v['groups'][0].get('deps', [])] if kind == 'EXECUTABLE' and v['groups'] else [])
            for arch, v in variants.items()
        })
        if kind == 'EXECUTABLE':
            binary_attrs.pop('deps', None)
            self.rule('cc_test' if name == 'unit_tests_dbms' else 'cc_binary', name, {
                **binary_attrs,
                'deps': deps,
                'linkopts': select({'@platforms//cpu:' + arch: v['linkopts'] for arch, v in variants.items()}),
                'features': ['-static_link_cpp_runtimes', '-fortify_source'],
                'target_compatible_with': compatible,
                **({'env': {'OPENSSL_CONF': '/dev/null'}, 'size': 'enormous', 'linkstatic': True}
                   if name == 'unit_tests_dbms' else {}),
            })
        else:
            self.rule('cc_library', name, {'deps': deps, 'target_compatible_with': compatible})

    def load_statements(self):
        loads = dict(self.loads)
        if any('repository_path(' in rule for rule in self.rules):
            loads['@clickhouse//bazel:repository_paths.bzl'] = {'repository_path'}
        return ''.join('load(' + json.dumps(path) + ', ' + ', '.join(map(json.dumps, sorted(names))) + ')\n'
                       for path, names in sorted(loads.items()))

    def text(self):
        return '\n'.join(self.rules)


def dependency_inputs(builds, owners):
    """Declare the non-CMake inputs and host tools beside their pinned sources."""
    def build(owner):
        if owner not in builds:
            builds[owner] = BuildFile(owner, owners)
        return builds[owner]

    for owner, name in [('contrib/delta-kernel-rs', 'cbindgen_sources'),
                        ('contrib/rust_vendor', 'sources')]:
        build(owner).rule('filegroup', name, {
            'srcs': Glob(['**'], ('**/BUILD', '**/BUILD.bazel')),
        })
    build('contrib/openssl').rule('filegroup', 'headers', {'srcs': Glob(['include/**'])})
    build('contrib/sysroot').rule('cc_library', 'linux_aarch64_hwcap', {
        'hdrs': ['linux-aarch64/aarch64-linux-gnu/libc/usr/include/asm/hwcap.h'],
        'strip_include_prefix': 'linux-aarch64/aarch64-linux-gnu/libc/usr/include',
        'target_compatible_with': ['@platforms//cpu:aarch64'],
    })
    krb5 = build('contrib/krb5')
    krb5.rule('perl_library', 'templates', {
        'srcs': Glob(['src/util/*.pm']), 'includes': ['src/util'],
    })
    for name, source in [('gen_map', 'gen-map.pl'), ('gen', 'gen.pl')]:
        krb5.rule('perl_binary', name, {'srcs': ['src/util/' + source], 'deps': [':templates']})
