"""Render the configured native graph as BUILD rules in its owning repositories."""

from dataclasses import dataclass
import ast
import json
from pathlib import Path
import re

import llvm_targets
from compact_build import compact, header_patterns
from repository_inputs import (
    REPOSITORIES, ROOT, CC_HEADER_SUFFIXES, HeaderDirectory,
    directory_sources, execution_path, repository, source_label, source_path, textual_source_names,
)

ARCHES = ('x86_64', 'aarch64')
LABEL_ATTRIBUTES = {'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps'}

# These libraries already export their complete public C/C++ headers. Private
# sources and generated configuration files must still be declared separately.
PUBLIC_HEADERS = {
    'contrib/fmtlib': ('contrib/fmtlib/include/fmt/', '_fmt'),
    'contrib/flatbuffers': ('contrib/flatbuffers/include/flatbuffers/', '_flatbuffers'),
    'contrib/libuv': ('contrib/libuv/include/', '_uv'),
}

# These CMake groups compile mutually including source repositories. Their
# header libraries have no compile dependencies, so the native libraries can
# share public headers without depending on one another's archives.
SOURCE_HEADER_DIRECTORIES = {
    'contrib/aws': (
        'generated/src/aws-cpp-sdk-glue/include',
        'generated/src/aws-cpp-sdk-s3/include',
        'src/aws-cpp-sdk-core/include',
    ),
    'contrib/arrow': ('cpp/src',),
    **{owner: ('include',) for owner in REPOSITORIES
       if (owner.startswith('contrib/aws-c-') and owner != 'contrib/aws-c-mqtt')
       or owner in {'contrib/aws-checksums', 'contrib/aws-crt-cpp'}},
}
SOURCE_HEADER_TARGETS = {
    owner: (REPOSITORIES[owner]['name'].removeprefix('clickhouse_') + '_headers')
    for owner in SOURCE_HEADER_DIRECTORIES
}
PUBLIC_HEADERS.update({
    owner: (tuple(owner + '/' + directory + '/' for directory in directories), SOURCE_HEADER_TARGETS[owner])
    for owner, directories in SOURCE_HEADER_DIRECTORIES.items()
})

def global_compile_options():
    """Read unconditional Bazel compiler options from the project's rc file."""
    options = {'copt': set(), 'conlyopt': set(), 'cxxopt': set()}
    for option, value in re.findall(
            r'^common --(copt|conlyopt|cxxopt)=(\S+)$',
            (ROOT / '.bazelrc').read_text(), re.MULTILINE):
        options[option].add(value)
    return {
        'C': options['copt'] | options['conlyopt'],
        'CXX': options['copt'] | options['cxxopt'],
        'ASM': options['copt'],
    }


GLOBAL_COMPILE_OPTIONS = global_compile_options()
CLICKHOUSE_WARNINGS = ast.literal_eval(next(
    node.value for node in ast.parse((ROOT / 'bazel/repository_paths.bzl').read_text()).body
    if isinstance(node, ast.Assign)
    and any(isinstance(name, ast.Name) and name.id == 'CLICKHOUSE_WARNINGS' for name in node.targets)
))
_LIBRARY_FLAGS = {
    node.targets[0].id: ast.literal_eval(node.value)
    for node in ast.parse((ROOT / 'bazel/ch_cc_library.bzl').read_text()).body
    if isinstance(node, ast.Assign)
}
CLICKHOUSE_TARGET_FLAGS = _LIBRARY_FLAGS['CLICKHOUSE_TARGET_FLAGS']
CLICKHOUSE_CODE_FLAGS = _LIBRARY_FLAGS['CLICKHOUSE_CODE_FLAGS']

def remove_global_compile_options(flags, language):
    """Omit flags from .bazelrc, including CMake's split -D definitions."""
    options = GLOBAL_COMPILE_OPTIONS[language]
    result = []
    position = 0
    while position < len(flags):
        flag = flags[position]
        if (flag == '-D' and position + 1 < len(flags)
                and '-D' + flags[position + 1] in options):
            position += 2
        elif flag in options:
            position += 1
        else:
            result.append(flag)
            position += 1
    return result


@dataclass
class Select:
    values: dict


@dataclass
class Glob:
    patterns: list
    exclude: tuple = ()
    allow_empty: bool = False


@dataclass
class Concat:
    values: list


@dataclass
class LocalInclude:
    directory: str
    owner: str | None
    raw_llvm: bool = False


def split_system_includes(flags, owner):
    options, includes = [], []
    flags = iter(flags)
    for flag in flags:
        if flag == '-isystem':
            directory = next(flags)
            includes.append(LocalInclude(directory, owner))
        else:
            options.append(flag)
    return options, includes


@dataclass
class CMakeCopts:
    flags: object
    arches: list


@dataclass
class CMakeNativeCopts:
    flags: object
    groups: dict


@dataclass
class CMakeWarnings:
    before: list
    after: list


@dataclass
class CMakeCxxCompilerVersion:
    arch: str
    prefix: str = ''
    suffix: str = ''


def select(values):
    choices = list(values.values())
    return choices[0] if all(value == choices[0] for value in choices) else Select(values)


def compile_dependencies(group_deps, target_deps):
    # CMake's llvm/include includes headers from every linked LLVM component.
    # Support alone supplies neither IRBuilder.h nor the generated IR headers.
    if '@llvm-project//llvm:Support' not in group_deps:
        return group_deps
    components = {
        llvm_targets.target(dep.rsplit(':', 1)[-1])
        for dep in target_deps
        if dep.startswith((':LLVM', '//:LLVM', '@clickhouse//:LLVM'))
    }
    return sorted(set(group_deps) | components)


def compile_options(copts, group_deps):
    # ClickHouse's LLVM, libunwind and libllvmlibc CMake targets, and
    # cmake/sanitize.cmake, mark these includes SYSTEM. The Bazel targets use -I.
    if '-Weverything' not in copts:
        return copts
    copts = list(copts)
    if '@llvm-project//llvm:Support' in group_deps:
        copts += ['-isystem', llvm_targets.PREFIX + '/llvm/include',
                  '-isystem', '$(GENDIR)/' + llvm_targets.PREFIX + '/llvm/include']
    for dep, include in [
        ('@llvm-project//libunwind:headers', '/libunwind/include'),
        ('@llvm-project//libc:libc_headers', '/libc'),
        ('@llvm-project//compiler-rt:headers', '/compiler-rt/include'),
    ]:
        if dep in group_deps:
            copts += ['-isystem', llvm_targets.PREFIX + include]
    return copts


def starlark(value, indent=0, paths=False, step=2):
    """Render data without Python's implicit adjacent string literals."""
    if isinstance(value, Concat):
        return ' + '.join(starlark(part, indent, paths, step) for part in value.values)
    if isinstance(value, LocalInclude):
        directory = value.directory.removeprefix('$(GENDIR)/')
        # Common same-repository dirs stay readable in the generated BUILD.
        if value.owner and repository(directory) == value.owner and not value.raw_llvm:
            relative = directory.removeprefix(value.owner).lstrip('/')
            return json.dumps(relative or '.')
        expression = execution_path(value.directory, raw_llvm=value.raw_llvm)
        if value.owner is None and expression == json.dumps(value.directory):
            return json.dumps(directory)
        return ('repository_local_include(' + expression +
                (', ' + json.dumps(value.owner) if value.owner else '') + ')')
    if isinstance(value, CMakeCopts):
        return (starlark(value.flags, indent, paths, step) + ' + cmake_prefix_maps('
                + json.dumps(value.arches) + ')')
    if isinstance(value, CMakeNativeCopts):
        # Executables and NASM retain their own flags; ch_cc_library supplies
        # these flags to native libraries in BuildFile.rule().
        choices = {'//conditions:default': []}
        for arch, group in value.groups.items():
            options = []
            if group['language'] in GLOBAL_COMPILE_OPTIONS:
                gcc_root = 'contrib/sysroot/linux-' + arch
                options = ['--sysroot=' + gcc_root + '/' + arch + '-linux-gnu/libc',
                           '--gcc-toolchain=' + gcc_root]
            if group['language'] == 'CXX':
                options.append('-nostdinc++')
            for flag, origin, directory in group['includes']:
                root = llvm_targets.PREFIX
                if origin == 'generated':
                    root = '$(GENDIR)/' + root + '/gen/' + arch
                options.extend([flag, root + '/' + directory])
            choices['@platforms//cpu:' + arch] = options
        return (starlark(value.flags, indent, paths, step) + ' + '
                + starlark(Select(choices), indent, 'cmake_runtime', step))
    if isinstance(value, CMakeWarnings):
        return (starlark(value.before, indent, paths, step) + ' + CLICKHOUSE_WARNINGS + '
                + starlark(value.after, indent, paths, step))
    if isinstance(value, CMakeCxxCompilerVersion):
        parts = [json.dumps(value.prefix)] if value.prefix else []
        parts.append('cmake_cxx_compiler_version(' + json.dumps(value.arch) + ')')
        if value.suffix:
            parts.append(json.dumps(value.suffix))
        return ' + '.join(parts)
    if isinstance(value, Select):
        return 'select(' + starlark(value.values, indent, paths, step) + ')'
    if isinstance(value, Glob):
        patterns = header_patterns(value.patterns)
        function = 'header_glob' if patterns is not None else 'glob'
        return function + '(' + json.dumps(value.patterns if patterns is None else patterns) + (
            ', exclude = ' + json.dumps(value.exclude) if value.exclude else '') + (
            ', allow_empty = True' if value.allow_empty else '') + ')'
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
            return execution_path(value, shell=paths == 'shell', raw_llvm=paths == 'cmake_runtime')
        return repr(value) if isinstance(value, bool) else json.dumps(value)
    if not entries:
        return brackets
    return brackets[0] + '\n' + ''.join(' ' * (indent + step) + e + ',\n' for e in entries) + ' ' * indent + brackets[1]


def target_owner(target):
    # Generated sources follow the native library. Configuration files stay in
    # configuration_owner(), even when the CMake wrapper is in another repo.
    sources = {
        repository(source['path'].removeprefix(str(ROOT) + '/'))
        for source in target.get('sources', [])
        if not source.get('isGenerated') and 'compileGroupIndex' in source
    } - {None}
    if len(sources) == 1:
        return next(iter(sources))
    directory = target['paths']['source'].removesuffix('-cmake')
    owner = repository(directory)
    if owner:
        return owner
    if len(sources) > 1:
        raise ValueError('Ambiguous source repository: ' + target['name'])
    return None


def target_label(name, owners):
    owner = owners.get(name)
    return ('@' + REPOSITORIES[owner]['name'] + '//:' if owner else '//:') + name


def native_source_owner(path, owner, language):
    """Keep generated, main-repo, and toolchain files with their native target."""
    if language not in GLOBAL_COMPILE_OPTIONS:
        return owner
    candidate = repository(path)
    if (candidate and not REPOSITORIES[candidate].get('module')
            and not path.removeprefix(candidate + '/').startswith(('config/', 'gen/'))):
        return candidate
    return owner


def source_compile_label(name, index, owner, first):
    """Name the first source-owned group for its repository; keep flag variants."""
    short = REPOSITORIES[owner]['name'].removeprefix('clickhouse_')
    if index != first:
        short += '__compile_' + str(index)
    return target_label(short, {short: owner})


def relocate_compilation(owners, targets):
    """Partition each native group without changing source or archive ordering."""
    foreign = {}
    for name, owner in list(owners.items()):
        first = {}
        for arch, arch_targets in targets.items():
            target = arch_targets.get(name)
            if not target or target['kind'] == 'EXECUTABLE':
                continue
            for index, group in enumerate(target.get('groups', [])):
                if group.get('cmake_runtime'):
                    continue
                for src in group.get('srcs', []):
                    src_owner = native_source_owner(source_path(src), owner, group['language'])
                    if src_owner != owner:
                        first[src_owner] = min(first.get(src_owner, index), index)
        for arch, arch_targets in targets.items():
            target = arch_targets.get(name)
            if not target or not first:
                continue
            for index, group in enumerate(target.get('groups', [])):
                if group.get('cmake_runtime'):
                    continue
                partitions = {}
                for src in group.get('srcs', []):
                    src_owner = native_source_owner(source_path(src), owner, group['language'])
                    partitions.setdefault(src_owner, []).append(src)
                group['srcs'] = partitions.pop(owner, [])
                group['foreign_deps'] = []
                for src_owner, srcs in partitions.items():
                    label = source_compile_label(name, index, src_owner, first[src_owner])
                    part_name = label.rsplit(':', 1)[-1]
                    if part_name in owners:
                        raise ValueError('Source-owned compile target name collision: ' + label)
                    key = (src_owner, part_name, name)
                    variants = foreign.setdefault(key, {})
                    if arch in variants:
                        raise ValueError('Multiple source-owned groups for ' + label)
                    group['foreign_deps'].append(label)
                    variants[arch] = {
                        **target, 'kind': 'STATIC_LIBRARY', 'deps': [],
                        'groups': [{**group, 'srcs': srcs, 'foreign_deps': [],
                                    'deps': compile_dependencies(group.get('deps', []), target['deps'])}],
                    }
    return foreign


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
    def __init__(self, owner, owners, header_consumers=None, targets=None, external_headers=None,
                 native_sources=()):
        self.owner = owner
        self.owners = owners
        self.header_consumers = header_consumers or {}
        self.targets = targets or {}
        self.external_headers = external_headers
        self.native_sources = native_sources
        self.public_directories = {}
        self.loads = {}
        self.rules = []
        self.names = set()
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

    def label_inputs(self, values):
        labels, parts = [], []
        for value in values:
            if isinstance(value, Glob):
                if labels:
                    parts.append(labels)
                    labels = []
                parts.append(value)
            else:
                labels.append(self.label(value))
        if labels or not parts:
            parts.append(labels)
        return parts[0] if len(parts) == 1 else Concat(parts)

    def dependency_header_directories(self, arch, dependencies):
        """Source header directories already exported by existing cc deps."""
        key = (arch, tuple(dependencies))
        if key in self.public_directories:
            return self.public_directories[key]
        targets = self.targets.get(arch, {})
        directories, seen = set(), set()
        pending = list(dependencies)
        while pending:
            # Configured native target names are unique across repositories.
            ref = pending.pop()
            target_name = ref.rsplit(':', 1)[-1]
            if target_name in seen:
                continue
            if ref not in {
                ':' + target_name,
                '//:' + target_name,
                target_label(target_name, self.owners),
            }:
                # An external Bazel module may use the same short name.
                continue
            seen.add(target_name)
            target = targets.get(target_name)
            if not target or target.get('kind') == 'EXECUTABLE':
                continue
            pending.extend(target.get('deps', []))
            for dep_group in target.get('groups', []):
                pending.extend(dep_group.get('deps', []))
                if dep_group.get('cmake_runtime') or dep_group.get('language') == 'ASM_NASM':
                    continue
                for item in dep_group.get('hdrs', []):
                    if (isinstance(item, HeaderDirectory)
                            and repository(item.directory) == self.owners.get(target_name)
                            # _boost_* substitutes boost_headers plus its own
                            # libs/<component>; it never exports all of boost/.
                            and not (item.directory == 'contrib/boost' and target_name.startswith('_boost_'))
                            and not self.header_consumers.get(item.directory, set()) <= {target_name}):
                        directories.add(item.directory.rstrip('/') + '/')
        result = tuple(sorted(directories))
        self.public_directories[key] = result
        return result

    def compile_inputs(self, name, group, binary=False, arch=None, library_deps=()):
        """Separate private headers, public headers, and non-compilable inputs."""
        fields = {key: list(group.get(key, [])) for key in
                  ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps')}
        original = {}
        for field in ('hdrs', 'additional_compiler_inputs'):
            directories = [item.directory for item in fields[field] if isinstance(item, HeaderDirectory)]
            fields[field] = [item for item in fields[field] if not isinstance(item, HeaderDirectory)]
            # Prefer src/** over src/unix/** and src/win/**; each contributes
            # the same files, and duplicate srcs are not valid Bazel targets.
            original[field] = [
                directory for directory in sorted(set(directories))
                if not any(directory.startswith(parent + '/') for parent in directories if parent != directory)
            ]
        custom = group.get('cmake_runtime') or group.get('language') == 'ASM_NASM'
        supplied = () if custom else self.dependency_header_directories(
            arch, tuple(group.get('deps', ())) + tuple(library_deps))
        for old_field, directories in original.items():
            for directory in directories:
                owner = repository(directory)
                field = ('srcs' if binary or self.header_consumers.get(directory, set()) <= {name} else 'hdrs')
                if not custom and owner == 'contrib/boost':
                    if owner != self.owner:
                        fields['deps'].append('@clickhouse_boost//:boost_headers')
                        continue
                    if directory == owner and name.startswith('_boost_'):
                        fields['deps'].append('@clickhouse_boost//:boost_headers')
                        # The compiled Boost component keeps all private
                        # implementation files, including textual .cpp inputs.
                        directory += '/libs/' + name.removeprefix('_boost_')
                if old_field == 'additional_compiler_inputs':
                    field = old_field
                elif custom:
                    field = 'hdrs'
                non_header_field = 'hdrs' if custom else 'additional_compiler_inputs'
                if owner == self.owner:
                    relative = directory.removeprefix(owner + '/') if owner else directory
                    if relative == owner:
                        relative = ''
                    pattern = (relative + '/' if relative else '') + '**'
                    excluded = (pattern + '/BUILD', pattern + '/BUILD.bazel')
                    headers = [pattern + '/*' + suffix for suffix in CC_HEADER_SUFFIXES]
                    if custom or field == 'additional_compiler_inputs':
                        fields[field].append(Glob([pattern], excluded, allow_empty=True))
                    else:
                        # Exclude any explicit srcs or generated inputs to
                        # prevent one header from being declared twice.
                        local = []
                        prefix = '@' + REPOSITORIES[owner]['name'] + '//:' if owner else '//:'
                        for key in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                            values = fields[key]
                            for item in values:
                                if isinstance(item, str) and item.startswith(prefix):
                                    local.append(item.removeprefix(prefix))
                        fields[field].append(Glob(headers, (*excluded, *local), allow_empty=True))
                        fields[non_header_field].append(Glob([pattern], (*excluded, *headers), allow_empty=True))
                else:
                    for path in directory_sources(directory):
                        public = PUBLIC_HEADERS.get(owner)
                        if (not custom and public and path.startswith(public[0])
                                and path.endswith(CC_HEADER_SUFFIXES)):
                            fields['deps'].append('@' + REPOSITORIES[owner]['name'] + '//:' + public[1])
                            continue
                        target_field = field if custom or path.endswith(CC_HEADER_SUFFIXES) else non_header_field
                        if (target_field in {'hdrs', 'additional_compiler_inputs'}
                                and path.endswith(CC_HEADER_SUFFIXES)
                                and path.startswith(supplied)):
                            continue
                        fields[target_field].append(source_label(path))
        if not custom and self.external_headers is not None:
            self.external_headers.use(fields, self.owner)
        if (not custom and not binary and self.owner in SOURCE_HEADER_DIRECTORIES
                and self.owner.startswith('contrib/aws')):
            candidates = {
                item for item in fields['additional_compiler_inputs']
                if isinstance(item, str) and item in self.native_sources
                and repository(source_path(item)) != self.owner
            }
            if candidates:
                included = textual_source_names(self.owner)
                if included is not None:
                    fields['additional_compiler_inputs'] = [
                        item for item in fields['additional_compiler_inputs']
                        if not isinstance(item, str) or item not in candidates
                        or Path(source_path(item)).name in included
                    ]
        if custom:
            fields['hdrs'].extend(fields['textual_hdrs'])
            fields['hdrs'].extend(fields['additional_compiler_inputs'])
            fields['textual_hdrs'] = []
            fields['additional_compiler_inputs'] = []
        elif binary:
            fields['additional_compiler_inputs'].extend(fields['hdrs'])
            fields['additional_compiler_inputs'].extend(fields['textual_hdrs'])
            fields['hdrs'] = []
            fields['textual_hdrs'] = []
        # Source labels may occur in both an explicit generated list and an
        # enumerated foreign directory. Keep glob operands in their order.
        for key, values in fields.items():
            seen = set()
            unique = []
            for value in values:
                if isinstance(value, str):
                    if value in seen:
                        continue
                    seen.add(value)
                unique.append(value)
            fields[key] = unique
        return fields

    def rule(self, kind, name, attrs):
        self.names.add(name)
        copts = attrs.get('copts')
        if (kind == 'cc_library' and isinstance(copts, CMakeNativeCopts)
                and all(group['language'] in GLOBAL_COMPILE_OPTIONS for group in copts.groups.values())):
            kind = 'ch_cc_library'
            self.loads.setdefault('@clickhouse//bazel:ch_cc_library.bzl', set()).add(kind)
            languages = {group['language'] for group in copts.groups.values()}
            if len(languages) != 1:
                raise ValueError(f'{name}: expected one native compiler language, got {languages}')
            language = next(iter(languages))
            if language != 'CXX':
                attrs['language'] = language
            arches = list(copts.groups)
            if set(arches) != set(ARCHES):
                attrs['arches'] = arches
            expected_includes = [] if language == 'CXX' else [['-I', 'generated', 'libcxx/include/c++/v1']]
            includes = [group['includes'] for group in copts.groups.values()]
            if all(value == [] for value in includes) and expected_includes:
                attrs['libcxx_headers'] = False
            elif not all(value == expected_includes for value in includes):
                raise ValueError(f'{name}: unsupported native libc++ includes: {includes}')
            flags = copts.flags
            if isinstance(flags, CMakeCopts) and set(flags.arches) == set(arches):
                flags = flags.flags
            else:
                # If a future CMake group uses source maps on only one CPU,
                # retain that select instead of applying the macro's default.
                attrs['map_source_files'] = False
            attrs['copts'] = flags
            defaults = {'-fortify_source', '-default_compile_flags', '-llvm_system_c_headers'}
            features = attrs.get('features', [])
            if isinstance(features, Select):
                attrs['features'] = select({key: [flag for flag in value if flag not in defaults]
                                            for key, value in features.values.items()})
            else:
                attrs['features'] = [flag for flag in features if flag not in defaults]
        if kind in {'cc_library', 'cc_binary', 'cc_test', 'cc_import'}:
            self.loads.setdefault('@rules_cc//cc:' + kind + '.bzl', set()).add(kind)
        elif kind == 'nasm_objects':
            self.loads.setdefault('@clickhouse//bazel:nasm.bzl', set()).add(kind)
        elif kind == 'cmake_link':
            self.loads.setdefault('@clickhouse//bazel:cmake_link.bzl', set()).add(kind)
        elif kind == 'cmake_runtime_objects':
            self.loads.setdefault('@clickhouse//bazel:cmake_runtime.bzl', set()).add(kind)
        elif kind.startswith('perl_'):
            self.loads.setdefault('@rules_perl//perl:perl.bzl', set()).add(kind)
        elif kind in {'copy_file', 'expand_template', 'write_file'}:
            self.loads.setdefault('@bazel_skylib//rules:' + kind + '.bzl', set()).add(kind)
        lines = [kind + '(\n    name = ' + json.dumps(name) + ',\n']
        for key, value in attrs.items():
            if value == []:
                continue
            paths = key in {'copts', 'local_defines', 'linkopts'}
            if kind == 'cmake_runtime_objects' and paths:
                paths = 'cmake_runtime'
            lines.append('    ' + key + ' = ' + starlark(value, 4, paths=paths, step=4) + ',\n')
        self.rules.append(''.join(lines) + ')\n')

    def target(self, name, variants, input_name=None):
        input_name = input_name or name
        if all('alias' in variant for variant in variants.values()):
            actual, = {variant['alias'] for variant in variants.values()}
            self.rule('alias', name, {'actual': actual})
            return
        kind, = {v['kind'] for v in variants.values()}
        compatible = select({
            '@platforms//cpu:' + arch: [] if arch in variants else ['@platforms//:incompatible']
            for arch in ARCHES
        })
        if all('static_library' in v for v in variants.values()):
            archive, = {v['static_library'] for v in variants.values()}
            self.rule('cc_import', name, {
                'static_library': self.label(archive),
                'deps': select({'@platforms//cpu:' + arch: [self.label(d) for d in v['deps']]
                                for arch, v in variants.items()}),
                'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
                'linkopts': select({'@platforms//cpu:' + arch: v['linkopts'] for arch, v in variants.items()}),
                'target_compatible_with': compatible,
            })
            return
        group_count = max(len(v['groups']) for v in variants.values())
        collapse_single = group_count == 1 and kind != 'EXECUTABLE' and all(
            group['language'] in GLOBAL_COMPILE_OPTIONS and not group.get('cmake_runtime')
            for variant in variants.values() for group in variant['groups']
        )
        binary_attrs = {}
        for index in range(group_count):
            groups = {arch: v['groups'][index] for arch, v in variants.items() if index < len(v['groups'])}
            if all(not g.get('srcs') and g.get('foreign_deps') for g in groups.values()):
                self.rule('cc_library', name + '__compile_' + str(index), {
                    'deps': select({'@platforms//cpu:' + arch: [self.label(d) for d in g['foreign_deps']]
                                    for arch, g in groups.items()}),
                    'target_compatible_with': compatible,
                })
                continue
            binary = kind == 'EXECUTABLE' and index == 0 and name != 'clickhouse'
            inputs = {
                arch: self.compile_inputs(
                    input_name, group, binary, arch,
                    variants[arch]['deps'] if collapse_single else (),
                )
                for arch, group in groups.items()
            }
            runtime_groups = [g for g in groups.values() if g.get('cmake_runtime')]
            if runtime_groups:
                if len(runtime_groups) != len(groups) or kind != 'STATIC_LIBRARY':
                    raise ValueError('Mixed CMake runtime compilation groups for ' + name)
                attrs = {}
                for field in ('srcs', 'hdrs', 'copts', 'local_defines'):
                    values = {}
                    for arch in ARCHES:
                        group = groups.get(arch, {})
                        value = inputs.get(arch, {}).get(field, []) if field in LABEL_ATTRIBUTES else group.get(field, [])
                        if field in LABEL_ATTRIBUTES:
                            value = self.label_inputs(value)
                        values['@platforms//cpu:' + arch] = value
                    attrs[field] = select(values)
                mapped_arches = [arch for arch, group in groups.items() if group.get('cmake_source_prefix_map')]
                if mapped_arches:
                    attrs['copts'] = CMakeCopts(attrs['copts'], sorted(mapped_arches))
                unmapped = {
                    '@platforms//cpu:' + arch: group['cmake_reference_source_root']
                    for arch, group in groups.items()
                    if not group.get('cmake_source_prefix_map')
                }
                if unmapped:
                    attrs['reference_source_root'] = select(unmapped)
                attrs['language'] = select({
                    '@platforms//cpu:' + arch: groups.get(arch, {}).get('language', 'C')
                    for arch in ARCHES
                })
                attrs['arch'] = select({'@platforms//cpu:' + arch: arch for arch in ARCHES})
                object_name = name + '__runtime_objects_' + str(index)
                self.rule('cmake_runtime_objects', object_name, {
                    **attrs,
                    'target_compatible_with': compatible,
                })
                self.rule('cc_library', name + '__compile_' + str(index), {
                    'srcs': [':' + object_name],
                    'target_compatible_with': compatible,
                    'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
                })
                continue
            attrs = {}
            native_library = kind != 'EXECUTABLE' or index != 0 or name == 'clickhouse'
            clickhouse_code = native_library and bool(groups) and all(
                any(group.get('copts', [])[start:start + len(CLICKHOUSE_CODE_FLAGS)] == CLICKHOUSE_CODE_FLAGS
                    for start in range(len(group.get('copts', []))))
                for group in groups.values()
            )
            system_includes = {}
            native_includes = {}
            for arch, group in groups.items():
                if group['language'] in GLOBAL_COMPILE_OPTIONS and not group.get('cmake_runtime'):
                    native_includes[arch] = []
                    system_includes[arch] = []
                    for flag, origin, directory in group.get('native_cxx_includes', []):
                        if flag == '-isystem':
                            prefix = ('$(GENDIR)/' + llvm_targets.PREFIX + '/gen/' + arch
                                      if origin == 'generated' else llvm_targets.PREFIX)
                            system_includes[arch].append(LocalInclude(prefix + '/' + directory, self.owner, raw_llvm=True))
                        else:
                            native_includes[arch].append([flag, origin, directory])
            for field in ['srcs', 'copts', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'local_defines', 'deps']:
                values = {}
                for arch in ARCHES:
                    group = groups.get(arch, {})
                    value = inputs.get(arch, {}).get(field, []) if field in inputs.get(arch, {}) else group.get(field, [])
                    if field == 'deps' and kind != 'EXECUTABLE' and arch in variants:
                        value = compile_dependencies(value, variants[arch]['deps'])
                        if collapse_single:
                            value = value + variants[arch]['deps']
                        value = value + group.get('foreign_deps', [])
                    if field == 'copts':
                        value = compile_options(value, group.get('deps', []))
                        language = group.get('language')
                        if arch in system_includes:
                            value, includes = split_system_includes(value, self.owner)
                            system_includes[arch] = includes + system_includes[arch]
                        if native_library and language in GLOBAL_COMPILE_OPTIONS:
                            # These original CMake C groups have no -Werror.
                            if language == 'C' and name in {'glibc-compatibility', 'harmful'}:
                                value = value + ['-Wno-error']
                            value = remove_global_compile_options(value, language)
                            value = [flag for flag in value if flag not in CLICKHOUSE_TARGET_FLAGS[arch]]
                            if clickhouse_code:
                                start = value.index(CLICKHOUSE_CODE_FLAGS[0])
                                value = value[:start] + value[start + len(CLICKHOUSE_CODE_FLAGS):]
                            if language in {'C', 'CXX'} and '-Weverything' in value:
                                start = value.index('-Weverything')
                                end = start + len(CLICKHOUSE_WARNINGS)
                                if value[start:end] == CLICKHOUSE_WARNINGS:
                                    value = CMakeWarnings(value[:start], value[end:])
                    if group.get('language') in {'C', 'CXX', 'ASM'}:
                        if field == 'hdrs':
                            value = value + ['@clickhouse_sysroot//:linux_native_headers']
                        if field == 'additional_compiler_inputs':
                            value = value + ['@clickhouse_sysroot//:linux_compile_files']
                            if group.get('native_cxx_includes'):
                                value += [
                                    '@clickhouse_llvm_project//:runtime_source_inputs',
                                    '@clickhouse_llvm_project//:runtime_generated_headers',
                                ]
                    if group.get('language') == 'ASM_NASM' and field == 'hdrs':
                        value = list(value)
                        for _, origin, directory in group.get('native_cxx_includes', []):
                            if origin == 'generated' and directory == 'libcxx/include/c++/v1':
                                value.append('@clickhouse_llvm_project//:runtime_generated_headers')
                            elif origin == 'source' and directory in {'libcxx/include', 'libcxxabi/include'}:
                                library = directory.split('/', 1)[0]
                                value.append('@clickhouse_llvm_project//:runtime_' + library + '_headers')
                            else:
                                raise ValueError(f'{name}: unsupported NASM include {origin} {directory}')
                    if field in LABEL_ATTRIBUTES:
                        value = self.label_inputs(value)
                        if field == 'deps' and collapse_single:
                            # The compile dependencies include public header
                            # libraries. Keep them before the old wrapper's
                            # dependencies and deduplicate after rebasing.
                            value = list(dict.fromkeys(value))
                    values['@platforms//cpu:' + arch] = value
                attrs[field] = select(values)
            if any(system_includes.values()):
                attrs['local_includes'] = select({
                    '@platforms//cpu:' + arch: system_includes.get(arch, [])
                    for arch in ARCHES
                })
            if clickhouse_code:
                attrs['clickhouse_code'] = True
            mapped_arches = [arch for arch, group in groups.items() if group.get('cmake_source_prefix_map')]
            if mapped_arches:
                attrs['copts'] = CMakeCopts(attrs['copts'], sorted(mapped_arches))
            native = {
                arch: {'language': group['language'], 'includes': native_includes.get(arch, [])}
                for arch, group in groups.items() if group['language'] in {'C', 'CXX', 'ASM'}
            }
            if native:
                attrs['copts'] = CMakeNativeCopts(attrs['copts'], native)
            if kind == 'EXECUTABLE' and index == 0:
                binary_attrs = attrs
                if binary:
                    del binary_attrs['hdrs'], binary_attrs['textual_hdrs']
                # A binary and its first compile group share these features.
                binary_attrs['features'] = select({
                    '@platforms//cpu:' + arch: ['-static_link_cpp_runtimes', '-fortify_source',
                                                '-llvm_system_c_headers'] +
                    (['-default_compile_flags'] if name == 'clickhouse' else []) +
                    (['system_include_paths'] if system_includes.get(arch) else [])
                    for arch in ARCHES
                })
                continue
            languages = {g['language'] for g in groups.values()}
            if 'ASM_NASM' in languages:
                if len(languages) != 1:
                    raise ValueError('Mixed compiler languages for ' + name)
                native_nasm = {
                    arch: {'language': group['language'], 'includes': group.get('native_cxx_includes', [])}
                    for arch, group in groups.items()
                }
                attrs['copts'] = CMakeNativeCopts(attrs['copts'], native_nasm)
                nasm_name = name + '__nasm_' + str(index)
                self.rule('nasm_objects', nasm_name, {
                    **{field: attrs[field] for field in ['srcs', 'hdrs', 'copts', 'local_defines', 'deps']},
                    'target_compatible_with': compatible,
                })
                attrs = {'srcs': [':' + nasm_name]}
            self.rule('cc_library', name if collapse_single else name + '__compile_' + str(index), {
                **attrs,
                # These copts already contain the CMake compile group's flags.
                # The LLVM toolchain otherwise forces frame pointers in Release.
                'features': select({
                    '@platforms//cpu:' + arch: ['-fortify_source', '-default_compile_flags'] +
                    (['-llvm_system_c_headers'] if arch in native else []) +
                    (['system_include_paths'] if system_includes.get(arch) else [])
                    for arch in ARCHES
                }),
                'target_compatible_with': compatible,
                'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
            })
            if collapse_single:
                return
        deps = select({
            '@platforms//cpu:' + arch: [self.label(d) for d in v['deps']] + [
                ':' + name + '__compile_' + str(i)
                for i in range(1 if kind == 'EXECUTABLE' else 0, group_count)
            ]
            for arch, v in variants.items()
        })
        if kind == 'EXECUTABLE':
            # Header providers added by compile_inputs must reach binaries as
            # well as libraries. The original raw CMake group has no such deps.
            deps = Concat([deps, binary_attrs.pop('deps', [])])
            if name == 'clickhouse':
                self.rule('cc_library', 'clickhouse__compile_0', {
                    **binary_attrs,
                    'deps': deps,
                    'target_compatible_with': compatible,
                })
                self.rule('cmake_link', 'clickhouse', {
                    'features': ['-static_link_cpp_runtimes', '-fortify_source'],
                    'target_compatible_with': ['@platforms//os:linux'],
                })
                return
            self.rule('cc_test' if name == 'unit_tests_dbms' else 'cc_binary', name, {
                **binary_attrs,
                'deps': deps,
                'linkopts': select({'@platforms//cpu:' + arch: v['linkopts'] for arch, v in variants.items()}),
                'target_compatible_with': compatible,
                **({'env': {'OPENSSL_CONF': '/dev/null'}, 'size': 'enormous', 'linkstatic': True}
                   if name == 'unit_tests_dbms' else {}),
            })
        else:
            self.rule('cc_library', name, {'deps': deps, 'target_compatible_with': compatible})

    def load_statements(self):
        loads = dict(self.loads)
        if any('header_glob(' in rule for rule in self.rules):
            loads.setdefault('@clickhouse//bazel:ch_cc_library.bzl', set()).add('header_glob')
        paths = {function for function in ('repository_path', 'repository_local_include', 'llvm_toolchain_path', 'cmake_prefix_maps',
                                           'cmake_cxx_compiler_version')
                 if any(function + '(' in rule for rule in self.rules)}
        if any('CLICKHOUSE_WARNINGS' in rule for rule in self.rules):
            paths.add('CLICKHOUSE_WARNINGS')
        if paths:
            loads['@clickhouse//bazel:repository_paths.bzl'] = paths
        return ''.join('load(' + json.dumps(path) + ', ' + ', '.join(map(json.dumps, sorted(names))) + ')\n'
                       for path, names in sorted(loads.items()))

    def text(self):
        return compact('\n'.join(self.rules))


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
    for owner in ('contrib/pcodec', 'contrib/wasmtime', 'contrib/chdig'):
        build(owner).rule('filegroup', 'cmake_cargo_sources', {
            'srcs': Glob(['**'], ('**/BUILD', '**/BUILD.bazel', 'target/**')),
        })
    build('contrib/chdig').rule('filegroup', 'cargo_manifest', {'srcs': ['Cargo.toml']})
    build('contrib/openssl').rule('filegroup', 'headers', {'srcs': Glob(['include/**'])})
    build('contrib/sysroot').rule('cc_library', 'linux_aarch64_hwcap', {
        'hdrs': ['linux-aarch64/aarch64-linux-gnu/libc/usr/include/asm/hwcap.h'],
        'strip_include_prefix': 'linux-aarch64/aarch64-linux-gnu/libc/usr/include',
        'target_compatible_with': ['@platforms//cpu:aarch64'],
    })
    for arch in ARCHES:
        directory = 'linux-' + arch
        build('contrib/sysroot').rule('filegroup', 'linux_' + arch + '_link_files', {
            'srcs': Glob([directory + '/**'], (directory + '/**/include/**', directory + '/**/share/**')),
        })
        build('contrib/sysroot').rule('filegroup', 'linux_' + arch + '_compile_files', {
            'srcs': Glob([directory + '/**'], (directory + '/**/share/**',)),
        })
        build('contrib/sysroot').rule('filegroup', 'linux_' + arch + '_native_headers', {
            'srcs': Glob([directory + '/' + arch + '-linux-gnu/libc/usr/include/**/*.h']),
        })
    build('contrib/sysroot').rule('filegroup', 'linux_compile_files', {
        'srcs': select({'@platforms//cpu:' + arch: [':linux_' + arch + '_compile_files'] for arch in ARCHES}),
    })
    build('contrib/sysroot').rule('filegroup', 'linux_link_files', {
        'srcs': select({'@platforms//cpu:' + arch: [':linux_' + arch + '_link_files'] for arch in ARCHES}),
    })
    build('contrib/sysroot').rule('filegroup', 'linux_native_headers', {
        'srcs': select({'@platforms//cpu:' + arch: [':linux_' + arch + '_native_headers'] for arch in ARCHES}),
    })
    build('contrib/sysroot').rule('filegroup', 'linux_math_library', {
        'srcs': select({
            '@platforms//cpu:' + arch: ['linux-' + arch + '/' + arch + '-linux-gnu/libc/lib64/libm.so.6']
            for arch in ARCHES
        }),
    })
    krb5 = build('contrib/krb5')
    krb5.rule('perl_library', 'templates', {
        'srcs': Glob(['src/util/*.pm']), 'includes': ['src/util'],
    })
    for name, source in [('gen_map', 'gen-map.pl'), ('gen', 'gen.pl')]:
        krb5.rule('perl_binary', name, {'srcs': ['src/util/' + source], 'deps': [':templates']})
