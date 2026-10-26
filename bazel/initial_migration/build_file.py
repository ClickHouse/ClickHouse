"""Render the configured native graph as BUILD rules in its owning repositories."""

from dataclasses import dataclass
import ast
import json
from pathlib import Path
import re

from initial_migration import llvm_targets
from initial_migration.compact_build import compact
from initial_migration.normalize_cpp_builds import native_compile_defaults
from initial_migration.repository_inputs import (
    REPOSITORIES, ROOT, CC_HEADER_SUFFIXES, TEXTUAL_SOURCE_SUFFIXES, HeaderDirectory,
    configuration_owner, directory_sources, execution_path, package_source_files, package_sources,
    included_source_files, repository, source_label, source_path,
    textual_source_names,
)
from initial_migration.simplify_globs import GlobFiles, simplify_globs, simplify_source_globs

ARCHES = ('x86_64', 'aarch64')

# These CMake targets get their ClickHouse headers from their declared dependencies.
TARGETS_WITH_DIRECT_SOURCE_HEADERS = {
    'array_distance_transposed', 'benchmark_runtime_filter_objects',
    'clickhouse-benchmark-lib', 'clickhouse-client-lib', 'clickhouse-format-lib',
    'clickhouse-install-lib', 'clickhouse-keeper-lib', 'clickhouse-keeper-utils-lib',
    'clickhouse-local-lib', 'clickhouse-server-lib', 'clickhouse_common_zookeeper',
    'clickhouse_dictionaries__compile_1',
    'clickhouse_functions_array__compile_0', 'clickhouse_functions_array__compile_1',
    'clickhouse_functions_array__compile_2', 'clickhouse_functions_array__compile_3',
    'clickhouse_functions_gatherutils', 'clickhouse_functions_obj__compile_0',
    'clickhouse_functions_time_series', 'clickhouse_functions_uniqtheta',
    'clickhouse_table_functions', 'dbms__compile_0', 'dbms_embedded',
}
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
    # Abseil's include root contains its public headers and the CI options
    # header included in the captured CMake inputs. Publish both as headers.
    'contrib/abseil-cpp': ('absl', 'ci'),
    'contrib/aws': (
        'generated/src/aws-cpp-sdk-glue/include',
        'generated/src/aws-cpp-sdk-s3/include',
        'src/aws-cpp-sdk-core/include',
    ),
    'contrib/postgres': ('src/include', 'src/interfaces/libpq'),
    'contrib/libpqxx': ('include',),
    **{owner: ('include',) for owner in REPOSITORIES
       if (owner.startswith('contrib/aws-c-') and owner != 'contrib/aws-c-mqtt')
       or owner in {'contrib/aws-checksums', 'contrib/aws-crt-cpp'}},
}
SOURCE_HEADER_SUFFIXES = {
    'contrib/postgres': ('.h',),
    'contrib/libpqxx': ('.hxx',),
}
SOURCE_HEADER_TARGETS = {
    owner: (REPOSITORIES[owner]['name'].removeprefix('clickhouse_') + '_headers')
    for owner in SOURCE_HEADER_DIRECTORIES
}
PUBLIC_HEADERS.update({
    owner: (tuple(owner + '/' + directory + '/' for directory in directories), SOURCE_HEADER_TARGETS[owner])
    for owner, directories in SOURCE_HEADER_DIRECTORIES.items()
})

# Existing cc_library providers whose source includes have been checked for
# header-name collisions in both configured native graphs. Libfiu's directory
# cannot be published: Poco XML and libfiu both contain internal.h.
NATIVE_PUBLIC_INCLUDES = {
    ('contrib/zstd', '_zstd__compile_0'): 'lib',
    ('contrib/re2', '_re2'): '.',
    ('contrib/libgsasl', '_gsasl'): 'src',
    ('contrib/libssh', '_ssh'): 'include',
    ('contrib/cctz', '_cctz'): 'include',
    ('contrib/crc32c', '_crc32c'): 'include',
    ('contrib/libxml2', '_libxml2'): 'include',
    ('contrib/miniselect', 'miniselect'): 'include',
}

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
    node.value for node in ast.parse((ROOT / 'bazel/rules/repository_paths.bzl').read_text()).body
    if isinstance(node, ast.Assign)
    and any(isinstance(name, ast.Name) and name.id == 'CLICKHOUSE_WARNINGS' for name in node.targets)
))
_LIBRARY_FLAGS = {
    node.targets[0].id: ast.literal_eval(node.value)
    for node in ast.parse((ROOT / 'bazel/rules/ch_cc_library.bzl').read_text()).body
    if isinstance(node, ast.Assign) and isinstance(node.value, (ast.Dict, ast.List))
}
CLICKHOUSE_TARGET_FLAGS = _LIBRARY_FLAGS['CLICKHOUSE_TARGET_FLAGS']
CLICKHOUSE_XRAY_FLAGS = _LIBRARY_FLAGS['CLICKHOUSE_XRAY_FLAGS']
CLICKHOUSE_CODE_FLAGS = CLICKHOUSE_XRAY_FLAGS + _LIBRARY_FLAGS['_CLICKHOUSE_CXX_WARNINGS']

def clickhouse_code_flags(language):
    if language == 'ASM':
        return CLICKHOUSE_CODE_FLAGS
    standard = '-std=gnu11' if language == 'C' else '-std=c++23'
    flags = CLICKHOUSE_XRAY_FLAGS if language == 'C' else CLICKHOUSE_CODE_FLAGS
    return flags + ['-Wframe-larger-than=65536'] + CLICKHOUSE_WARNINGS + [standard]


def has_clickhouse_code_flags(group):
    language = group['language']
    if language not in GLOBAL_COMPILE_OPTIONS:
        return False
    prefix = clickhouse_code_flags(language)
    flags = remove_global_compile_options(group.get('copts', []), language)
    return any(flags[i:i + len(prefix)] == prefix for i in range(len(flags)))


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


@dataclass
class Concat:
    values: list


@dataclass
class LocalInclude:
    directory: str
    owner: str | None
    raw_llvm: bool = False


def split_system_includes(flags, owner, native_library, public_includes=(),
                          preserve_base_includes=False):
    options, includes, source_includes = [], [], []
    flags = iter(flags)
    for flag in flags:
        if flag == '-isystem':
            directory = next(flags)
            includes.append(LocalInclude(directory, owner))
        elif flag == '-I' and native_library and owner != llvm_targets.PREFIX:
            directory = next(flags)
            original = directory
            if owner is not None and directory.startswith('bazel/config/'):
                _, _, arch, relative = directory.split('/', 3)
                if relative.startswith('contrib/') and configuration_owner(relative) == owner:
                    directory = owner + '/config/' + arch + '/' + relative
            # CppKafka's quoted queue.h must precede the other system headers.
            if original == 'contrib/cppkafka/include/cppkafka':
                options.extend([flag, original])
            elif original in public_includes:
                continue
            elif (owner is None and not preserve_base_includes and
                  (original in {'base', 'src', 'programs'}
                   or original.startswith(('base/', 'src/', 'programs/'))
                   or re.fullmatch(r'bazel/config/(?:x86_64|aarch64)/base', original))):
                # -I source dirs have priority over every captured -isystem.
                source_includes.append(LocalInclude(directory, owner))
            elif owner is not None and repository(directory.removeprefix('$(GENDIR)/')) == owner:
                includes.append(LocalInclude(directory, owner))
            else:
                options.extend([flag, original])
        else:
            options.append(flag)
    return options, source_includes + includes


@dataclass(frozen=True)
class PackageEmbedDirectory:
    target: str
    directory: str


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
    if isinstance(value, PackageEmbedDirectory):
        return ('"--embed-dir=" + package_relative_label(' + json.dumps(':' + value.target)
                + ').workspace_root + ' + json.dumps('/' + value.directory))
    if isinstance(value, CMakeCopts):
        return (starlark(value.flags, indent, paths, step) + ' + cmake_prefix_maps('
                + json.dumps(value.arches) + ')')
    if isinstance(value, CMakeNativeCopts):
        choices = {'//conditions:default': []}
        for arch, group in value.groups.items():
            options = []
            for flag, origin, directory in group['includes']:
                root = llvm_targets.PREFIX
                if origin == 'generated':
                    root = '$(GENDIR)/' + root + '/gen/' + arch
                options.extend([flag, root + '/' + directory])
            choices['@platforms//cpu:' + arch] = options
        flags = starlark(value.flags, indent, paths, step)
        if not any(choices.values()):
            return flags
        return flags + ' + ' + starlark(Select(choices), indent, 'cmake_runtime', step)
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


def compile_group_name(name, index, sources):
    # Keep the one-file libc++ hardening group in src/Common on both CPUs.
    if name == 'clickhouse_common_io' and list(sources) == ['src/Common/IntervalTree.cpp']:
        return 'interval_tree'
    return f'{name}__compile_{index}'


def target_label(name, owners):
    owner = owners.get(name)
    return ('@' + REPOSITORIES[owner]['name'] + '//:' if owner else '//:') + name


# These CMake archive members read copied files through source-relative #embed.
EMBEDDED_SOURCES = {
    'clickhouse_common_io': ('src/Common/FrequencyHolder.cpp',),
    'dbms': (
        'src/Server/IndexRequestHandler.cpp',
        'src/Server/KeeperDashboardRequestHandler.cpp',
        'src/Server/KeeperJemallocHandler.cpp',
        'src/Server/WebTerminalRequestHandler.cpp',
        'src/Server/WebUIRequestHandler.cpp',
    ),
}


def embedded_indices(name, sources):
    """Find the source positions within the original CMake compile group."""
    wanted = EMBEDDED_SOURCES.get(name, ())
    result = [i for i, source in enumerate(sources) if source in wanted] if wanted else []
    if result and tuple(sources[i] for i in result) != wanted:
        raise ValueError(f'{name}: embedded sources changed or no longer share a CMake compile group')
    return result


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
                        'groups': [{**group, 'srcs': srcs, 'foreign_deps': [], 'embedded_inputs': {},
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

    def label(self, value, source_file=False):
        if value.startswith(':'):
            value = '//' + value
        if value.startswith('//:') and value[3:] in self.owners:
            value = target_label(value[3:], self.owners)
        if value == '@clickhouse_fastops//:_fastops__compile_0':
            value = '@clickhouse_fastops//:_fastops'
        if self.owner:
            prefix = '@' + REPOSITORIES[self.owner]['name'] + '//:'
            if value.startswith(prefix):
                self.local_sources.add(value)
                local = value.removeprefix(prefix)
                if source_file and local in package_source_files(self.owner):
                    return local
                return ':' + local
            if value.startswith('//:') and (ROOT / value[3:]).is_file():
                self.root_sources.add(value[3:])
            if value.startswith('//'):
                return '@clickhouse' + value
        if value.startswith('@'):
            self.external_inputs.add(value)
        return value

    def label_inputs(self, values, source_files=False):
        labels, parts = [], []
        for value in values:
            if isinstance(value, Glob):
                if labels:
                    parts.append(labels)
                    labels = []
                parts.append(value)
            else:
                labels.append(self.label(value, source_file=source_files))
        if labels or not parts:
            parts.append(labels)
        return parts[0] if len(parts) == 1 else Concat(parts)

    def dependency_headers(self, arch, dependencies):
        """Header directories and explicit headers exported by existing cc deps."""
        key = (arch, tuple(dependencies))
        if key in self.public_directories:
            return self.public_directories[key]
        targets = self.targets.get(arch, {})
        directories, headers, repositories, seen = set(), set(), set(), set()
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
            repositories.add(self.owners.get(target_name))
            pending.extend(target.get('deps', []))
            for dep_group in target.get('groups', []):
                if dep_group.get('cmake_runtime') or dep_group.get('language') == 'ASM_NASM':
                    continue
                pending.extend(dep_group.get('deps', []))
                headers.update(
                    item for field in ('hdrs', 'textual_hdrs') for item in dep_group.get(field, [])
                    if isinstance(item, str) and item.endswith((*CC_HEADER_SUFFIXES, '.th', '.defs'))
                )
                for field in ('hdrs', 'textual_hdrs'):
                    for item in dep_group.get(field, []):
                        if isinstance(item, HeaderDirectory):
                            repositories.add(repository(item.directory))
                        elif isinstance(item, str):
                            repositories.add(repository(source_path(item)))
                for item in dep_group.get('hdrs', []):
                    if (isinstance(item, HeaderDirectory)
                            and repository(item.directory) == self.owners.get(target_name)
                            # _boost_* substitutes the non-OpenSSL headers plus its own
                            # libs/<component>; it never exports all of boost/.
                            and not (item.directory == 'contrib/boost' and target_name.startswith('_boost_'))
                            and not self.header_consumers.get(item.directory, set()) <= {target_name}):
                        directories.add(item.directory.rstrip('/') + '/')
        result = tuple(sorted(directories)), frozenset(headers), frozenset(repositories), frozenset(seen)
        self.public_directories[key] = result
        return result

    def compile_inputs(self, name, group, binary=False, arch=None, library_deps=()):
        """Separate private headers, public headers, and non-compilable inputs."""
        fields = {key: list(group.get(key, [])) for key in
                  ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps')}
        nasm = group.get('language') == 'ASM_NASM'
        if nasm or (self.owner == 'contrib/isa-l' and group.get('language') == 'C'):
            headers = included_source_files(self.owner, group)
            fields['hdrs'] = [path for path in headers if nasm or path.startswith('include/')]
            if not nasm:
                fields['srcs'].extend(path for path in headers if not path.startswith('include/'))
            fields['textual_hdrs'] = []
            fields['additional_compiler_inputs'] = []
            return fields
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
        custom = group.get('cmake_runtime')
        supplied, supplied_labels, supplied_repos, _ = ((), (), frozenset(), frozenset()) if custom else self.dependency_headers(
            arch, tuple(group.get('deps', ())) + tuple(library_deps))
        for old_field, directories in original.items():
            for directory in directories:
                owner = repository(directory)
                if old_field == 'hdrs' and directory == self.owner == 'contrib/minizip-ng' and name == '_minizip':
                    # Minizip installs each stream header with its selected compression backend.
                    headers = {Path(source_path(src)).stem + '.h' for src in fields['srcs']
                               if isinstance(src, str) and src.endswith('.c')}
                    public = (headers & package_source_files(self.owner)) | {'mz.h', 'mz_config.h', 'mz_strm_os.h'}
                    fields['hdrs'].extend([Glob(['compat/*.h']), *sorted(public)])
                    continue
                if (old_field == 'hdrs' and owner == self.owner == 'contrib/icu' and name == '_icuuc'
                        and directory == 'contrib/icu/icu4c/source/common'):
                    # ICU installs unicode/*.h. Its genbrk tool also includes rbbidata.h.
                    common = 'icu4c/source/common/'
                    private = [common + header for header in (
                        '*_data.h', 'bmpset.h', 'charstrmap.h', 'localsvc.h', 'locdistance.h',
                        'loclikelysubtags.h', 'lsr.h', 'norm2allmodes.h', 'rbbi*.h', 'ruleiter.h',
                        'ubidiimp.h', 'ucptrie_impl.h', 'uniquecharstr.h', 'unisetspan.h', 'utrie2_impl.h',
                    )]
                    shared = common + 'rbbidata.h'
                    fields['srcs'].append(Glob(private, [shared]))
                    fields['hdrs'].extend([Glob([common + '**/*.h'], private), shared])
                    continue
                if directory == 'contrib/zstd/lib' and owner == self.owner and name == '_zstd':
                    # zstd installs lib/*.h; also retain its deprecated zbuff API.
                    if group['language'] == 'C':
                        fields['hdrs'].append(Glob(['lib/*.h', 'lib/deprecated/*.h']))
                        fields['srcs'].append(Glob([
                            'lib/common/*.h', 'lib/compress/*.h', 'lib/decompress/*.h',
                            'lib/dictBuilder/*.h', 'lib/legacy/*.h',
                        ], ('lib/common/portability_macros.h',)))
                    elif group['language'] == 'ASM':
                        fields['hdrs'].append('lib/common/portability_macros.h')
                    else:
                        raise ValueError("Unexpected zstd compile language: " + group['language'])
                    continue
                if directory == 'contrib/arrow/cpp/src' and owner != self.owner:
                    # Search directories do not make their entire contents compiler inputs.
                    continue
                field = ('srcs' if binary or self.header_consumers.get(directory, set()) <= {name} else 'hdrs')
                if not custom and owner == 'contrib/boost':
                    if owner != self.owner:
                        fields['deps'].append('@clickhouse_boost//:boost_headers_no_openssl')
                        continue
                    if directory == owner and name.startswith('_boost_'):
                        fields['deps'].append('@clickhouse_boost//:boost_headers_no_openssl')
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
                        fields[field].append(Glob([pattern], excluded))
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
                        if (owner in SOURCE_HEADER_SUFFIXES and relative in SOURCE_HEADER_DIRECTORIES[owner]
                                and not local):
                            fields['deps'].append(':' + SOURCE_HEADER_TARGETS[owner])
                        else:
                            fields[field].append(Glob(
                                headers, (*excluded, *local)))
                        fields[non_header_field].append(Glob([pattern], (*excluded, *headers)))
                else:
                    for path in directory_sources(directory):
                        # vec.th belongs to upstream tests; mongoc-counters.defs belongs to libmongoc.
                        if not custom and owner == 'contrib/mongo-c-driver' and path in {
                            'contrib/mongo-c-driver/src/common/src/mlib/vec.th',
                            'contrib/mongo-c-driver/src/libmongoc/src/mongoc/mongoc-counters.defs',
                        }:
                            continue
                        public = PUBLIC_HEADERS.get(owner)
                        pqxx_wrapper = (
                            owner == 'contrib/libpqxx'
                            and path.rsplit('/', 1)[0] == owner + '/include/pqxx'
                            and '.' not in path.rsplit('/', 1)[-1]
                        )
                        if (not custom and public and path.startswith(public[0])
                                and (path.endswith(SOURCE_HEADER_SUFFIXES.get(owner, CC_HEADER_SUFFIXES))
                                     or pqxx_wrapper)):
                            fields['deps'].append('@' + REPOSITORIES[owner]['name'] + '//:' + public[1])
                            continue
                        # This pinned Abseil revision has no non-header
                        # textual includes. stacktrace_config.h selects .inc
                        # files, which abseil_cpp_headers already supplies.
                        if not custom and owner == 'contrib/abseil-cpp':
                            continue
                        target_field = field if custom or path.endswith(CC_HEADER_SUFFIXES) else non_header_field
                        if (target_field in {'hdrs', 'additional_compiler_inputs'}
                                and path.endswith(CC_HEADER_SUFFIXES)
                                and path.startswith(supplied)):
                            continue
                        fields[target_field].append(source_label(path))
        if not custom and self.external_headers is not None:
            self.external_headers.use(fields, self.owner)
        if not custom and not binary:
            if self.owner == 'contrib/orc' and name == '_orc':
                # ORC installs c++/include/orc/. Its generated Protobuf and
                # Adaptor headers are implementation inputs in c++/src/CMakeLists.txt.
                private = [item for item in fields['hdrs'] if isinstance(item, str)
                           and item.endswith(('/contrib/arrow-cmake/orc_proto.pb.h',
                                              '/contrib/orc/c++/include/Adaptor.hh'))]
                fields['srcs'].extend(private)
                fields['hdrs'] = [item for item in fields['hdrs'] if item not in private]
            if self.owner == 'contrib/cassandra' and name == '_cassandra':
                # CMake's binary directory contains the private driver configuration.
                config = [item for item in fields['hdrs']
                          if isinstance(item, str) and item.endswith('/contrib/cassandra-cmake/driver_config.hpp')]
                fields['srcs'].extend(config)
                fields['hdrs'] = [item for item in fields['hdrs'] if item not in config]
            for field in ('hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                fields[field] = [
                    item for item in fields[field]
                    if not isinstance(item, str) or item not in supplied_labels
                ]
            if self.owner == 'contrib/mongo-cxx-driver' and name in {'_bsoncxx', '_mongocxx'}:
                # The pinned Mongo C++ sources include none of Mongo C's .c
                # sources, private textual includes, or build data.
                fields['additional_compiler_inputs'] = [
                    item for item in fields['additional_compiler_inputs']
                    if not isinstance(item, str)
                    or not item.startswith('@clickhouse_mongo_c_driver//:')
                ]
        if not custom:
            # PostgreSQL and PQXX publish their headers through cc_library.
            # Their other captured include-directory files are PostgreSQL
            # catalog data, translations, scripts and non-selected sources,
            # not compiler inputs for a different repository. Codegen rules
            # and these repositories' own compile groups retain their inputs.
            foreign_postgres = tuple(
                '@' + REPOSITORIES[owner]['name'] + '//:'
                for owner in ('contrib/postgres', 'contrib/libpqxx')
                if owner != self.owner
            )
            fields['additional_compiler_inputs'] = [
                item for item in fields['additional_compiler_inputs']
                if not isinstance(item, str) or not item.startswith(foreign_postgres)
            ]
            if self.owner == 'contrib/postgres' and name == '_libpq':
                # libpq uses postgres_headers and OpenSSL's published headers.
                # The CMake configuration headers are already in hdrs. Files
                # used to generate OpenSSL headers aren't inputs to libpq.
                fields['additional_compiler_inputs'] = []
        if not custom and self.owner != 'contrib/abseil-cpp':
            prefix = '@' + REPOSITORIES['contrib/abseil-cpp']['name'] + '//:'
            for field in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs'):
                kept = []
                for item in fields[field]:
                    if isinstance(item, str) and item.startswith(prefix):
                        filename = item.removeprefix(prefix)
                        if filename.startswith(('absl/', 'ci/')) and filename.endswith(CC_HEADER_SUFFIXES):
                            fields['deps'].append(prefix + SOURCE_HEADER_TARGETS['contrib/abseil-cpp'])
                            continue
                        if field == 'additional_compiler_inputs' and not filename.startswith(('config/', 'gen/')):
                            continue
                    kept.append(item)
                fields[field] = kept
        if not custom and not any(
            isinstance(flag, str) and flag.startswith(
                ('-include', '-imacros', '-ivfsoverlay')
            )
            for flag in group.get('copts', ())
        ):
            # An input might be included by a header from another repository.
            # Scan all repositories providing this compile group's headers.
            related = {None, self.owner, *supplied_repos}
            related.update(repository(directory) for paths in original.values() for directory in paths)
            for field in ('srcs', 'hdrs', 'textual_hdrs'):
                related.update(
                    repository(source_path(item)) for item in fields[field] if isinstance(item, str)
                )
            related.difference_update({'contrib/sysroot', llvm_targets.PREFIX})
            def source_data(path, owner):
                relative = path.removeprefix(owner + '/') if owner else path
                # These root directories are scanned by
                # repository_inputs.textual_source_names(None).
                if owner is None and not (
                    relative.startswith(('src/', 'base/', 'programs/'))
                    or re.match(r'contrib/[^/]+-cmake/', relative)
                ):
                    return False
                return (
                    not relative.startswith(('config/', 'gen/', 'bazel/'))
                    and '/gen/' not in relative
                    and not relative.endswith(TEXTUAL_SOURCE_SUFFIXES)
                    and not Path(relative).name.lower().startswith(('version', 'config.', 'config_'))
                    and relative in package_source_files(owner)
                )
            candidates = {
                item for item in fields['additional_compiler_inputs']
                if isinstance(item, str) and repository(source_path(item)) not in {'contrib/sysroot', llvm_targets.PREFIX}
                and source_data(source_path(item), repository(source_path(item)))
            }
            globs = self.owner not in {'contrib/sysroot', llvm_targets.PREFIX} and any(
                isinstance(item, Glob) for item in fields['additional_compiler_inputs']
            )
            if candidates or globs:
                related.update(repository(source_path(item)) for item in candidates)
                included = frozenset().union(*(textual_source_names(owner) for owner in related))
                kept = []
                files = GlobFiles(package_sources(self.owner)) if globs else None
                for item in fields['additional_compiler_inputs']:
                    if isinstance(item, str):
                        if item not in candidates or Path(source_path(item)).name in included:
                            kept.append(item)
                    elif globs and isinstance(item, Glob):
                        original_files = files.result(item.patterns, item.exclude)
                        data = [
                            path for path in original_files
                            if Path(path).name in included
                            or not source_data(self.owner + '/' + path if self.owner else path, self.owner)
                        ]
                        # Keep large globs rather than listing hundreds of
                        # files or adding another list of exclusions.
                        if len(data) != len(original_files) and (
                            len(data) <= 32 or len(data) <= 256 and len(data) * 3 < len(original_files)
                        ):
                            kept.extend(source_label(self.owner + '/' + path if self.owner else path)
                                        for path in data)
                        else:
                            kept.append(item)
                    else:
                        kept.append(item)
                fields['additional_compiler_inputs'] = kept
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

    def embedded_rule(self, name, groups, attrs):
        """Move only the six #embed compiles and their source-relative assets."""
        child = dict(attrs)
        fields = {key: {} for key in ('srcs', 'implementation_deps', 'additional_compiler_inputs')}
        private_sources, private_inputs, private_deps = {}, {}, {}
        def header(value):
            patterns = value.patterns if isinstance(value, Glob) else [value]
            return all(isinstance(p, str) and p.endswith(CC_HEADER_SUFFIXES) for p in patterns)
        for arch, group in groups.items():
            key = '@platforms//cpu:' + arch
            def values(field):
                value = attrs.get(field, [])
                value = value.values[key] if isinstance(value, Select) else value
                parts = value.values if isinstance(value, Concat) else [value]
                return [v for part in parts for v in (part if isinstance(part, list) else [part])]
            assets = group['embedded_inputs']
            embedded = [self.label(v, source_file=True) for v in assets]
            owned = {self.label(v) for items in assets.values() for v in items}
            sources = {self.label(v, source_file=True) for v in group['srcs']}
            if ([v for v in values('srcs') if isinstance(v, str) and v in embedded] != embedded or
                    not owned <= {v for v in values('additional_compiler_inputs') if isinstance(v, str)}):
                raise ValueError(f'{name} {arch}: #embed source or asset missing from the CMake compile action')
            fields['srcs'][key] = self.label_inputs([v for v in values('srcs') if v not in embedded], source_files=True)
            fields['additional_compiler_inputs'][key] = self.label_inputs(
                [v for v in values('additional_compiler_inputs') if not isinstance(v, str) or v not in owned])
            fields['implementation_deps'][key] = [*values('implementation_deps'), ':' + name + '_embedded']
            # Only header files can move from srcs: an unrecognized .cpp would
            # otherwise become another object in the child archive.
            inherited = [v for v in values('srcs') if not isinstance(v, str) or v not in sources]
            for v in inherited:
                if not header(v) and (not isinstance(v, str) or Path(v).suffix):
                    raise ValueError(f'{name} {arch}: unexpected CMake compile input in srcs: {v}')
            declared = values('hdrs') + values('textual_hdrs')
            private_sources[key] = self.label_inputs(
                embedded + [v for v in inherited + declared if header(v)], source_files=True)
            # An extensionless generator or filegroup may supply a header.
            # Retain its files without compiling unclassified outputs.
            private_inputs[key] = self.label_inputs(
                values('additional_compiler_inputs') + [v for v in inherited + declared if not header(v)])
            private_deps[key] = list(dict.fromkeys(values('deps') + values('implementation_deps')))
        if set(groups) != set(ARCHES):
            raise ValueError(f'{name}: embedded compile group must be present on both native CPUs')
        attrs.update({field: select(branches) for field, branches in fields.items()})
        for field in ('hdrs', 'textual_hdrs', 'deps'):
            child.pop(field, None)
        child.update(srcs=select(private_sources), additional_compiler_inputs=select(private_inputs),
                     implementation_deps=select(private_deps))
        return child

    def rule(self, kind, name, attrs):
        self.names.add(name)
        copts = attrs.get('copts')
        if (kind == 'cc_library' and isinstance(copts, CMakeNativeCopts)
                and all(group['language'] in GLOBAL_COMPILE_OPTIONS for group in copts.groups.values())):
            kind = 'ch_cc_library'
            self.loads.setdefault('@clickhouse//bazel/rules:ch_cc_library.bzl', set()).add(kind)
            languages = {group['language'] for group in copts.groups.values()}
            if len(languages) != 1:
                raise ValueError(f'{name}: expected one native compiler language, got {languages}')
            language = next(iter(languages))
            if (language == 'CXX' and attrs.get('clickhouse_code', False)
                    and name not in TARGETS_WITH_DIRECT_SOURCE_HEADERS):
                attrs.setdefault('source_headers', True)
            if language != 'CXX':
                attrs['language'] = language
            arches = list(copts.groups)
            if set(arches) != set(ARCHES):
                attrs['arches'] = arches
            expected_includes = [] if language == 'CXX' else [['-I', 'generated', 'libcxx/include/c++/v1']]
            includes = [group['includes'] for group in copts.groups.values()]
            if any(includes) and not all(value == expected_includes for value in includes):
                raise ValueError(f'{name}: unsupported native libc++ includes: {includes}')
            flags = copts.flags
            if isinstance(flags, CMakeCopts) and set(flags.arches) == set(arches):
                flags = flags.flags
            else:
                # If a future CMake group uses source maps on only one CPU,
                # retain that select instead of applying the macro's default.
                attrs['map_source_files'] = False
            attrs['copts'] = flags
            defaults = {'-fortify_source', '-default_compile_flags'}
            features = attrs.get('features', [])
            if isinstance(features, Select):
                attrs['features'] = select({key: [flag for flag in value if flag not in defaults]
                                            for key, value in features.values.items()})
            else:
                attrs['features'] = [flag for flag in features if flag not in defaults]
        if self.owner == 'contrib/krb5' and name == '_krb5':
            attrs['includes'] = ['src/include', 'config/contrib/krb5-cmake/include']
        if self.owner == 'contrib/openssl' and name == 'crypto':
            # crypto's compile groups also use private OpenSSL directories.
            # Only these two roots belong to the published OpenSSL interface.
            attrs['includes'] = [
                LocalInclude('contrib/openssl-cmake/common/include', self.owner),
                'include',
            ]
        if kind in {'cc_library', 'ch_cc_library'} and (self.owner, name) in NATIVE_PUBLIC_INCLUDES:
            attrs['includes'] = attrs.get('includes', []) + [NATIVE_PUBLIC_INCLUDES[self.owner, name]]
        if kind in {'cc_library', 'cc_binary', 'cc_test', 'cc_import'}:
            self.loads.setdefault('@rules_cc//cc:' + kind + '.bzl', set()).add(kind)
        elif kind == 'rust_vendor_licenses':
            self.loads.setdefault('@clickhouse//bazel/rules:codegen_actions.bzl', set()).add(kind)
        elif kind == 'nasm_objects':
            self.loads.setdefault('@clickhouse//bazel/rules:nasm.bzl', set()).add(kind)
        elif kind == 'cmake_link':
            self.loads.setdefault('@clickhouse//bazel/rules:cmake_link.bzl', set()).add(kind)
        elif kind == 'llvm_runtime_objects':
            runtime = '@clickhouse//bazel/rules:llvm_runtime.bzl'
            if runtime not in self.loads:
                arch = Select({'@platforms//cpu:' + arch: arch for arch in ARCHES})
                self.rules.append('_RUNTIME_ARCH = ' + starlark(arch) + '\n')
            self.loads.setdefault(runtime, set()).add(kind)
        elif kind.startswith('perl_'):
            self.loads.setdefault('@rules_perl//perl:perl.bzl', set()).add(kind)
        elif kind in {'copy_file', 'expand_template', 'write_file'}:
            self.loads.setdefault('@bazel_skylib//rules:' + kind + '.bzl', set()).add(kind)
        if self.owner == llvm_targets.PREFIX and kind in ('cc_library', 'ch_cc_library'):
            llvm_targets.component_headers(name, attrs)
        lines = [kind + '(\n    name = ' + json.dumps(name) + ',\n']
        if kind == 'llvm_runtime_objects':
            lines.append('    arch = _RUNTIME_ARCH,\n')
        defaults = native_compile_defaults(attrs.get('srcs'), attrs.get('language', 'CXX'),
                                          attrs.get('clickhouse_code', False)) if kind == 'ch_cc_library' else {}
        # CMake does not inherit the Bazel macro's defaults when a group has no options.
        for key in defaults:
            attrs.setdefault(key, [])
        for key, value in attrs.items():
            if value == [] and key not in defaults:
                continue
            paths = key in {'copts', 'local_defines', 'linkopts'}
            if kind == 'llvm_runtime_objects' and paths:
                paths = 'cmake_runtime'
            lines.append('    ' + key + ' = ' + starlark(value, 4, paths=paths, step=4) + ',\n')
        self.rules.append(''.join(lines) + ')\n')

    def croaring(self, variants):
        # CMake links clickhouse_common_io to CRoaring. The pinned C sources use only CRoaring's C headers.
        def inputs(variant):
            groups = []
            for group in variant['groups']:
                if group['language'] != 'C':
                    raise ValueError('Expected CRoaring C sources')
                flags = group['copts']
                srcs = tuple(self.label(s, source_file=True) for s in group['srcs'])
                copts = tuple(f for f in flags if f == '-w' or f.startswith('-std='))
                forced = tuple(flags[i + 1] for i, flag in enumerate(flags) if flag == '-include')
                groups.append((srcs, copts, forced))
            return tuple(groups)

        ((core, core_flags, core_headers), (aligned, aligned_flags, aligned_headers)), = {
            inputs(variant) for variant in variants.values()
        }
        header = 'contrib/croaring-cmake/page_aligned_functions.h'
        if core_headers or aligned != ('src/roaring.c',) or aligned_headers != (header,):
            raise ValueError('Expected only roaring.c to force page_aligned_functions.h')
        self.loads.setdefault('@clickhouse//bazel/rules:ch_cc_library.bzl', set()).add('ch_cc_library')
        self.rule('ch_cc_library', '_roaring__compile_0', {
            'srcs': list(core), 'hdrs': Glob(['include/**/*.h']), 'includes': ['include'],
            'language': 'C', 'copts': list(core_flags),
        })
        self.rule('ch_cc_library', '_roaring__compile_1', {
            'srcs': [*aligned, '@clickhouse//contrib/croaring-cmake:page_aligned_functions.h'],
            'language': 'C', 'copts': [*aligned_flags, '-include', header], 'implementation_deps': [':_roaring__compile_0'],
        })
        self.rule('cc_library', '_roaring', {
            'hdrs': Glob(['cpp/**/*.hh']), 'includes': ['cpp'], 'deps': [':_roaring__compile_0', ':_roaring__compile_1'],
        })

    def target(self, name, variants, input_name=None):
        if (self.owner, name) == ('contrib/croaring', '_roaring'):
            self.croaring(variants)
            return
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
        llvm_runtime = self.owner == llvm_targets.PREFIX and name in llvm_targets.NATIVE_RUNTIMES
        math_archive = llvm_runtime and name == 'libllvmlibc'
        if math_archive and any(len(v['groups']) != 1 or not v['groups'][0].get('cmake_runtime')
                                for v in variants.values()):
            raise ValueError('Rust localization requires one libllvmlibc archive per CPU')
        fastops = self.owner == 'contrib/fastops' and name == '_fastops'
        if fastops and (kind != 'STATIC_LIBRARY' or
                        {arch: len(variant['groups']) for arch, variant in variants.items()} !=
                        {'x86_64': 1, 'aarch64': 2} or
                        any(group['language'] != 'CXX'
                            for variant in variants.values() for group in variant['groups'])):
            raise ValueError('Expected native FastOps and a separate ARM-only SVE compile group')
        collapse_single = group_count == 1 and kind != 'EXECUTABLE' and all(
            group['language'] in GLOBAL_COMPILE_OPTIONS and not group.get('cmake_runtime')
            for variant in variants.values() for group in variant['groups']
        )
        binary_attrs = {}
        group_names = []
        for index in range(group_count):
            groups = {arch: v['groups'][index] for arch, v in variants.items() if index < len(v['groups'])}
            group_name, = {compile_group_name(name, index, (source_path(src) for src in g['srcs']))
                           for g in groups.values()}
            group_names.append(group_name)
            sve = fastops and index == 1
            attribute_arches = tuple(groups) if sve else ARCHES
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
                group_compatible = ['@platforms//cpu:' + next(iter(groups))] if len(groups) == 1 else compatible
                attrs = {}
                for field in ('srcs', 'hdrs', 'copts', 'local_defines'):
                    values = {}
                    for arch, group in groups.items():
                        value = inputs[arch][field] if field in LABEL_ATTRIBUTES else group.get(field, [])
                        if field in LABEL_ATTRIBUTES:
                            value = self.label_inputs(value, source_files=field == 'srcs')
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
                    '@platforms//cpu:' + arch: group['language'] for arch, group in groups.items()
                })
                object_name = name + '__runtime_objects_' + str(index)
                self.rule('llvm_runtime_objects', object_name, {
                    **attrs,
                    'target_compatible_with': group_compatible,
                })
                archive_objects = [':' + object_name]
                if name == 'clang_rt_builtins' and index > 0:
                    if index > 1:
                        continue
                    archive_objects = Concat([archive_objects, select({
                        '@platforms//cpu:x86_64': [],
                        '@platforms//cpu:aarch64': [':clang_rt_builtins_outline_objects'],
                    })])
                self.rule('cc_library', name if math_archive else name + '__compile_' + str(index), {
                    'srcs': archive_objects,
                    'deps': select({
                        '@platforms//cpu:' + arch: (
                            ([self.label(d) for d in variants[arch]['deps']] if math_archive else []) +
                            group.get('runtime_link_deps', [])
                        )
                        for arch, group in groups.items()
                    }),
                    'linkopts': select({'@platforms//cpu:' + arch: v['linkopts'] for arch, v in variants.items()})
                    if math_archive else [],
                    'target_compatible_with': group_compatible,
                    'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
                })
                if math_archive:
                    return
                continue
            attrs = {}
            native_library = kind != 'EXECUTABLE' or index != 0 or name == 'clickhouse'
            clickhouse_code = native_library and bool(groups) and all(
                has_clickhouse_code_flags(group) for group in groups.values()
            )
            system_includes = {}
            native_includes = {}
            postgres = self.owner == 'contrib/postgres' and name == '_libpq'
            nuraft = self.owner == 'contrib/NuRaft' and name == '_nuraft'
            implementation_deps = {'@platforms//cpu:' + arch: list(group['implementation_deps'])
                                   for arch, group in groups.items() if group.get('implementation_deps')}
            for arch, group in groups.items():
                if self.owner == 'contrib/libhdfs3' and name == '_hdfs3' and group['language'] == 'CXX':
                    implementation_deps.setdefault('@platforms//cpu:' + arch, []).append(':hdfs3_proto_headers')
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
                for arch in attribute_arches:
                    group = groups.get(arch, {})
                    value = inputs.get(arch, {}).get(field, []) if field in inputs.get(arch, {}) else group.get(field, [])
                    if field == 'deps' and kind != 'EXECUTABLE' and arch in variants:
                        value = compile_dependencies(value, variants[arch]['deps'])
                        if collapse_single:
                            value = value + variants[arch]['deps']
                    if field == 'deps' and group.get('header_deps'):
                        # Preserve CMake's original dependency order, including the executable wrapper.
                        linked = {self.label(dep) for dep in value}
                        if binary:
                            linked.update(self.label(dep) for dep in variants[arch]['deps'])
                        value += [dep for dep in group['header_deps'] if self.label(dep) not in linked]
                    value = value + group.get('foreign_deps', [])
                    if field == 'copts':
                        value = compile_options(value, group.get('deps', []))
                        language = group.get('language')
                        if language in {'C', 'CXX'}:
                            last_w = max((i for i, flag in enumerate(value) if flag == '-w'), default=-1)
                            value = [flag for i, flag in enumerate(value) if flag != '-w' or i == last_w]
                        if arch in system_includes:
                            public_includes = set()
                            if native_library and self.owner not in {
                                llvm_targets.PREFIX, 'contrib/arrow', 'contrib/cppkafka',
                            }:
                                deps = compile_dependencies(inputs[arch]['deps'], variants[arch]['deps'])
                                if collapse_single:
                                    deps = deps + variants[arch]['deps']
                                deps = deps + group.get('foreign_deps', [])
                                _, _, _, reachable = self.dependency_headers(arch, tuple(deps))
                                for (owner, provider), relative in NATIVE_PUBLIC_INCLUDES.items():
                                    ref = '@' + REPOSITORIES[owner]['name'] + '//:' + provider
                                    cmake_name = provider.split('__compile_', 1)[0]
                                    # miniselect is emitted by HeaderLibraries,
                                    # so it has no corresponding CMake target.
                                    if owner != self.owner and (ref in deps or
                                            self.owners.get(cmake_name) == owner and cmake_name in reachable):
                                        public_includes.add((owner + '/' + relative).removesuffix('/.'))
                            value, includes = split_system_includes(
                                value, self.owner, native_library, public_includes,
                                # NativeSnapshots.base_includes moves common's
                                # source and generated directories together.
                                preserve_base_includes=self.owner is None and name == 'common')
                            system_includes[arch] = includes + system_includes[arch]
                        if language == 'CXX':
                            # LLVM supplies libc++ for native libraries and executables.
                            value = [flag for flag in value if flag != '-nostdinc++']
                        if native_library and language in GLOBAL_COMPILE_OPTIONS:
                            # These original CMake C groups have no -Werror.
                            if language == 'C' and name in {'glibc-compatibility', 'harmful'}:
                                value = value + ['-Wno-error']
                            value = remove_global_compile_options(value, language)
                            value = [flag for flag in value if flag not in CLICKHOUSE_TARGET_FLAGS[arch]]
                            if clickhouse_code:
                                prefix = clickhouse_code_flags(language)
                                if value[:len(prefix)] != prefix:
                                    raise ValueError(f'{name} {arch}: ch_cc_library {language} flags are not the expected prefix')
                                value = value[len(prefix):]
                            if self.owner:
                                prefix = '--embed-dir=' + self.owner + '/'
                                value = [PackageEmbedDirectory(name, flag.removeprefix(prefix))
                                         if flag.startswith(prefix) else flag for flag in value]
                            if language in {'C', 'CXX'} and '-Weverything' in value:
                                start = value.index('-Weverything')
                                end = start + len(CLICKHOUSE_WARNINGS)
                                if value[start:end] == CLICKHOUSE_WARNINGS:
                                    value = CMakeWarnings(value[:start], value[end:])
                    if self.owner == 'contrib/libhdfs3' and name == '_hdfs3' and field in {
                        'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs',
                    }:
                        prefix = '//bazel:gen/' + arch + '/contrib/libhdfs3-cmake/'
                        value = [v for v in value if not (isinstance(v, str) and v.startswith(prefix)
                                                          and v.endswith('.pb.h'))]
                    if (field == 'hdrs' and name == 'clickhouse_aggregate_functions' and arch == 'x86_64'
                            and group.get('srcs') == ['//:src/AggregateFunctions/AggregateFunctionUniq.cpp']):
                        # The -mno-bmi group consumes the package headers privately.
                        # Retain foreign headers from CMake's broader include directories.
                        package = 'src/AggregateFunctions/'
                        value = [Glob(item.patterns, (*item.exclude, package + '**/*.h')) if isinstance(item, Glob) else item
                                 for item in value if not isinstance(item, str) or not (
                                     item.endswith('.h') and source_path(item).startswith(package))]
                        implementation_deps.setdefault('@platforms//cpu:' + arch, []).append('//src/AggregateFunctions:headers')
                    if (field == 'additional_compiler_inputs' and group.get('language') in GLOBAL_COMPILE_OPTIONS
                            and group.get('native_cxx_includes')):
                        value += [
                            '@clickhouse_llvm_project//:runtime_source_inputs',
                            '@clickhouse_llvm_project//:runtime_generated_headers',
                        ]
                    if self.owner in {'contrib/krb5', 'contrib/libhdfs3'}:
                        public = ('contrib/krb5-cmake/include/com_err.h', 'contrib/krb5-cmake/include/krb5/krb5.h')
                        if field in {'hdrs', 'textual_hdrs', 'additional_compiler_inputs'}:
                            value = [label for label in value if not label.endswith(public)]
                            if name == '_krb5' and field == 'hdrs':
                                value += ['@clickhouse_krb5//:config/' + arch + '/' + public[0],
                                          '@clickhouse_krb5//:config/' + public[1]]
                        elif name == '_hdfs3' and index == 0 and field == 'deps':
                            value = [label for label in value if label != '@clickhouse_krb5//:_krb5']
                            implementation_deps.setdefault('@platforms//cpu:' + arch, []).append(
                                '@clickhouse_krb5//:_krb5')
                    if field in LABEL_ATTRIBUTES:
                        value = self.label_inputs(value, source_files=field == 'srcs')
                        if field == 'deps' and collapse_single:
                            # The compile dependencies include public header
                            # libraries. Keep them before the old wrapper's
                            # dependencies and deduplicate after rebasing.
                            value = list(dict.fromkeys(value))
                    if field == 'deps' and (postgres or nuraft) and arch in groups:
                        if not collapse_single:
                            raise ValueError(f'{name} must have one native compile group')
                        openssl = {
                            self.label(target_label(dep, self.owners))
                            for dep in ('crypto', 'ssl')
                        }
                        supplied = [dep for dep in value if dep in openssl]
                        # src/asio_service.cxx uses OpenSSL only when NuRaft's TLS support is enabled.
                        if (postgres or supplied) and set(supplied) != openssl:
                            raise ValueError(f'{name} must link both OpenSSL libraries for TLS: {value}')
                        if nuraft:
                            # asio_service.cxx uses both <asio/ip/address.hpp> and <boost/asio.hpp>.
                            # Only the complete Boost library exports the extra 'boost' include directory.
                            boost = [
                                '@clickhouse_boost//:boost_headers',
                                '@clickhouse_boost//:boost_headers_no_openssl',
                            ]
                            value = [dep for dep in value if dep not in boost]
                            supplied.extend(boost)
                        if supplied:
                            implementation_deps['@platforms//cpu:' + arch] = supplied
                        value = [dep for dep in value if dep not in openssl]
                    values['@platforms//cpu:' + arch] = value
                attrs[field] = select(values)
            if self.owner == 'contrib/librdkafka' and name == '_rdkafka':
                # The ClickHouse wrapper includes this C source to inspect its static configuration table.
                wrapper = 'contrib/librdkafka-cmake/rdkafka_conf_sensitive.c'
                attrs['private_textual_srcs'] = select({
                    '@platforms//cpu:' + arch: ['src/rdkafka_conf.c'] if any(
                        source_path(src) == wrapper for src in groups.get(arch, {}).get('srcs', ())) else []
                    for arch in attribute_arches
                })
            if postgres:
                foreign = {
                    'contrib/openssl-cmake/common/include',
                    'contrib/openssl/include',
                    llvm_targets.PREFIX + '/libcxxabi/include',
                }
                for arch, includes in system_includes.items():
                    supplied = {inc.directory for inc in includes if inc.directory in foreign}
                    if supplied != foreign:
                        raise ValueError(f'Postgres _libpq {arch} changed its OpenSSL or ABI includes: {supplied}')
                    system_includes[arch] = [inc for inc in includes if inc.directory not in foreign]
            elif native_library and self.owner not in {None, 'contrib/openssl'}:
                # crypto publishes these public include roots. Only remove a
                # captured local copy when this compile group already reaches
                # crypto through its own native library dependencies.
                openssl_includes = {'contrib/openssl-cmake/common/include', 'contrib/openssl/include'}
                for arch, includes in system_includes.items():
                    group = groups[arch]
                    if group['language'] not in {'C', 'CXX'}:
                        continue
                    deps = tuple(group.get('deps', ()))
                    if collapse_single:
                        deps += tuple(variants[arch]['deps'])
                    _, _, _, reachable = self.dependency_headers(arch, deps)
                    if 'crypto' in reachable:
                        system_includes[arch] = [inc for inc in includes if inc.directory not in openssl_includes]
            if native_library and self.owner not in {None, llvm_targets.PREFIX}:
                # External C libraries do not use the captured C++ ABI include directory.
                abi = LocalInclude(llvm_targets.PREFIX + '/libcxxabi/include', self.owner)
                for arch, includes in system_includes.items():
                    if groups[arch]['language'] == 'C':
                        system_includes[arch] = [inc for inc in includes if inc != abi]
            if implementation_deps:
                attrs['implementation_deps'] = select({
                    '@platforms//cpu:' + arch: implementation_deps.get('@platforms//cpu:' + arch, [])
                    for arch in attribute_arches
                })
            if any(system_includes.values()):
                attrs['local_includes'] = select({
                    '@platforms//cpu:' + arch: system_includes.get(arch, [])
                    for arch in attribute_arches
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
                binary_attrs['features'] = (
                    ['-static_link_cpp_runtimes', '-fortify_source'] +
                    (['-default_compile_flags'] if name == 'clickhouse' else [])
                )
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
            rule_name = name if collapse_single else group_name
            rule_compatible = compatible
            if fastops:
                rule_name = '_fastops_sve' if sve else '_fastops'
                if sve:
                    attrs['visibility'] = ['@clickhouse//:__pkg__']
                    rule_compatible = Select({
                        '@platforms//cpu:aarch64': [],
                        '//conditions:default': ['@platforms//:incompatible'],
                    })
                else:
                    attrs['deps'] = Concat([attrs['deps'], select({
                        '@platforms//cpu:' + arch: [self.label(dep) for dep in variant['deps']] +
                        ([':_fastops_sve'] if arch == 'aarch64' else [])
                        for arch, variant in variants.items()
                    })])
            rule_attrs = {
                **attrs,
                # These copts already contain the CMake compile group's flags.
                # The LLVM toolchain otherwise forces frame pointers in Release.
                'features': ['-fortify_source', '-default_compile_flags'],
                'target_compatible_with': rule_compatible,
                'alwayslink': select({'@platforms//cpu:' + arch: v['alwayslink'] for arch, v in variants.items()}),
            }
            embedded = self.embedded_rule(name, groups, rule_attrs) if any(g.get('embedded_inputs') for g in groups.values()) else None
            self.rule('cc_library', rule_name, rule_attrs)
            if embedded is not None:
                self.rule('cc_library', name + '_embedded', embedded)
            if collapse_single:
                return
        if fastops:
            return
        deps = select({
            '@platforms//cpu:' + arch: [self.label(d) for d in v['deps']] + [
                ':' + group_names[i]
                for i in range(1 if kind == 'EXECUTABLE' else 0, group_count)
                if (not llvm_runtime or i < len(v['groups'])) and (name != 'clang_rt_builtins' or i < 2)
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
        paths = {function for function in ('repository_path', 'repository_local_include', 'llvm_toolchain_path', 'cmake_prefix_maps',
                                           'cmake_cxx_compiler_version')
                 if any(function + '(' in rule for rule in self.rules)}
        if any('CLICKHOUSE_WARNINGS' in rule for rule in self.rules):
            paths.add('CLICKHOUSE_WARNINGS')
        if paths:
            loads['@clickhouse//bazel/rules:repository_paths.bzl'] = paths
        return ''.join('load(' + json.dumps(path) + ', ' + ', '.join(map(json.dumps, sorted(names))) + ')\n'
                       for path, names in sorted(loads.items()))

    def text(self):
        files = package_sources(self.owner)
        text = simplify_globs('\n'.join(self.rules), files)
        if self.owner:
            text = simplify_source_globs(text, files)
        return compact(text)


def rust_license_filenames(root):
    """Read the filename precedence from process_rust_crate in list-licenses.sh."""
    script = (root / 'utils/list-licenses/list-licenses.sh').read_text()
    body = script.split('process_rust_crate() {', 1)[1].split('declare -a arr=(', 1)[1].split(')', 1)[0]
    names = []
    for line in body.splitlines():
        if line.strip():
            match = re.fullmatch(r'"([a-zA-Z0-9_./-]+)"', line.strip())
            if not match or match[1] in names:
                raise ValueError(f'Unexpected Rust license filename: {line}')
            names.append(match[1])
    if not names:
        raise ValueError('Empty Rust license filename list')
    return names


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
    build('contrib/rust_vendor').rule('rust_vendor_licenses', 'licenses', {
        'filenames': rust_license_filenames(ROOT),
    })
    for owner in ('contrib/pcodec', 'contrib/wasmtime', 'contrib/chdig'):
        build(owner).rule('filegroup', 'cmake_cargo_sources', {
            'srcs': Glob(['**'], ('**/BUILD', '**/BUILD.bazel', 'target/**')),
        })
    build('contrib/sql-console').rule('filegroup', 'dist_standalone', {
        'srcs': Glob(['dist-standalone/**']),
        'visibility': ['@clickhouse//bazel:__pkg__'],
    })
    build('contrib/chdig').rule('filegroup', 'cargo_manifest', {'srcs': ['Cargo.toml']})
    build('contrib/openssl').rule('filegroup', 'headers', {'srcs': Glob(['include/**'])})
    # Cargo build scripts link host tools and target C/C++ configure checks.
    build('contrib/sysroot').rule('filegroup', 'cargo_link_files', {
        'srcs': select({
            '@platforms//cpu:' + arch: Glob(['linux-' + arch + '/**/' + lib + '/**' for lib in ('lib', 'lib64')])
            for arch in ARCHES
        }),
    })
    # The ClickHouse PIE links these glibc libraries and GCC startup objects.
    glibc_libraries = ('ld-linux-*.so.*', 'libc.so*', 'libc_nonshared.a', 'libdl.so*',
                       'libm.so*', 'libpthread.so*', 'librt.so*')
    build('contrib/sysroot').rule('filegroup', 'linux_link_files', {
        'srcs': select({
            '@platforms//cpu:' + arch: Glob(['linux-' + arch + '/**/' + name for name in glibc_libraries])
            for arch in ARCHES
        }),
    })
    build('contrib/sysroot').rule('filegroup', 'gcc_pie_startup_objects', {
        'srcs': select({
            '@platforms//cpu:' + arch: Glob(['linux-' + arch + '/lib/gcc/**/crt*S.o'])
            for arch in ARCHES
        }),
    })
    build('contrib/sysroot').rule('filegroup', 'linux_math_library', {
        'srcs': select({
            '@platforms//cpu:' + arch: ['linux-' + arch + '/' + arch + '-linux-gnu/libc/lib64/libm.so.6']
            for arch in ARCHES
        }),
    })
    krb5 = build('contrib/krb5')
    # In the initial CMake graph the six generated private error tables
    # still belong to //bazel. configured_codegen_outputs unifies their
    # two architecture-specific output labels after the overlays are emitted.
    error_tables = ('krb5_err', 'k5e1_err', 'kdb5_err', 'kv5m_err', 'krb524_err', 'asn1_err')
    parts = {
        '@platforms//cpu:' + arch: ['src/include/krb5/krb5.hin'] + [
            '@clickhouse//bazel:gen/' + arch + '/contrib/krb5-cmake/include_private/lib/krb5/error_tables/' + table + '.h'
            for table in error_tables
        ] for arch in ARCHES
    }
    krb5.rule('genrule', 'generate_contrib_krb5_cmake_include_krb5_krb5_h', {
        'srcs': select(parts),
        'outs': ['config/contrib/krb5-cmake/include/krb5/krb5.h'],
        'cmd': select({cpu: 'cat ' + ' '.join('"$(location ' + part + ')"' for part in inputs) + ' > "$@"'
                       for cpu, inputs in parts.items()}),
    })
    krb5.rule('perl_library', 'templates', {
        'srcs': Glob(['src/util/*.pm']), 'includes': ['src/util'],
    })
    for name, source in [('gen_map', 'gen-map.pl'), ('gen', 'gen.pl')]:
        krb5.rule('perl_binary', name, {'srcs': ['src/util/' + source], 'deps': [':templates']})
