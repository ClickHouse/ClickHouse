#!/usr/bin/env python3
"""Generate native Bazel target data from both configured CMake File API graphs."""

import ast
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.codegen import Codegen, common_googleapis_rules, googleapis_path, googleapis_source_map, render_rules
from initial_migration.cmake_metadata import read_metadata, write_metadata
from initial_migration.rust_archives import ARCHIVE_TARGETS as RUST_ARCHIVES
from initial_migration.reference_readers import read_file_api
from initial_migration.configuration import read_configurations
from initial_migration import llvm_targets
from initial_migration.external_headers import HeaderLibraries
from initial_migration.format_build_files import format_build_files
from initial_migration.prune_inherited_headers import prune, prune_published
from initial_migration.prune_unused_exports import prune_native_exports, prune_unused_exports
from initial_migration.build_file import (
    BuildFile, CMakeCxxCompilerVersion, Glob,
    SOURCE_HEADER_DIRECTORIES, SOURCE_HEADER_EXCLUDES, SOURCE_HEADER_SUFFIXES, SOURCE_HEADER_TARGETS,
    dependency_inputs, relocate_compilation,
    relocate_tools, select, starlark, target_label, target_owner,
)
from initial_migration.repository_inputs import (
    REPOSITORIES, cmake_has_source_prefix_map, comparison_root, configuration_owner, execution_path, repository,
    CC_HEADER_SUFFIXES, HeaderDirectory, directory_sources, input_key,
    package_sources, source_label, source_path, write_overlays,
)

ROOT = Path(__file__).resolve().parents[2]
ARCHES = ('x86_64', 'aarch64')
COMPILE_TYPES = {'STATIC_LIBRARY', 'OBJECT_LIBRARY', 'EXECUTABLE'}
HEADER_SUFFIXES = {'.h', '.hh', '.hpp', '.hxx', '.inc', '.ipp', '.inl', '.tcc', '.def'}
BCR_DEPENDENCIES = json.loads((ROOT / 'bazel/initial_migration/bcr_dependencies.json').read_text())
LICENSES_OUTPUT = 'src/Storages/System/StorageSystemLicenses.generated.cpp'


def llvm_source_overlays():
    """Read ClickHouse source files supplied to the LLVM module extension."""
    tree = ast.parse((ROOT / 'bazel/llvm.MODULE.bazel').read_text())
    for stmt in tree.body:
        if (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                and isinstance(stmt.value.func, ast.Attribute)
                and stmt.value.func.attr == 'from_archive'
                and isinstance(stmt.value.func.value, ast.Name)
                and stmt.value.func.value.id == 'llvm'):
            files = next(kw.value for kw in stmt.value.keywords if kw.arg == 'files')
            return {dest: source_path(label) for dest, label in ast.literal_eval(files).items()}
    raise ValueError('Missing llvm.from_archive files in bazel/llvm.MODULE.bazel')
BUILD_OPTIONS_OUTPUT = 'src/Storages/System/StorageSystemBuildOptions.generated.cpp'
# CMake installs these headers unchanged beside the configured conf.h.
# Keep the upstream source tree authoritative instead of checking in copies.
COPIED_INCLUDE_DIRS = {
    'contrib/wasmtime/include': 'contrib/wasmtime/crates/c-api/include',
    'contrib/libcotp/include': 'contrib/libcotp/src',
}
CMAKE_ONLY_SOURCES = {'contrib/icu-cmake/empty.cpp'}
def cmake_build_options(text):
    """Preserve CMake feature/version data and extract compiler metadata."""
    substitutions = {
        '@BAZEL_COMPILER_VERSION_DEFINES@': '',
        '@BAZEL_BUILD_SYSTEM_ENTRY@': '',
    }
    for key in ['C_COMPILER', 'CXX_COMPILER', 'C_FLAGS', 'CXX_FLAGS', 'LINK_FLAGS',
                'CMAKE_VERSION', 'C_COMPILER_VERSION', 'CXX_COMPILER_VERSION']:
        pattern = r'(^    "' + key + r'", )(.*),$'
        matches = list(re.finditer(pattern, text, re.MULTILINE))
        if len(matches) != 1:
            raise ValueError(f'Expected one CMake build metadata value for {key}, found {len(matches)}')
        marker = '@BAZEL_' + key + '@'
        substitutions[marker] = matches[0][2]
        text = re.sub(pattern, lambda match: match[1] + marker + ',', text, flags=re.MULTILINE)
    text = text.replace('    "CMAKE_VERSION",', '@BAZEL_BUILD_SYSTEM_ENTRY@    "CMAKE_VERSION",')
    return '@BAZEL_COMPILER_VERSION_DEFINES@' + text, substitutions


class Graph:
    def __init__(self, arch):
        self.arch = arch
        self.build = ROOT / ('build_bazel_reference_' + arch)
        _, self.targets = read_file_api(self.build / '.cmake/api/v1/reply')
        self.config = ROOT / 'bazel/config' / arch
        self.rust_headers = set()
        self.header_cache = {}
        self.include_cache = {}
        self.missing = set()
        self.configured = set()
        self.source_headers = {}
        self.compiled = {n for n, t in self.targets.items() if t['type'] in COMPILE_TYPES}
        self.link_object_owners = {}
        for name, target in self.targets.items():
            owners = set()
            # INTERFACE libraries can forward TARGET_OBJECTS only through the
            # link command, without a File API dependency edge or source entry.
            for fragment in target.get('link', {}).get('commandFragments', []):
                for operand in shlex.split(fragment['fragment']):
                    if not operand.endswith('.o'):
                        continue
                    match = re.search(r'CMakeFiles/([^/]+)\.dir/', operand)
                    if not match or match[1] not in self.compiled:
                        raise ValueError('Unknown direct link object: ' + operand)
                    owners.add(match[1])
            self.link_object_owners[name] = owners
        self.direct_object_targets = set().union(*self.link_object_owners.values())
        self.id_names = {t['id']: n for n, t in self.targets.items()}
        self.codegen = Codegen(self)
        self.custom_outputs = {out for out, edge in self.codegen.nodes.items()
                               if edge['rule'] == 'CUSTOM_COMMAND'}
        self.build_options = {}
        self.parity_sources = {}
        self.embed_copies = set()

    def quoted_references(self, path, directive):
        if path not in self.include_cache:
            references = {'include': [], 'embed': []}
            for kind, value in re.findall(r'^\s*#\s*(include|embed)\s+"([^"]+)"', path.read_text(errors='replace'), re.MULTILINE):
                references[kind].append(value)
            self.include_cache[path] = references
        return self.include_cache[path][directive]

    def build_options_template(self, text):
        # Feature/version values come from the reference configuration. By
        # default, compiler metadata describes Bazel's selected toolchain;
        # comparison builds can replay the original CMake values exactly.
        for key in ['C_FLAGS', 'CXX_FLAGS']:
            value = re.search(r'^    "' + key + r'", (".*"),$', text, re.MULTILINE)
            self.build_options[key.lower()] = self.flags([{'fragment': json.loads(value[1])}])
        template, self.build_options['cmake_substitutions'] = cmake_build_options(text)
        return template

    def snapshot_configuration(self):
        self.configuration_rules = read_configurations(ROOT, self.build)
        for directory, dirs, names in os.walk(self.build):
            dirs[:] = [d for d in dirs if d not in {'CMakeFiles', '.cmake', 'native', 'cargo', 'external'}]
            for name in names:
                path = Path(directory) / name
                if path.suffix not in HEADER_SUFFIXES | {'.c', '.cc', '.cpp', '.S'} and name not in {'__config_site', '__assertion_handler'}:
                    continue
                rel = path.relative_to(self.build)
                dest = self.config / rel
                # Outputs remain owned by their generators after a CMake build.
                if rel.as_posix() in self.custom_outputs:
                    dest.unlink(missing_ok=True)
                    continue
                if (rel.is_relative_to(llvm_targets.PREFIX)
                        and not rel.is_relative_to(llvm_targets.PREFIX + '/llvm')):
                    continue
                for installed, source in COPIED_INCLUDE_DIRS.items():
                    if rel.is_relative_to(installed):
                        original = ROOT / source / rel.relative_to(installed)
                        if original.is_file():
                            if path.read_bytes() != original.read_bytes():
                                raise ValueError('Installed header differs from its source: ' + str(rel))
                            self.source_headers[rel.as_posix()] = original.relative_to(ROOT).as_posix()
                if rel.as_posix() in self.source_headers:
                    dest.unlink(missing_ok=True)
                    continue
                if rel.as_posix() == LICENSES_OUTPUT:
                    dest.unlink(missing_ok=True)
                    continue
                if rel.parts[0] == 'contrib':
                    if rel.as_posix() in CMAKE_ONLY_SOURCES:
                        continue
                    if rel.as_posix() not in self.configuration_rules and rel.as_posix() != 'contrib/cctz-cmake/TimeZones.generated.cpp':
                        raise ValueError('Untranslated configured input: ' + str(rel))
                    self.configured.add(rel.as_posix())
                    continue
                dest.parent.mkdir(parents=True, exist_ok=True)
                # Keep configuration relocatable; runtime build metadata is
                # regenerated alongside target data, never copied from a binary.
                text = path.read_text().replace(str(self.build), 'bazel/gen/' + self.arch).replace(str(ROOT) + '/', '')
                if rel.as_posix() == BUILD_OPTIONS_OUTPUT:
                    dest.unlink(missing_ok=True)
                    dest = dest.with_suffix('.cpp.in')
                    # Compatibility mode replays the exact reference metadata,
                    # including its path strings; do not relocate these values.
                    dest.write_text(self.build_options_template(path.read_text()))
                    continue
                dest.write_text(text)
                self.configured.add(rel.as_posix())
        # Empty CMake include directories do not need Bazel inputs.
        for marker in self.config.rglob('bazel_directory.h'):
            marker.unlink()

    def path(self, value):
        value = os.path.normpath(value)
        if value.startswith(str(self.build) + '/'):
            value = value.removeprefix(str(self.build) + '/')
            if value in self.source_headers:
                return self.source_headers[value]
            if value in self.configured:
                return 'bazel/config/' + self.arch + '/' + value
            self.missing.add(value)
            return 'bazel/gen/' + self.arch + '/' + value
        if value.startswith(str(ROOT) + '/'):
            return value.removeprefix(str(ROOT) + '/')
        if value.startswith('build_bazel_reference_' + self.arch + '/'):
            return self.path(str(ROOT / value))
        if os.path.isabs(value):
            raise ValueError('Undeclared host path: ' + value)
        return value

    def headers(self, directory, visited=None):
        directory = os.path.normpath(directory)
        for base in (ROOT, self.build):
            prefix = str(base / llvm_targets.PREFIX) + '/'
            if (directory.startswith(prefix)
                    and not directory.startswith((prefix + 'llvm/', prefix + 'third-party/siphash/'))):
                return []  # The registered toolchain owns runtime headers.
        if directory.startswith(str(self.build)):
            rel = os.path.relpath(directory, self.build)
            prefix = '' if rel == '.' else rel + '/'
            paths = [p for p in sorted(self.configured) if p.startswith(prefix)]
            if rel == 'contrib/librdkafka-cmake/auxdir':
                paths.append('contrib/librdkafka-cmake/config.h')
            return [source_label('bazel/config/' + self.arch + '/' + p) for p in paths] + [
                source_label(source) for p, source in sorted(self.source_headers.items()) if p.startswith(prefix)
            ]
        relative = self.path(directory)
        if relative.startswith('rust/workspace/'):
            return [source_label(p.relative_to(ROOT).as_posix())
                    for p in sorted((ROOT / relative).rglob('*'))
                    if p.is_file() and p.suffix in HEADER_SUFFIXES]
        if relative in {'.', ''}:
            raise ValueError('Repository-wide header glob requested')
        if relative.startswith('bazel/gen/'):
            return []
        if visited is None:
            visited = set()
        if relative in visited:
            return []
        visited.add(relative)
        result = [HeaderDirectory(relative)]
        # Headers can themselves include siblings outside the -I directory:
        # OpenLDAP's ldap-int.h includes ../liblber/lber-int.h, for example.
        # Follow only existing quoted includes that escape this declared root.
        if relative not in self.header_cache:
            root = ROOT / relative
            dependencies = set()
            for header in root.rglob('*'):
                if not header.is_file() or (header.suffix and header.suffix not in HEADER_SUFFIXES):
                    continue
                for include in self.quoted_references(header, 'include') + self.quoted_references(header, 'embed'):
                    path = Path(os.path.normpath(header.parent / include))
                    if not path.is_relative_to(root) and path.is_file():
                        dependencies.add(str(path.parent))
            self.header_cache[relative] = sorted(dependencies)
        for dependency in self.header_cache[relative]:
            result.extend(self.headers(dependency, visited))
        return sorted(set(result), key=input_key)

    def flags(self, fragments):
        result = []
        for fragment in fragments:
            for flag in shlex.split(fragment['fragment']):
                # The registered hermetic LLVM toolchain owns these paths.
                if flag.startswith(('--gcc-toolchain=', '--ld-path=', '-ffile-prefix-map=')):
                    continue
                if str(ROOT) in flag:
                    if flag.startswith(str(self.build) + '/'):
                        relative = flag.removeprefix(str(self.build) + '/')
                        if any(p.startswith(relative + '/') for p in self.configured):
                            flag = 'bazel/config/' + self.arch + '/' + relative
                        else:
                            flag = '$(GENDIR)/bazel/gen/' + self.arch + '/' + relative
                    flag = flag.replace(str(ROOT) + '/', '')
                result.append(flag)
                if flag == '-Weverything':
                    # cc_library emits both source and generated include dirs.
                    # RBE creates a generated dir only if it has action inputs.
                    result.append('-Wno-missing-include-dirs')
        return result

    def rust_cxx_configuration(self):
        cache = (self.build / 'CMakeCache.txt').read_text()
        result, headers = {}, set()
        for field, key in (('ccopts', 'RUST_CFLAGS'), ('copts', 'RUST_CXXFLAGS')):
            value = re.search(r'^' + key + r':INTERNAL=(.*)$', cache, re.MULTILINE)
            if not value:
                raise ValueError('Missing reference ' + key)
            flags = iter(self.flags([{'fragment': value[1]}]))
            copts = []
            for flag in flags:
                if flag == '--sysroot':
                    next(flags)  # The registered hermetic toolchain owns libc.
                    continue
                copts.append(flag)
                if flag in {'-I', '-isystem'}:
                    directory = next(flags)
                    if directory.startswith(llvm_targets.PREFIX + '/') or ('/' + llvm_targets.PREFIX + '/') in directory:
                        copts.pop()
                        continue
                    copts.append(directory)
                    headers.update(self.headers(str(ROOT / directory)))
            result[field] = copts
        self.rust_headers = headers
        labels = set()
        for header in headers:
            if isinstance(header, HeaderDirectory):
                labels.update(source_label(file) for file in directory_sources(header.directory))
            else:
                labels.add(header)
        return result | {'hdrs': sorted(labels)}

    def embedded_source(self, source, additional_inputs, flags):
        """Keep quoted #embed lookups inside Bazel's generated output tree.

        Clang resolves these paths relative to the source's real path, which
        escapes a symlink sandbox for checkout files. Copy the source and its
        source-relative assets together, preserving their bytes and layout.
        Root-relative assets also need this layout when fetched externally.
        """
        if source.endswith('/contrib/cctz-cmake/TimeZones.generated.cpp'):
            assets = self.timezone_sources()
            prefix = 'bazel/gen/' + self.arch + '/embedded/'
            self.embed_copies.update(assets)
            additional_inputs.append('//bazel:cctz_timezone_data')
            flags.append('--embed-dir=$(GENDIR)/' + prefix.rstrip('/'))
            return source
        path = ROOT / source
        if not path.is_file():
            return source
        source_assets, root_assets = [], []
        for value in self.quoted_references(path, 'embed'):
            asset = Path(os.path.normpath(path.parent / value))
            if asset.is_file():
                source_assets.append(asset.relative_to(ROOT).as_posix())
            elif (ROOT / value).is_file():
                root_assets.append(value)
        assets = source_assets + root_assets
        if not assets:
            return source
        prefix = 'bazel/gen/' + self.arch + '/embedded/'
        self.embed_copies.update(assets)
        additional_inputs.extend(source_label(prefix + asset) for asset in assets)
        if root_assets:
            flags.append('--embed-dir=$(GENDIR)/' + prefix.rstrip('/'))
        if source_assets:
            self.embed_copies.add(source)
            return prefix + source
        return source

    def timezone_sources(self):
        return sorted(p.relative_to(ROOT).as_posix()
                      for p in (ROOT / 'contrib/cctz/testdata/zoneinfo').rglob('*')
                      if p.is_file() and not p.is_symlink() and p.suffix != '.tab' and p.name != 'localtime')

    def declare_configuration(self, builds, owners):
        for relative in sorted(self.configured):
            if not relative.startswith('contrib/'):
                continue
            owner = configuration_owner(relative)
            if owner not in builds:
                builds[owner] = BuildFile(owner, owners)
            build = builds[owner]
            output = 'config/' + self.arch + '/' + relative
            name = 'configure_' + self.arch + '_' + hashlib.sha256(relative.encode()).hexdigest()[:16]
            if relative == 'contrib/cctz-cmake/TimeZones.generated.cpp':
                sources = [build.label(source_label(path)) for path in self.timezone_sources()]
                build.rule('genrule', name, {
                    'srcs': sources,
                    'outs': [output],
                    'tools': ['@clickhouse//bazel/rules:generate_timezones'],
                    'cmd': '$(execpath @clickhouse//bazel/rules:generate_timezones) --output=$@ $(SRCS)',
                })
                continue
            spec = self.configuration_rules[relative]
            if spec['kind'] == 'write':
                build.rule('write_file', name, {'out': output, 'content': [spec['content']]})
                continue
            source = build.label(source_label(spec['source'].resolve().relative_to(ROOT).as_posix()))
            if spec['kind'] == 'copy':
                build.rule('copy_file', name, {'src': source, 'out': output})
            else:
                substitutions = {
                    key: value.replace(str(self.build), 'bazel/gen/' + self.arch).replace(str(ROOT) + '/', '')
                    for key, value in spec['substitutions'].items()
                }
                for key in ('@CMAKE_CXX_COMPILER_VERSION@', '@MONGOC_CXX_COMPILER_VERSION@'):
                    if key in substitutions:
                        substitutions[key] = CMakeCxxCompilerVersion(self.arch)
                mongo_cxx_version = '#cmakedefine MONGOCXX_COMPILER_VERSION "${MONGOCXX_COMPILER_VERSION}"'
                if mongo_cxx_version in substitutions:
                    substitutions[mongo_cxx_version] = CMakeCxxCompilerVersion(
                        self.arch, '#define MONGOCXX_COMPILER_VERSION "', '"')
                build.rule('expand_template', name, {'template': source, 'out': output, 'substitutions': substitutions})
        # These were generated snapshots, never authoritative source inputs.
        if (self.config / 'contrib').exists():
            shutil.rmtree(self.config / 'contrib')

    def target(self, name, target, native=False):
        if llvm_targets.native_target(name, target):
            return {'alias': '@' + llvm_targets.RAW_REPOSITORY + '//:' + name,
                    'kind': target['type'], 'groups': [], 'deps': [], 'linkopts': [], 'alwayslink': False}
        module_target = llvm_targets.target(name)
        if module_target:
            result = {'groups': [], 'deps': [], 'linkopts': [], 'kind': target['type'], 'alwayslink': False}
            if target['type'] == 'EXECUTABLE':
                return result | {'alias': module_target}
            dependencies = [self.id_names[d['id']] for d in target.get('dependencies', [])]
            result['deps'] = [':' + d for d in sorted(dependencies)
                              if d in self.compiled and self.targets[d]['type'] != 'EXECUTABLE']
            result['deps'].insert(0, module_target)
            if name == '_fuzzer':
                result['deps'].append('@llvm-project//compiler-rt:fuzzer')
            return result
        if name in BCR_DEPENDENCIES and not native:
            return {'groups': [], 'deps': BCR_DEPENDENCIES[name]['deps'],
                    'linkopts': [], 'kind': target['type'], 'alwayslink': False}
        groups = []
        for index, cg in enumerate(target.get('compileGroups', [])):
            sources = []
            for i in cg['sourceIndexes']:
                path = target['sources'][i]['path']
                absolute = Path(os.path.normpath(ROOT / path))
                if absolute.is_relative_to(self.build) and absolute.relative_to(self.build).as_posix() in CMAKE_ONLY_SOURCES:
                    if absolute.read_text().strip():
                        raise ValueError('CMake-only translation unit is no longer empty: ' + path)
                    continue
                sources.append(self.path(path))
            if not sources:
                continue
            flags = self.flags(cg.get('compileCommandFragments', []))
            generated_inputs = self.codegen.target_generated_inputs(name)
            hdrs = {p for p in generated_inputs if Path(p).suffix in HEADER_SUFFIXES}
            for i, flag in enumerate(flags[:-1]):
                # Bare names such as XGBoost's -include exception resolve
                # through the declared include roots or compiler toolchain.
                # Only direct workspace files need an additional input here.
                if flag in {'-include', '-imacros'} and (ROOT / flags[i + 1]).is_file():
                    hdrs.add(source_label(flags[i + 1]))
            # Arrow includes generated Protobuf implementation files as text.
            textual_hdrs = [p for p in generated_inputs if p.endswith('.cc')]
            additional_inputs = [p for p in generated_inputs if p.endswith('.gz')]
            for flag in flags:
                if flag.startswith('--embed-dir='):
                    additional_inputs.extend(self.headers(str(ROOT / flag.split('=', 1)[1])))
            generated_paths = {p.removeprefix('//bazel:gen/' + self.arch + '/')
                               for p in generated_inputs if p.startswith('//bazel:gen/')}
            generated_paths.update(s.removeprefix('bazel/gen/' + self.arch + '/')
                                   for s in sources if s.startswith('bazel/gen/'))
            external_deps = set()
            native_cxx_includes = []
            if name == '_vectorscan' and self.arch == 'aarch64':
                # Preserve the reference sysroot's ARM feature constants for
                # Vectorscan's runtime dispatch without replacing libc headers.
                external_deps.add(':linux_aarch64_hwcap')
            for src in sources:
                if not src.startswith('bazel/'):
                    hdrs.update(self.headers(str(ROOT / src).rsplit('/', 1)[0]))
                    # Quoted relative includes can reach outside both the source
                    # directory and CMake's -I paths (e.g. zxc's ../../include).
                    source = ROOT / src
                    for include in self.quoted_references(source, 'include'):
                        header = Path(os.path.normpath(source.parent / include))
                        if header.is_file():
                            hdrs.update(self.headers(str(header.parent)))
                            if native and header.suffix in {'.c', '.cc', '.cpp'}:
                                additional_inputs.append(source_label(header.relative_to(ROOT).as_posix()))
            for inc in cg.get('includes', []):
                flag = '-isystem' if inc.get('isSystem') else '-I'
                path = os.path.normpath(inc['path'])
                relative = path.removeprefix(str(self.build) + '/').removeprefix(str(ROOT) + '/')
                if relative in llvm_targets.TOOLCHAIN_INCLUDE_DIRECTORIES:
                    if cg['language'] in {'C', 'ASM', 'ASM_NASM'}:
                        native_cxx_includes.append([
                            flag, 'generated' if path.startswith(str(self.build) + '/') else 'source',
                            relative.removeprefix(llvm_targets.PREFIX + '/'),
                        ])
                    continue
                module_dependencies = llvm_targets.include_dependencies(relative)
                if module_dependencies is not None:
                    external_deps.update(module_dependencies)
                    continue
                if path == str(self.build / 'contrib/librdkafka-cmake/auxdir'):
                    path = str(self.build / 'contrib/librdkafka-cmake')
                if path == str(self.build / 'contrib/delta-kernel-rs/include'):
                    external_deps.add('//bazel/rust:delta_kernel_ffi_cc')
                    continue
                replacement = next((d for dep_name, d in BCR_DEPENDENCIES.items()
                                    if not (native and dep_name == name)
                                    and path.removeprefix(str(ROOT) + '/') in d['include_directories']), None)
                if replacement:
                    external_deps.update(replacement['deps'])
                    continue
                # Program libraries resolve generated configuration headers
                # from their declared deps, not from CMake's build root.
                if path == str(self.build) and all(s.startswith('programs/') for s in sources):
                    continue
                hdrs.update(self.headers(path))
                if path.startswith(str(self.build)):
                    rel = os.path.relpath(path, self.build)
                    prefix = '' if rel == '.' else rel + '/'
                    directories = []
                    if any(p.startswith(prefix) for p in self.configured):
                        directories.append('bazel/config/' + self.arch + '/' + rel)
                    if rel in COPIED_INCLUDE_DIRS:
                        directories.append(COPIED_INCLUDE_DIRS[rel])
                    if any(p.startswith(prefix) for p in generated_paths):
                        directories.append('$(GENDIR)/bazel/gen/' + self.arch + '/' + rel)
                    # Only declared inputs populate Bazel's generated tree;
                    # empty CMake include roots have no files to find here.
                    for directory in directories:
                        flags += [flag, directory]
                else:
                    flags += [flag, self.path(path)]
            sources = [self.embedded_source(s, additional_inputs, flags) for s in sources]
            groups.append({
                'srcs': [source_label(s) for s in sources],
                'copts': flags,
                'cmake_source_prefix_map': cmake_has_source_prefix_map(cg.get('compileCommandFragments', []), ROOT),
                # cc_library tokenizes defines with Bourne-shell rules. Preserve
                # literal quotes (e.g. Cyrus SASL's CONFIGDIR="") through that step.
                'local_defines': [shlex.quote(d['define'].replace(str(ROOT) + '/', '')) for d in cg.get('defines', [])],
                'hdrs': sorted(hdrs, key=input_key),
                'textual_hdrs': textual_hdrs,
                'additional_compiler_inputs': sorted(set(additional_inputs), key=input_key),
                'native_cxx_includes': native_cxx_includes,
                'deps': sorted(external_deps),
                'language': cg['language'],
            })
        dependencies = {self.id_names[d['id']] for d in target.get('dependencies', [])}
        object_refs = set()
        generated_objects = []
        for src in target.get('sources', []):
            path = self.path(src['path'])
            output = path.removeprefix('bazel/gen/' + self.arch + '/')
            if output.endswith('.o') and output in self.codegen.handled:
                generated_objects.append(source_label(path))
            match = re.search(r'CMakeFiles/([^/]+)\.dir/', src['path'])
            if match and match[1] in self.compiled:
                object_refs.add(match[1])
        object_refs.update(self.link_object_owners[name])
        if generated_objects:
            groups.append({'srcs': generated_objects, 'language': 'OBJECT'})
        deps = [':' + d for d in sorted((dependencies | object_refs) & self.compiled)
                if self.targets[d]['type'] != 'EXECUTABLE']
        linkopts = []
        if target['type'] == 'EXECUTABLE':
            # Link-only flags are retained separately from the archive closure.
            for flag in self.flags([f for f in target.get('link', {}).get('commandFragments', []) if f['role'] == 'flags']):
                if flag.endswith('.a') or flag in {'-Wl,--whole-archive', '-Wl,--no-whole-archive', '-llibllvmlibc'}:
                    continue
                linkopts.append(flag)
            for frag in target.get('link', {}).get('commandFragments', []):
                if frag['role'] == 'libraries' and frag['fragment'].startswith('-l'):
                    linkopts.append(frag['fragment'])
                if frag['role'] == 'libraries':
                    for library in shlex.split(frag['fragment']):
                        library = library.removeprefix(str(self.build) + '/')
                        if library in RUST_ARCHIVES:
                            deps.append('//bazel/rust:' + RUST_ARCHIVES[library] + '_cc')
            # CMake loads these whole archives before all ordinary archives.
            # In particular, llvm-libc's math definitions must be seen before
            # the lazy glibc compatibility archive, which also supplies them.
            runtimes = [':' + n for n in ['clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc']
                        if n in self.compiled]
            deps = runtimes + [d for d in deps if d not in runtimes]
        # TARGET_OBJECTS can also forward a STATIC_LIBRARY's objects directly.
        alwayslink = (target['type'] in {'OBJECT_LIBRARY', 'EXECUTABLE'}
                      or name in self.direct_object_targets
                      or name in {'clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc'})
        return {'groups': groups, 'deps': deps, 'linkopts': linkopts,
                'kind': target['type'], 'alwayslink': alwayslink}

    def generate(self):
        self.snapshot_configuration()
        self.codegen.generate()
        self.codegen.licenses(LICENSES_OUTPUT)
        data = {name: self.target(name, t) for name, t in sorted(self.targets.items()) if t['type'] in COMPILE_TYPES}
        self.codegen.embedded_sources()
        for target in data.values():
            for group in target['groups']:
                for source in group['srcs']:
                    configured = re.match(r'@([^/]+)//:config/' + self.arch + r'/(contrib/.*)', source)
                    if configured:
                        relative = configured[2]
                        owner = configuration_owner(relative)
                        identity = '@build/' + self.arch + '/' + relative
                        self.parity_sources['build_bazel_reference_' + self.arch + '/' + relative] = identity
                        self.parity_sources['bazel-out/k8-opt/bin/' + comparison_root(owner) + '/config/' + self.arch + '/' + relative] = identity
                        continue
                    original = source_path(source)
                    repo = repository(original)
                    if repo:
                        self.parity_sources[comparison_root(repo) + original.removeprefix(repo)] = original
                    for kind in ['config', 'gen']:
                        prefix = '//bazel:' + kind + '/' + self.arch + '/'
                        if not source.startswith(prefix):
                            continue
                        relative = source.removeprefix(prefix)
                        if kind == 'gen' and relative.startswith('embedded/'):
                            self.parity_sources['bazel-out/k8-opt/bin/bazel/gen/' + self.arch + '/' + relative] = relative.removeprefix('embedded/')
                            continue
                        identity = '@build/' + self.arch + '/' + relative
                        self.parity_sources['build_bazel_reference_' + self.arch + '/' + relative] = identity
                        bazel_path = source.replace('//bazel:', 'bazel/', 1)
                        if kind == 'gen':
                            bazel_path = 'bazel-out/k8-opt/bin/' + bazel_path
                        self.parity_sources[bazel_path] = identity
        # Module-owned libraries no longer pass through translated compile
        # groups, but their actual source files remain in the parity audit.
        module_root = llvm_targets.TOOLCHAIN_ROOT
        for name in self.compiled:
            if not llvm_targets.target(name):
                continue
            target = self.targets[name]
            for group in target.get('compileGroups', []):
                for index in group['sourceIndexes']:
                    source = target['sources'][index]['path']
                    if source.startswith(llvm_targets.PREFIX + '/'):
                        self.parity_sources[module_root + source.removeprefix(llvm_targets.PREFIX)] = source
        for destination, source in llvm_source_overlays().items():
            self.parity_sources[module_root + '/' + destination] = source
        self.build_options.update({
            'template': 'config/' + self.arch + '/' + BUILD_OPTIONS_OUTPUT + '.in',
            'out': 'gen/' + self.arch + '/' + BUILD_OPTIONS_OUTPUT,
            'link_flags': data['clickhouse']['linkopts'],
        })
        self.codegen.handled.add(BUILD_OPTIONS_OUTPUT)
        self.generated_targets = data
        translated_outputs = self.codegen.handled | {
            output.removeprefix('gen/' + self.arch + '/')
            for rule in self.codegen.rules.values() for output in rule['outs']
        }
        substitutions = {
            output: '//bazel/rust:' + name + '_processed'
            for output, name in RUST_ARCHIVES.items() if output in self.custom_outputs
        }
        # compile_et is a shell wrapper around et_h.awk/et_c.awk. Their native
        # generator actions invoke those exact scripts directly.
        substitutions['contrib/krb5-cmake/compile_et'] = 'direct et_h.awk/et_c.awk actions'
        substitutions.update({output: llvm_targets.generated_target(output) for output in self.custom_outputs
                              if llvm_targets.generated_target(output)})
        remaining = self.custom_outputs - translated_outputs - substitutions.keys()
        classified, untranslated = self.classify_custom_outputs(remaining)
        return {
            'targets': len(self.targets),
            'native_targets': len(data),
            'compile_actions': sum(len(g['srcs']) for t in data.values() for g in t['groups'] if g['language'] != 'OBJECT'),
            'generated_object_inputs': sum(len(g['srcs']) for t in data.values() for g in t['groups'] if g['language'] == 'OBJECT'),
            'bcr_targets': sorted(BCR_DEPENDENCIES),
            'codegen_actions': len(self.codegen.rules) + 1,  # Toolchain build metadata.
            'native_substitutions': dict(sorted(substitutions.items())),
            'cmake_only_outputs': classified,
            'untranslated_outputs': untranslated,
            'configure_outputs': sorted(self.configured),
            'source_headers': dict(sorted(self.source_headers.items())),
            'build_outputs': sorted(self.missing),
            'utility_targets': sorted(n for n, t in self.targets.items() if t['type'] == 'UTILITY'),
        }

    def classify_custom_outputs(self, outputs):
        """Keep replaced bookkeeping and maintenance commands visible by purpose."""
        categories = {
            'cmake_cache': 'CMake cache editing and regeneration; not build artifacts.',
            'cmake_install': 'CMake installation commands; Bazel packaging is not implemented.',
            'ctest': 'CTest entry points; unit_tests_dbms is a native cc_test.',
            'cargo_build': 'Replaced by the seven native rules_rs archive targets.',
            'rust_symbols': 'Replaced by the seven *_processed archive targets.',
            'cargo_clean': 'Cargo maintenance commands; Bazel manages its own outputs.',
            'googleapis_external_project': 'ExternalProject stamps; native actions extract the pinned schema archive.',
            'tablegen_external_project': 'ExternalProject stamps; native execution-platform TableGen targets replace the nested build.',
            'contributors': 'Manual release maintenance; the build consumes the checked-in contributors source.',
        }
        classified = {key: {'reason': reason, 'outputs': []} for key, reason in categories.items()}
        rust_names = {Path(path).stem.removeprefix('lib') for path in RUST_ARCHIVES}
        external_stages = {'build', 'configure', 'done', 'download', 'install', 'mkdir', 'patch', 'update'}
        unknown = []
        for output in sorted(outputs):
            command = self.codegen.nodes[output].get('command', '')
            name = output.split('CMakeFiles/', 1)[-1]
            category = None
            if name == 'edit_cache.util' and command.endswith('/usr/bin/cmake -E echo No\\ interactive\\ CMake\\ dialog\\ available.'):
                category = 'cmake_cache'
            elif name == 'rebuild_cache.util' and '/usr/bin/cmake --regenerate-during-build ' in command:
                category = 'cmake_cache'
            elif name in {'install.util', 'install/local.util', 'install/strip.util'} and command.endswith(' -P cmake_install.cmake'):
                category = 'cmake_install'
            elif name == 'test.util' and command.endswith('/usr/bin/ctest --force-new-ctest-process'):
                category = 'ctest'
            elif name.removesuffix('.util').removeprefix('_cargo-build_') in rust_names and name.startswith('_cargo-build_'):
                category = 'cargo_build'
            elif name.removeprefix('localize_rust_c_') in rust_names and name.startswith('localize_rust_c_'):
                category = 'rust_symbols'
            elif name in {'strip_pco_symbols', 'strip_polyglot_symbols', 'strip_prql_symbols'}:
                category = 'rust_symbols'
            elif name.removeprefix('cargo-clean_') in rust_names and name.startswith('cargo-clean_'):
                category = 'cargo_clean'
            elif output == 'contrib/google-cloud-cpp-cmake/CMakeFiles/googleapis_download-complete' or output.removeprefix('external/googleapis/src/googleapis_download-stamp/googleapis_download-') in external_stages:
                category = 'googleapis_external_project'
            elif output == 'contrib/llvm-project-cmake/CMakeFiles/native-tblgen-build-complete' or output.removeprefix('contrib/llvm-project-cmake/native-tblgen-build-prefix/src/native-tblgen-build-stamp/native-tblgen-build-') in external_stages:
                category = 'tablegen_external_project'
            elif output == 'src/Storages/System/CMakeFiles/generate-contributors' and command.endswith(' && ./StorageSystemContributors.sh'):
                category = 'contributors'
            if category:
                classified[category]['outputs'].append(output)
            else:
                unknown.append(output)
        return {key: value for key, value in classified.items() if value['outputs']}, unknown


def main():
    licenses = subprocess.check_output(
        ['bash', str(ROOT / 'utils/list-licenses/list-licenses.sh'), str(ROOT)],
        cwd=ROOT, text=True,
    )
    license_manifest = [line.split('\t') for line in licenses.splitlines()]
    if any(len(row) != 3 for row in license_manifest):
        raise ValueError('Unexpected canonical license inventory format')
    (ROOT / 'bazel/rules/license_manifest.json').write_text(json.dumps(license_manifest, indent=2) + '\n')
    graphs = {arch: Graph(arch) for arch in ARCHES}
    owners = {}
    for graph in graphs.values():
        for name in sorted(graph.compiled):
            owner = None if (name in BCR_DEPENDENCIES or llvm_targets.target(name)
                             or llvm_targets.native_target(name, graph.targets[name])) else target_owner(graph.targets[name])
            if name in owners and owners[name] != owner:
                raise ValueError("Repository owner differs across architectures: " + name)
            owners[name] = owner
    inventories = {}
    codegen_rules = {}
    build_options = {}
    cmake_metadata = read_metadata(ROOT)
    rust_cxx = {}
    parity_sources = {}
    for arch in ARCHES:
        graph = graphs[arch]
        inventories[arch] = graph.generate()
        rust_cxx[arch] = graph.rust_cxx_configuration()
        codegen_rules.update(graph.codegen.rules)
        build_options[arch] = graph.build_options
        cmake_metadata[arch]['build_options'] = build_options[arch].pop('cmake_substitutions')
        parity_sources.update(graph.parity_sources)
    codegen_rules = common_googleapis_rules(relocate_tools(codegen_rules, owners))
    # Keep CMake node names until Graph.generate() finishes indexing generators.
    # Only the Google APIs input/compile directories are configuration-neutral.
    for graph in graphs.values():
        for target in graph.generated_targets.values():
            for group in target['groups']:
                for field in ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'copts'):
                    if field in group:
                        group[field] = [googleapis_path(value) for value in group[field]]
    parity_sources = googleapis_source_map(parity_sources)
    # Count CMake target names across both platforms, not generated compile
    # group names. A directory consumed only by one target is private.
    native_lz4 = {arch: graph.target('_lz4', graph.targets['_lz4'], native=True)
                  for arch, graph in graphs.items() if '_lz4' in graph.targets}
    consumers = collections.defaultdict(set)
    for arch, graph in graphs.items():
        targets = dict(graph.generated_targets, _lz4=native_lz4.get(arch, {'groups': []}))
        for name, target in targets.items():
            for group in target['groups']:
                for field in ('hdrs', 'additional_compiler_inputs'):
                    for item in group.get(field, []):
                        if isinstance(item, HeaderDirectory):
                            consumers[item.directory].add(name)
        for item in graph.rust_headers:
            if isinstance(item, HeaderDirectory):
                consumers[item.directory].add('rust_cxx')
    targets = {arch: graph.generated_targets for arch, graph in graphs.items()}
    external = HeaderLibraries(targets)
    native_sources = {
        src for arch_targets in targets.values() for target in arch_targets.values()
        for group in target.get('groups', ())
        if group.get('language') in {'C', 'CXX', 'ASM'} and not group.get('cmake_runtime')
        for src in group.get('srcs', ())
    }
    foreign = relocate_compilation(owners, targets)
    builds = {owner: BuildFile(owner, owners, consumers, targets, external_headers=external,
                               native_sources=native_sources)
              for owner in {None, *owners.values()}}
    for graph in graphs.values():
        graph.declare_configuration(builds, owners)
    for name, owner in sorted(owners.items()):
        builds[owner].target(name, {arch: graph.generated_targets[name]
                                  for arch, graph in graphs.items() if name in graph.generated_targets})
    for (owner, name, input_name), variants in sorted(foreign.items()):
        build = builds.setdefault(owner, BuildFile(
            owner, owners, consumers, targets, external_headers=external, native_sources=native_sources))
        build.target(name, variants, input_name=input_name)
    for owner, directories in SOURCE_HEADER_DIRECTORIES.items():
        build = builds.setdefault(owner, BuildFile(owner, owners, consumers, targets))
        patterns = [directory + '/**/*' + suffix for directory in directories
                    for suffix in SOURCE_HEADER_SUFFIXES.get(owner, CC_HEADER_SUFFIXES)]
        attrs = {'hdrs': Glob(patterns, SOURCE_HEADER_EXCLUDES.get(owner, ()))}
        if owner == 'contrib/libpqxx':
            # The pinned include/pqxx extensionless files are the public
            # wrapper headers, e.g. #include <pqxx/connection>.
            attrs['textual_hdrs'] = Glob(['include/pqxx/*'], ['include/pqxx/*.*'])
        build.rule('cc_library', SOURCE_HEADER_TARGETS[owner], attrs)
    external.declare(
        builds, lambda owner: BuildFile(owner, owners, consumers, targets, external_headers=external))
    # The public _lz4 wrapper serves rules_rs/BCR clients; the CMake link uses
    # the original group's ordered objects from its own pinned repository.
    lz4 = builds.setdefault('contrib/lz4', BuildFile('contrib/lz4', owners | {'_lz4': 'contrib/lz4'}, consumers))
    lz4.target('_lz4', native_lz4)
    boost_patterns = ['boost/**/*' + suffix for suffix in CC_HEADER_SUFFIXES]
    builds['contrib/boost'].rule('cc_library', 'boost_headers', {
        'hdrs': Glob(boost_patterns),
        'features': ['external_include_paths'],
    })
    llvm, llvm_native_targets, llvm_root_inputs = llvm_targets.declare_from_references(
        ROOT, {arch: graph.build for arch, graph in graphs.items()})
    builds[llvm_targets.PREFIX] = llvm
    dependency_inputs(builds, owners)
    root = builds[None]
    # Keep every configured dependency in //... even when it is only an
    # auxiliary library or tool outside the server's transitive closure.
    root.rule('filegroup', 'contrib', {'srcs': select({
        '@platforms//cpu:' + arch: [target_label(name, owners) for name in sorted(graph.compiled) if owners[name]]
        for arch, graph in graphs.items()
    })})
    llvm_overlay_inputs = set(llvm_source_overlays().values())
    rust_root_inputs = {label[3:] for config in rust_cxx.values()
                        for label in config['hdrs'] if label.startswith('//:')}
    root_exports = sorted(set().union(*(build.root_sources for build in builds.values()))
                          | llvm_overlay_inputs | llvm_root_inputs | rust_root_inputs)
    (ROOT / 'BUILD.bazel').write_text(
        root.load_statements() +
        'load("//bazel/initial_migration:root_inputs.bzl", "declare_inputs")\n\n'
        'package(default_visibility = ["//visibility:public"])\n\n'
        'declare_inputs()\n\nexports_files(' + starlark(root_exports) + ')\n\n' + root.text())
    (ROOT / 'bazel/headers.bzl').unlink(missing_ok=True)
    (ROOT / 'bazel/initial_migration/coverage.json').write_text(json.dumps(inventories, indent=2) + '\n')
    preamble = 'load("//bazel/rules:repository_paths.bzl", "repository_path")\n'
    (ROOT / 'bazel/rules/codegen_rules.bzl').write_text(render_rules(codegen_rules))
    (ROOT / 'bazel/rules/build_options_data.bzl').write_text(preamble + 'BUILD_OPTIONS = ' + starlark(build_options) + '\n')
    write_metadata(ROOT, cmake_metadata)
    (ROOT / 'bazel/rules/rust_cxx_flags.bzl').write_text(preamble + 'RUST_CXX = ' + starlark(rust_cxx) + '\n')
    # An external cc_library otherwise exports a quote include root that takes
    # precedence over CMake's system include path. Preserve the system boundary
    # with rules_cc's standard feature where every reference use agrees.
    repository_includes = collections.defaultdict(set)
    for graph in graphs.values():
        for target in graph.targets.values():
            for group in target.get('compileGroups', []):
                for include in group.get('includes', []):
                    path = include['path'].removeprefix(str(ROOT) + '/')
                    if path in REPOSITORIES:
                        repository_includes[path].add(bool(include.get('isSystem')))
    system_repositories = {path for path, kinds in repository_includes.items() if kinds == {True}}
    write_overlays(builds, [codegen_rules, rust_cxx], system_repositories)
    repository_files = {owner: package_sources(owner) for owner in builds if owner in REPOSITORIES}
    prune(ROOT, repository_files, write=True)
    prune_unused_exports(ROOT, write=True)
    parity_path = ROOT / 'bazel/initial_migration/any2bazel.json'
    parity = json.loads(parity_path.read_text())
    parity['target_map'] = {
        name: (llvm_targets.target(name).replace('@llvm-project', '@@' + llvm_targets.TOOLCHAIN_ROOT.removeprefix('external/'))
               if llvm_targets.target(name) else
               ('@@' + comparison_root(owner).removeprefix('external/') + '//:' if owner else ':') + name)
        for name, owner in sorted(owners.items())
        if any(graph.targets.get(name, {}).get('type') == 'EXECUTABLE' for graph in graphs.values())
    }
    # any2bazel source_map replaces the ENTIRE source path, unlike its include
    # map. Map individual files so distinct generated TUs cannot collapse.
    parity['ignore']['source_map'] = [{'from': source, 'to': identity}
                                      for source, identity in sorted(parity_sources.items())]
    generated_includes = [
        {'from': comparison_root(prefix), 'to': prefix} for prefix in sorted(REPOSITORIES)
    ] + [
        {'from': 'bazel-out/k8-opt/bin/' + comparison_root(prefix) + '/config/' + arch, 'to': '@build/' + arch}
        for arch, graph in graphs.items()
        for prefix in sorted({configuration_owner(path) for path in graph.configured if path.startswith('contrib/')})
    ]
    generated_includes += llvm_targets.include_mappings(llvm_targets.TOOLCHAIN_ROOT)
    for arch in ARCHES:
        generated_includes += [
            {'from': 'build_bazel_reference_' + arch + '/contrib/libcotp/include',
             'to': COPIED_INCLUDE_DIRS['contrib/libcotp/include']},
            {'from': 'build_bazel_reference_' + arch + '/contrib/librdkafka-cmake/auxdir',
             'to': '@build/' + arch + '/contrib/librdkafka-cmake'},
        ]
    generated_prefixes = {row['from'] for row in generated_includes}
    existing = [row for row in parity['ignore']['include_map']
                if row['from'] not in generated_prefixes and not row['from'].startswith((
                    'external/+contrib+', 'bazel-out/k8-opt/bin/external/+contrib+',
                    'external/llvm++llvm+llvm-project',
                    'bazel-out/k8-opt/bin/external/llvm++llvm+llvm-project'))]
    parity['ignore']['include_map'] = existing + generated_includes
    llvm_targets.update_any2bazel_mappings(parity, llvm_native_targets)
    parity_path.write_text(json.dumps(parity, indent=2) + '\n')
    (ROOT / 'REPO.bazel').write_text(
        'ignore_directories(' + starlark(['build', 'build_*', 'tmp'] + sorted(REPOSITORIES)) + ')\n')
    from initial_migration.configured_external_outputs import Migration as ExternalConfigs
    from initial_migration.configured_codegen_outputs import Migration as ExternalCodegen
    from initial_migration.configured_llvm_outputs import Migration as LlvmOutputs
    ExternalConfigs(ROOT).run(write=True)
    ExternalCodegen(ROOT).run(write=True)
    LlvmOutputs(ROOT).run(write=True)
    # Capture CMake at repository scope, then render native Bazel packages.
    # Existing native BUILD files must not hide CMake inputs on a second run.
    from initial_migration.first_party_packages import Build, Migration as NativePackages, native_directory
    native_packages = NativePackages(ROOT, stage='all', from_generator=True)
    native_builds = native_packages.prepare()
    for path, content in native_builds.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    from initial_migration.compact_external_options import (
        PackageFiles, compact as compact_external_options, format_and_prove, source_globs,
    )
    from initial_migration.normalize_cpp_builds import normalize, prove
    for owner, spec in REPOSITORIES.items():
        path = ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        if path.is_file():
            content, _ = compact_external_options(path.read_text(), spec['name'], package_sources(owner))
            path.write_text(normalize(content))
    from initial_migration.configured_native_snapshots import Migration as NativeSnapshots
    for path, content in NativeSnapshots(ROOT).prepare(native_builds).items():
        path.write_text(content)
    from initial_migration.configured_native_codegen import Migration as NativeCodegen
    NativeCodegen(ROOT).run(write=True)
    prune_published(ROOT, repository_files, write=True, consumers={'@clickhouse'} | {
        '@clickhouse_' + name for name in
        ('nuraft', 'grpc', 'librdkafka', 'croaring', 'arrow', 'google_cloud_cpp')
    })
    for path in native_builds:
        before = path.read_text()
        build = Build(ROOT, path, native_packages.files, native_packages.packages, before)
        files = PackageFiles(build.inventory)
        after = source_globs(before, files, collections.Counter(), [])
        if after != before:
            path.write_text(format_and_prove(ROOT, before, after, str(path), files, {}))
    prune_native_exports(ROOT, write=True)
    from initial_migration.library_runtime import normalize as normalize_library_runtime
    # Pruning must see direct runtime dependencies before ch_cc_library supplies
    # them. Keep the same order on regeneration, after native packages exist.
    for path in native_builds:
        package = path.parent.relative_to(ROOT).as_posix().removeprefix('.')
        if path.name == 'BUILD.bazel' and (not package or native_directory(package)):
            before = path.read_text()
            after = normalize(before, native=True)
            prove(before, after, path)
            after = normalize_library_runtime(after, package)
            if after != before:
                path.write_text(after)
    for spec in REPOSITORIES.values():
        path = ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        if path.is_file():
            before = path.read_text()
            after = normalize_library_runtime(before, '')
            if after != before:
                path.write_text(after)
    from initial_migration.base_cpp_dependencies import run as publish_base_cpp_dependencies
    publish_base_cpp_dependencies(ROOT, from_generator=True)
    from initial_migration.web_resources import run as publish_web_resources
    publish_web_resources(ROOT, write=True)
    from initial_migration.prune_native_cpp_inputs import run as prune_native_cpp_inputs
    prune_native_cpp_inputs(ROOT, write=True)
    from initial_migration.native_proto_owners import transfer as publish_native_protos
    publish_native_protos(ROOT, write=True)
    # Publishing base moves its BUILD file. Resolve globs against the final
    # package boundaries and retain the exact order of every original file.
    from initial_migration.first_party_packages import git_inventory
    header_inventory = git_inventory(ROOT)
    build_paths = {
        ROOT / name for name in subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
             '--', 'BUILD', 'BUILD.bazel', '**/BUILD', '**/BUILD.bazel'], cwd=ROOT,
        ).decode().split('\0')
        if name and (ROOT / name).is_file()
    }
    final_packages = {
        path.parent.relative_to(ROOT).as_posix().removeprefix('.') for path in build_paths
    }
    for path in sorted(build_paths):
        package = path.parent.relative_to(ROOT).as_posix().removeprefix('.')
        if not package or native_directory(package):
            before = path.read_text()
            build = Build(ROOT, path, header_inventory, final_packages, before)
            files = PackageFiles(build.inventory)
            after = source_globs(before, files, collections.Counter(), [], headers=True)
            if after != before:
                path.write_text(format_and_prove(ROOT, before, after, str(path), files, {}, headers=True))
    from initial_migration.native_include_deps import run as prune_native_include_deps
    prune_native_include_deps(ROOT, write=True)
    format_build_files()
    # Factor formatted lists last: earlier normalization folds equal CPU
    # branches and would discard required no-match errors.
    from initial_migration.factor_build_selections import run as factor_build_selections
    selection_builds = {
        path for path in build_paths
        if not (package := path.parent.relative_to(ROOT).as_posix().removeprefix('.'))
        or native_directory(package)
    }
    selection_builds.update(
        ROOT / 'bazel/dependencies' / (spec['name'] + '.BUILD.bazel')
        for spec in REPOSITORIES.values()
    )
    factor_build_selections(ROOT, selection_builds)
    # Rule transfers can leave adjacent literal lists. Preserve select()
    # and its no-match error after the CPU selections have been factored.
    from initial_migration.compact_build import Source, simplify_list_expressions
    for path in sorted(selection_builds):
        before = path.read_text()
        after = before
        while True:
            updated = simplify_list_expressions(Source(after), preserve_selects=True)
            if updated == after:
                break
            after = updated
        if after != before:
            path.write_text(after)
    format_build_files()
    format_build_files()
    for arch, inventory in inventories.items():
        print(arch, inventory['native_targets'], 'native targets,', inventory['compile_actions'], 'compile actions')


if __name__ == '__main__':
    main()
