#!/usr/bin/env python3
"""Supplement any2bazel with compile variants and per-binary object closures.

ClickHouse compiles several sources more than once with different ISA flags or
defines. Keep those variants separate instead of selecting one arbitrary TU
per source path. The upstream scripts and their raw action models are unchanged.
"""

import argparse
from collections import Counter, defaultdict
from dataclasses import asdict, replace
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration import llvm_targets
from compare_cmake_actions import expanded_arguments
from initial_migration.rust_archives import PACKAGE_TARGETS as RUST_ARCHIVES

OBJECT_ACTIONS = {'CppCompile', 'ObjcCompile', 'Assemble', 'PreprocessAssemble', 'NasmCompile'}
ARCHIVE_ACTIONS = {'CppArchive', 'ClickHouseNativeArchive'}
WHOLE_START = {'-Wl,--whole-archive', '-Wl,-whole-archive', '--whole-archive'}
WHOLE_END = {'-Wl,--no-whole-archive', '-Wl,-no-whole-archive', '--no-whole-archive'}


class BuildIncludeEvidence:
    """Explain an absent CMake build include using its trace, files and Ninja."""

    HEADER_SUFFIXES = {'.h', '.hh', '.hpp', '.hxx', '.inc', '.ipp', '.inl', '.tcc', '.def'}

    def __init__(self, build, repo_root, cfg):
        from initial_migration.reference_readers import read_ninja

        self.build = build
        self.prefix = cfg.map_include(os.path.relpath(build, repo_root))
        self.nodes = {}
        self.writes = []
        self.cache = {}
        self.rejections = {}
        self.reason = None
        ninja = build / 'build.ninja'
        trace = build / 'trace.jsonl'
        try:
            if not re.fullmatch(r'@build/[^/]+', self.prefix):
                raise ValueError('CMake build directory has no unique @build mapping')
            self.nodes = read_ninja(ninja)
            records = 0
            version = False
            with trace.open() as lines:
                for line in lines:
                    # CMake can interleave unstructured status messages with
                    # its JSON trace. A malformed JSON record is not evidence.
                    if not line.startswith('{'):
                        continue
                    record = json.loads(line)
                    version |= 'version' in record
                    cmd = record.get('cmd', '').lower()
                    args = record.get('args', [])
                    if not cmd:
                        continue
                    records += 1
                    outputs = []
                    directories = False
                    if cmd == 'configure_file' and len(args) >= 2:
                        outputs = args[1:2]
                    elif cmd == 'file' and args:
                        mode = args[0].upper()
                        if mode in {'WRITE', 'APPEND'} and len(args) >= 2:
                            outputs = args[1:2]
                        elif mode in {'GENERATE', 'CONFIGURE'} and 'OUTPUT' in args:
                            outputs = args[args.index('OUTPUT') + 1:args.index('OUTPUT') + 2]
                        elif mode in {'COPY', 'INSTALL'} and 'DESTINATION' in args:
                            outputs = args[args.index('DESTINATION') + 1:args.index('DESTINATION') + 2]
                            directories = True
                        elif mode in {'RENAME', 'CREATE_LINK'} and len(args) >= 3:
                            outputs = args[2:3]
                    for value in outputs:
                        self.writes.append((os.path.normpath(value), directories))
            if not version or not records or not self.nodes:
                raise ValueError('CMake JSON trace or Ninja graph is incomplete')
            self.sources = {
                'ninja_sha256': hashlib.sha256(ninja.read_bytes()).hexdigest(),
                'trace_sha256': hashlib.sha256(trace.read_bytes()).hexdigest(),
                'trace_records': records,
            }
        except (OSError, ValueError, KeyError, IndexError) as error:
            self.reason = str(error)
            self.sources = {'unavailable': self.reason}

    @classmethod
    def possible_header(cls, path):
        # Standard-library and generated headers may be extensionless. Do not
        # treat a missing suffix as proof that an unknown file is irrelevant.
        return not path.suffix or path.suffix in cls.HEADER_SUFFIXES

    def write_matches(self, path, output, directory):
        if os.path.isabs(output):
            written = Path(output)
            return path == written or (directory and path.is_relative_to(written))
        # A relative configure_file output uses CMAKE_CURRENT_BINARY_DIR,
        # which a trace included from a .cmake module need not name. Refuse to
        # exclude a matching file even when that relative location is ambiguous.
        relative = path.relative_to(self.build).as_posix()
        if directory:
            return relative.startswith(output + '/') or ('/' + output + '/') in ('/' + relative)
        return relative == output or relative.endswith('/' + output)

    def inspect(self, target, include):
        key = (target, include)
        if key in self.cache:
            return self.cache[key]
        self.cache[key] = None
        if not include.startswith(self.prefix + '/'):
            return None
        def reject(reason, **evidence):
            self.rejections[key] = {'include': include, 'reason': reason, **evidence}
            return None
        if self.reason:
            return reject('Ninja or trace evidence unavailable', error=self.reason)
        directory = Path(os.path.normpath(self.build / include[len(self.prefix) + 1:]))
        if not directory.is_relative_to(self.build):
            return reject('include directory is outside CMake build', directory=str(directory))
        order = 'cmake_object_order_depends_target_' + target
        if order not in self.nodes or self.nodes[order]['rule'] != 'phony':
            return reject('target has no Ninja order phony', ninja_order_target=order,
                          ninja_rule=self.nodes.get(order, {}).get('rule'))
        reached = set()
        pending = [order]
        while pending:
            path = pending.pop()
            if path in reached:
                continue
            reached.add(path)
            node = self.nodes.get(path)
            if node is None and path.startswith('cmake_object_order_depends_target_'):
                # A truncated graph does not prove a missing target has no
                # generated headers.
                return reject('Ninja order dependency has no declaration', dependency=path,
                              ninja_order_target=order)
            if node and node['rule'] == 'phony':
                pending.extend(node['inputs'])
            elif node and node['rule'] == 'CUSTOM_COMMAND':
                for output in node['outputs']:
                    written = Path(os.path.normpath(self.build / output))
                    if written.is_relative_to(directory) and self.possible_header(written):
                        # Ninja declares the header even if it is not present
                        # on disk after the build or after remote eviction.
                        return reject('Ninja target requires a generated header', file=str(written),
                                      ninja_order_target=order, ninja_rule=node['rule'])
        for output, is_directory in self.writes:
            written = Path(output)
            # An absolute output remains relevant even if the file went
            # missing after configuration. Relative outputs are checked
            # against actual files below; unexplained files also fail closed.
            if (written.is_absolute() and not is_directory and
                    written.is_relative_to(directory) and self.possible_header(written)):
                return reject('CMake trace writes a header in the include directory', file=str(written))
        unrelated = []
        def walk_error(error):
            raise error
        try:
            if directory.exists() and not directory.is_dir():
                return reject('include directory is not a directory', directory=str(directory))
            for parent, _, files in os.walk(directory, onerror=walk_error):
                for name in files:
                    path = Path(parent) / name
                    if not self.possible_header(path):
                        continue
                    relative = path.relative_to(self.build).as_posix()
                    node = self.nodes.get(relative) or self.nodes.get(str(path))
                    # An extensionless CMake executable, library or phony
                    # binary alias is not a header. Never extend this to .h.
                    if (not path.suffix and node and
                            (node['rule'] == 'phony' or '_LINKER' in node['rule'])):
                        continue
                    write = next(((output, directory_write)
                                  for output, directory_write in self.writes
                                  if self.write_matches(path, output, directory_write)), None)
                    if write is not None:
                        return reject('include file matches a CMake trace write', file=relative,
                                      trace_output=write[0], trace_output_is_directory=write[1])
                    if not node or node['rule'] != 'CUSTOM_COMMAND':
                        return reject('include file has no Ninja custom command', file=relative,
                                      ninja_rule=node['rule'] if node else None)
                    if any(output in reached for output in node['outputs']):
                        return reject('Ninja target requires an include file', file=relative,
                                      ninja_order_target=order, ninja_rule=node['rule'])
                    unrelated.append(relative)
        except OSError as error:
            return reject('include directory could not be inspected', directory=str(directory),
                          error=str(error))
        result = {'include': include, 'ninja_order_target': order,
                  'ninja_generated_headers_not_required_by_target': sorted(unrelated),
                  'configured_headers': [], 'unexplained_headers': []}
        self.cache[key] = result
        return result

    def classify(self, difference):
        row = asdict(difference)
        if row['kind'] != 'includes_diff' or row['severity'] != 'error' or not row['cmake_only']:
            return row
        proofs = [self.inspect(row['target'], include) for include in row['cmake_only']]
        # A mixed row remains an error if even one original include root
        # cannot be justified. No source or real header mismatch is removed.
        if all(proof is not None for proof in proofs):
            row['original_severity'] = row['severity']
            row['severity'] = 'warn'
            row['build_include_evidence'] = proofs
        else:
            rejected = [self.rejections[(row['target'], include)] for include in row['cmake_only']
                        if (row['target'], include) in self.rejections]
            if rejected:
                row['build_include_rejections'] = rejected
        return row


def checked_cargo_sources(argv, declared, mounts, output_base):
    """Check each staged file against the captured action's individual or directory inputs."""
    from functools import cache

    sources = set()
    i = 0
    while i < len(argv):
        flag = argv[i]
        i += 1
        if flag not in ('--source', '--sources'):
            continue
        if i == len(argv) or argv[i].startswith('-'):
            raise ValueError(f'Cargo option lacks a source file: {flag}')
        while i < len(argv) and not argv[i].startswith('-'):
            sources.add(argv[i])
            i += 1
            if flag == '--source':
                break
    resolved = cache(Path.resolve)
    for value in sources:
        path = Path(value)
        roots = [Path(root) for root in mounts.values() if value.startswith(root + '/')]
        if len(roots) != 1 or path.is_absolute() or '..' in path.parts or str(path) != value:
            raise ValueError(f'Cargo source is outside its declared mount: {value}')
        if value in declared:
            continue
        root, = roots
        # A source directory represents its physical files in the pinned Bazel repository.
        directory = next((p for p in path.parents if str(p) in declared and p.is_relative_to(root)), None)
        if output_base is None or not root.parts or root.parts[0] != 'external' or directory is None:
            raise ValueError(f'Cargo source has no declared input: {value}')
        repository = resolved(output_base / root)
        physical = resolved(output_base / directory)
        file = output_base / path
        leaf = file.resolve() if file.is_symlink() else resolved(file.parent) / file.name
        if not physical.is_relative_to(repository) or not file.is_file() or not leaf.is_relative_to(physical):
            raise ValueError(f'Cargo source is missing from its declared directory: {value}')
    return sources


class ArchiveEvidence:
    """Walk actual link/ar/MRI operands, never infer archive members from depsets."""

    def __init__(self, native, assembly, mri, cfg, build, repo_root, bazel_output_base=None):
        from extract_bazel import _build_path_index, DepsetResolver
        self.producers = defaultdict(list)
        self.links = defaultdict(list)
        self.cache = {}
        self.cfg = cfg
        self.build = build
        self.repo_root = repo_root
        self.bazel_output_base = bazel_output_base
        self.custom_assembly = {}
        self.cargo_native = {}
        for filename in (native, assembly, mri):
            if filename is None:
                continue
            graph = json.loads(filename.read_text())
            artifacts = _build_path_index(graph)
            labels = {t['id']: t['label'] for t in graph.get('targets', [])}
            resolver = DepsetResolver(graph)
            for action in graph.get('actions', []):
                outputs = tuple(artifacts[p] for p in action.get('outputIds', []))
                record = (action, labels[action['targetId']], outputs, artifacts, resolver)
                for path in outputs:
                    # members() can reach only link operands, llvm-ar members,
                    # Symlink operands or an MRI FileWrite. Do not index the
                    # toolchain's shareable header Symlinks or dependency files.
                    if path.endswith(('.a', '.lo', '.o', '.mri')):
                        self.producers[path].append(record)
                if action.get('mnemonic') in {'CppLink', 'ClickHouseNativeLink'}:
                    self.links[labels[action['targetId']]].append(record)

    def producer(self, path):
        records = self.unique_records(self.producers.get(path, ()))
        if len(records) > 1:
            candidates = [(label, action.get('mnemonic'), action.get('actionKey'))
                          for action, label, _, _, _ in records]
            raise ValueError(f'Multiple aquery producers for needed artifact {path}: {candidates}')
        return records[0] if records else None

    def unique_records(self, records):
        # Bazel's union aquery can return a link/compile already present in
        # the native aquery. DepSet IDs are local to each aquery JSON file;
        # compare resolved inputs, command and outputs before coalescing.
        unique = []
        for record in records:
            action, label, outputs, _, _ = record
            for previous in unique:
                other, other_label, other_outputs, _, _ = previous
                if (action.get('actionKey')
                        and action.get('actionKey') == other.get('actionKey')
                        and label == other_label and outputs == other_outputs
                        and action.get('mnemonic') == other.get('mnemonic')
                        and action.get('environmentVariables') == other.get('environmentVariables')
                        and expanded_arguments(action) == expanded_arguments(other)
                        and self.inputs(record) == self.inputs(previous)):
                    break
            else:
                unique.append(record)
        return unique

    @staticmethod
    def inputs(record):
        action, _, _, artifacts, resolver = record
        return {artifacts[ident] for ident in resolver.resolve(action.get('inputDepSetIds', []))}

    def skim_cargo(self, source, record):
        """Prove the Cargo input contract that compiles Skim's native bridge.

        This inspects the captured producer, not its target name in a link
        plan. Cargo archive member bytes remain the responsibility of the
        archive and final ELF comparisons.
        """
        from rules.cmake_cargo import native_flags

        action, label, outputs, _, _ = record
        argv = expanded_arguments(action)
        evidence_output = str(Path(source).with_name('cargo-evidence.json'))
        if (action.get('mnemonic') != 'ClickHouseCargo' or label != '//bazel/rust:skim_cargo'
                or not source.endswith('/bazel/rust/skim_cargo/lib_ch_rust_skim_rust.a')
                or set(outputs) != {source, evidence_output}
                or not argv or not argv[0].endswith('/bazel/rules/cmake_cargo')):
            raise ValueError('Skim native input is not the captured skim_cargo ClickHouseCargo action')

        def values(option):
            result = []
            for index, value in enumerate(argv[1:], 1):
                if value == option:
                    if index + 1 >= len(argv):
                        raise ValueError(f'Skim Cargo option lacks an operand: {option}')
                    result.append(argv[index + 1])
                elif value.startswith(option + '='):
                    result.append(value[len(option) + 1:])
            return result

        def single(option):
            operands = values(option)
            if len(operands) != 1:
                raise ValueError(f'Skim Cargo requires exactly one {option}, got {len(operands)}')
            return operands[0]

        manifest = single('--manifest')
        workspace_manifest = single('--workspace-manifest')
        config = single('--config')
        tools = {name: single('--' + name) for name in
                 ('cargo', 'rustc', 'rust-sysroot-anchor', 'cc', 'cxx', 'ar')}
        declared = self.inputs(record)
        if (single('--output') != source or single('--evidence') != evidence_output
                or single('--cargo-package') != '_ch_rust_skim_rust'
                or argv.count('--native-compilation') != 1
                or values('--cargo-feature') or values('--openssl-headers') or values('--cmake')
                or single('--native-source-root') != self.repo_root
                or not {manifest, workspace_manifest, config, *tools.values()}.issubset(declared)
                or self.cfg.map_include(config) != 'contrib/corrosion-cmake/config.toml.in'):
            raise ValueError('Skim Cargo package, native mode, tools, configuration or inputs differ')

        mounts = {}
        for index, arg in enumerate(argv):
            if arg != '--mount':
                continue
            if index + 2 >= len(argv):
                raise ValueError('Skim Cargo --mount lacks two operands')
            original, destination = argv[index + 1:index + 3]
            if destination in mounts:
                raise ValueError('Skim Cargo mount has duplicate destination: ' + destination)
            mounts[destination] = original
        for destination in ('rust/workspace', 'contrib/rust_vendor', 'contrib/llvm-project', 'contrib/sysroot'):
            if destination not in mounts or self.cfg.map_include(mounts[destination]) != destination:
                raise ValueError('Skim Cargo lacks its declared repository mount: ' + destination)
        sources = checked_cargo_sources(argv[1:], declared, mounts, self.bazel_output_base)
        sysroot = mounts['contrib/sysroot'].rstrip('/') + '/'
        if not any(path.startswith(sysroot) for path in sources):
            raise ValueError('Skim Cargo has no declared sysroot sources')

        def staged(destination, relative):
            return str(Path(mounts[destination]) / relative)

        lockfile = Path(self.repo_root) / 'rust/workspace/Cargo.lock'
        versions = {}
        for package in tomllib.loads(lockfile.read_text())['package']:
            if package['name'] in {'cxx', 'cxx-build'}:
                if package['name'] in versions:
                    raise ValueError('Ambiguous Skim Cargo dependency in Cargo.lock: ' + package['name'])
                versions[package['name']] = package['version']
        if versions.keys() != {'cxx', 'cxx-build'}:
            raise ValueError('Skim Cargo.lock is missing cxx or cxx-build')
        bridge = {staged('rust/workspace', name) for name in (
            'Cargo.toml', 'Cargo.lock', 'skim/Cargo.toml',
            'skim/build.rs', 'skim/src/lib.rs', 'skim/src/check_abi.cpp',
        )}
        bridge.update(staged('contrib/rust_vendor', name) for name in (
            'cxx-' + versions['cxx'] + '/Cargo.toml',
            'cxx-' + versions['cxx'] + '/build.rs',
            'cxx-' + versions['cxx'] + '/src/cxx.cc',
            'cxx-build-' + versions['cxx-build'] + '/Cargo.toml',
            'cxx-build-' + versions['cxx-build'] + '/src/lib.rs',
        ))
        headers = {staged('contrib/llvm-project', name) for name in (
            'libcxx/include/__config', 'libcxx/include/__config_site', 'libcxx/include/string',
        )}
        if (manifest != staged('rust/workspace', 'skim/Cargo.toml')
                or workspace_manifest != staged('rust/workspace', 'Cargo.toml')
                or not (bridge | headers).issubset(sources)):
            absent = sorted((bridge | headers) - sources)
            raise ValueError(f'Skim Cargo bridge, manifests, cxx or libc++ sources are not staged: {absent}')

        cache = {}
        for line in (self.build / 'CMakeCache.txt').read_text().splitlines():
            for name in ('RUSTFLAGS', 'RUST_CFLAGS', 'RUST_CXXFLAGS', 'Rust_CARGO_TARGET'):
                prefix = name + ':INTERNAL='
                if line.startswith(prefix):
                    cache[name] = shlex.split(line[len(prefix):])
        if cache.keys() != {'RUSTFLAGS', 'RUST_CFLAGS', 'RUST_CXXFLAGS', 'Rust_CARGO_TARGET'}:
            raise ValueError('CMakeCache.txt lacks native Rust target or C/C++/Rust flags')
        # The Cargo driver adds sysroot/linker/sanitizer metadata flags. Reject
        # extra caller-supplied metadata, including a second -Cmetadata that
        # Rust would append to Cargo's own package metadata.
        reference_rust = []
        metadata = []
        flags = iter(cache['RUSTFLAGS'])
        for flag in flags:
            if flag in {'-C', '-A'}:
                operand = next(flags, None)
                if operand is None:
                    raise ValueError('CMakeCache.txt contains an incomplete Rust ' + flag)
                flag += operand
            # Compare CMake's Rust spelling to MODULE.bazel's toolchain
            # caller spelling. native_rust_flags restores the CMake spelling.
            if flag.startswith('-Ctarget_feature='):
                flag = '-Ctarget-feature=' + flag.removeprefix('-Ctarget_feature=')
            if flag.startswith(('-Ctarget-cpu=', '-Ctarget-feature=', '-Csymbol-mangling-version=')):
                reference_rust.append(flag)
            elif flag.startswith('-Cmetadata='):
                metadata.append(flag)
        if (values('--rust-flag') != reference_rust
                or metadata != ['-Cmetadata=no-sanitizer']):
            raise ValueError('Skim Cargo target or sanitizer metadata flags differ from native CMake')
        target = single('--target')
        if ([target] != cache['Rust_CARGO_TARGET']
                or target not in ('x86_64-unknown-linux-gnu', 'aarch64-unknown-linux-gnu')):
            raise ValueError('Skim Cargo target differs from recorded CMake: ' + target)
        cxx = cache['RUST_CXXFLAGS']
        gcc = [flag.removeprefix('--gcc-toolchain=') for flag in cxx if flag.startswith('--gcc-toolchain=')]
        if len(gcc) != 1 or cxx.count('--sysroot') != 1:
            raise ValueError('Native Skim RUST_CXXFLAGS have ambiguous gcc/sysroot operands')
        sysroot = cxx[cxx.index('--sysroot') + 1]
        actual_c, actual_cxx = native_flags(
            SimpleNamespace(native_c_flag=values('--native-c-flag'), native_cxx_flag=values('--native-cxx-flag')),
            Path(self.repo_root), Path(gcc[0]), Path(sysroot),
        )
        if actual_c != cache['RUST_CFLAGS'] or actual_cxx != cxx:
            raise ValueError('Skim Cargo native C/C++ flags differ from recorded CMakeCache')
        return {
            'producer': label,
            'raw_archive': source,
            'cargo_package': '_ch_rust_skim_rust',
            'rust_target': target,
            'native_compilation': True,
            'rust_caller_flags': reference_rust,
            'sanitizer_metadata': 'no-sanitizer (added by bazel/rules/cmake_cargo.py)',
            'native_c_flags': values('--native-c-flag'),
            'native_cxx_flags': values('--native-cxx-flag'),
            'required_bridge_sources': sorted(bridge),
            'required_native_headers': sorted(headers),
            'sysroot': mounts['contrib/sysroot'],
            'tools': tools,
            'limits': 'Captured Cargo producer and declared native inputs/flags only; Cargo member byte identity is not checked.',
        }

    def members(self, path, active=()):
        """Return ordered leaf objects/Rust imports; unresolved inputs are leaves."""
        if path in active:
            return [('unknown', path, 'archive/symlink cycle')]
        if path in self.cache:
            return self.cache[path]
        record = self.producer(path)
        if record is None:
            return [('unknown', path, 'no captured producing action')]
        action, label, outputs, _, _ = record
        mnemonic = action.get('mnemonic')
        try:
            argv = expanded_arguments(action)
            if path.endswith('.o') and mnemonic in OBJECT_ACTIONS:
                result = [('object', path, label)]
            elif mnemonic == 'Genrule' and path.endswith('.o'):
                # This CMake CUSTOM_COMMAND is not a compile_commands.json TU.
                # Accept only the two actual commands emitted for HDFS by
                # bazel/codegen.py, with their declared tools, source and outs.
                if (label != '//bazel:generate_x86_64_hdfs_crc'
                        or not path.endswith('/bazel/gen/x86_64/contrib/libhdfs3-cmake/crc_iscsi_v_pcl.o')
                        or set(outputs) != {path, path + '.asm'}
                        or len(argv) != 3 or Path(argv[0]).name not in {'bash', 'sh'}
                        or argv[1] != '-c'):
                    raise ValueError('unsupported generated assembly provider or shell')
                declared = self.inputs(record)
                lexer = shlex.shlex(argv[2], posix=True, punctuation_chars=';&|()<>')
                lexer.whitespace_split = True
                lexer.commenters = ''
                command = list(lexer)
                if (len(command) >= 3 and command[0] == 'source'
                        and command[1].endswith('/tools/genrule/genrule-setup.sh')
                        and command[1] in declared and command[2] == ';'):
                    command = command[3:]
                if len(command) != 15:
                    raise ValueError('HDFS assembly requires prepare_hdfs_crc && nasm with exact operands')
                prepare, source, converted = command[:3]
                nasm = command[4]
                # source_map contains only CMake compilation database TUs.
                # Use the declared repository prefix for this CUSTOM_COMMAND
                # source, retaining every byte of its in-repository name.
                original = self.cfg.map_include(source)
                if (not prepare.endswith('/bazel/prepare_hdfs_crc') or Path(nasm).name != 'nasm'
                        or not {prepare, source, nasm}.issubset(declared)
                        or original != 'contrib/libhdfs3/src/common/crc_iscsi_v_pcl.asm'
                        or converted != path + '.asm'
                        or command != [prepare, source, converted, '&&', nasm,
                                       '-f', 'elf64', '-g', '-F', 'dwarf', '-D', 'LINUX',
                                       '-o', path, converted]):
                    raise ValueError('HDFS NASM command, declared source, tools or outputs do not match')
                self.custom_assembly[path] = {
                    'producer': label,
                    'source': original,
                    'conversion': prepare,
                    'assembler': nasm,
                    'flags': ['-f', 'elf64', '-g', '-F', 'dwarf', '-D', 'LINUX'],
                    'limits': 'Checks the declared Yasm-to-NASM source conversion and operands; not object byte equality.',
                }
                result = [('object', path, label)]
            elif mnemonic == 'FileWrite':
                raise ValueError('FileWrite is not an object or archive producer')
            elif mnemonic == 'Symlink':
                candidates = [p for p in self.inputs(record) if p.endswith(('.a', '.lo', '.o'))]
                if len(candidates) != 1:
                    raise ValueError(f'symlink has {len(candidates)} possible archive/object inputs')
                result = self.members(candidates[0], (*active, path))
            elif mnemonic in ARCHIVE_ACTIONS:
                # Both the Linux hermetic-llvm CppArchive and cmake_link use
                # llvm-ar rcsD <output> <objects...>. Reject other operations.
                if len(argv) < 3 or not Path(argv[0]).name.startswith('llvm-ar'):
                    raise ValueError('archive action does not execute llvm-ar')
                if argv[1].lstrip('-') not in {'rcsD', 'rcs', 'rcsDT', 'rcsT'} or argv[2] != path:
                    raise ValueError(f'unsupported llvm-ar operation/output {argv[1:3]}')
                declared = self.inputs(record)
                result = []
                for member in argv[3:]:
                    if not member.endswith('.o') or member.startswith('-') or member not in declared:
                        raise ValueError(f'archive operand is not a declared object: {member}')
                    result.extend(self.members(member, (*active, path)))
            elif mnemonic == 'ClickHouseRustArchive':
                # Match the exact run_shell contract from cmake_link.bzl.
                if len(argv) < 5 or '"$1" -M < "$2"' not in argv:
                    raise ValueError('unsupported Rust archive shell command')
                ar, script = argv[-2:]
                if not Path(ar).name.startswith('llvm-ar') or not script.endswith('.mri'):
                    raise ValueError('missing llvm-ar or MRI argv operand')
                declared = self.inputs(record)
                writer = self.producer(script)
                if script not in declared or writer is None or writer[0].get('mnemonic') != 'FileWrite':
                    raise ValueError(f'MRI operand lacks a declared, captured FileWrite: {script}')
                content = writer[0].get('fileContents')
                if content is None:
                    raise ValueError(f'MRI FileWrite contents unavailable: {script}')
                lines = content.splitlines()
                if (len(lines) < 4 or lines[0] != 'CREATE ' + path
                        or lines[-2:] != ['SAVE', 'END'] or not lines[1].startswith('ADDLIB ')):
                    raise ValueError(f'unsupported MRI CREATE/ADDLIB/SAVE/END in {script}')
                operands = [(lines[1][len('ADDLIB '):], '.a')]
                for line in lines[2:-2]:
                    if not line.startswith('ADDMOD '):
                        raise ValueError(f'unsupported MRI directive in {script}: {line}')
                    operands.append((line[len('ADDMOD '):], '.o'))
                result = []
                for operand, suffix in operands:
                    if not operand.endswith(suffix) or operand not in declared:
                        raise ValueError(f'MRI operand is not a declared {suffix}: {operand}')
                    result.extend(self.members(operand, (*active, path)))
            elif mnemonic == 'ClickHouseRustSymbols':
                # --reference archives are localization inputs; they are NOT
                # bundled members. This proves only a specific processed Rust
                # import, never its internal Rust member bytes or flags.
                rust_labels = {'//bazel/rust:' + name + '_processed': name
                               for name in RUST_ARCHIVES.values()}
                if label not in rust_labels or argv.count('--source') != 1 or argv.count('--output') != 1:
                    raise ValueError('unrecognized processed Rust archive provider')
                source = argv[argv.index('--source') + 1]
                output = argv[argv.index('--output') + 1]
                if output != path or not source.endswith('.a') or source not in self.inputs(record):
                    raise ValueError('Rust --source/--output do not match declared archive artifacts')
                if rust_labels[label] == 'skim':
                    origin = self.producer(source)
                    if origin and origin[0].get('mnemonic') == 'ClickHouseCargo':
                        self.cargo_native[path] = self.skim_cargo(source, origin)
                    elif source.endswith('/bazel/rust/skim_cargo/lib_ch_rust_skim_rust.a'):
                        raise ValueError('Skim Cargo archive lacks a captured ClickHouseCargo producer')
                result = [('rust', path, rust_labels[label])]
            else:
                raise ValueError(f'unsupported producer {mnemonic} ({label})')
        except (ValueError, IndexError) as error:
            result = [('unknown', path, str(error))]
        self.cache[path] = result
        return result

    def link(self, label, name):
        records = self.unique_records(
            record for record in self.links.get(label, [])
            if any(path.endswith('/bin/' + name) for path in record[2]))
        if len(records) != 1:
            raise ValueError(f'Expected one actual {label} link action, got {len(records)}')
        record = records[0]
        argv = expanded_arguments(record[0])
        declared = self.inputs(record)
        uses = defaultdict(list)
        rust = defaultdict(list)
        unresolved = []
        lazy = False
        whole = False
        skip_output = False
        for index, arg in enumerate(argv[1:], 1):
            if skip_output:
                skip_output = False
                continue
            if arg == '-o':
                skip_output = True
                continue
            if arg in {'-Wl,--start-lib', '--start-lib'}:
                if lazy:
                    raise ValueError(f'{label}: nested --start-lib')
                lazy = True
            elif arg in {'-Wl,--end-lib', '--end-lib'}:
                if not lazy:
                    raise ValueError(f'{label}: unmatched --end-lib')
                lazy = False
            elif arg in WHOLE_START:
                whole = True
            elif arg in WHOLE_END:
                whole = False
            elif not arg.startswith('-') and arg.endswith(('.o', '.a', '.lo')):
                if arg not in declared:
                    unresolved.append({'link_index': index, 'operand': arg,
                                       'reason': 'link operand is not a declared input'})
                    continue
                for member_index, (kind, path, value) in enumerate(self.members(arg)):
                    position = (index, member_index)
                    if kind == 'object':
                        mandatory = whole or (arg.endswith('.o') and not lazy)
                        uses[path].append((position, mandatory))
                    elif kind == 'rust':
                        entry = {'archive': path, 'link_operand': arg, 'position': position}
                        if path in self.cargo_native:
                            entry['cargo_native_proof'] = self.cargo_native[path]
                        rust[value].append(entry)
                    else:
                        unresolved.append({'link_index': index, 'operand': arg,
                                           'member': path, 'reason': value})
        if lazy or whole or skip_output:
            raise ValueError(f'{label}: unterminated link grouping or -o')
        return {'argv': argv, 'uses': uses, 'rust': rust, 'unresolved': unresolved,
                'custom_assembly': {path: row for path, row in self.custom_assembly.items() if path in uses}}


def target_configuration(model, cfg, linked_objects):
    """Keep the configuration that produces the main ClickHouse binary.

    The raw model merges target and execution configurations under each label.
    A correct host-tool compile must not hide a broken target compile.
    """
    label = cfg.target_map.get('clickhouse', ':clickhouse')
    roots = {
        path.removesuffix('/bin/clickhouse')
        for action in model.targets[label].actions if action.mnemonic == 'CppLink'
        for path in action.outputs if path.endswith('/bin/clickhouse')
    }
    if len(roots) != 1:
        raise ValueError(f'Expected one ClickHouse output configuration, got {sorted(roots)}')
    root = roots.pop() + '/'
    excluded = Counter()
    targets = {}
    for label, target in model.targets.items():
        actions = []
        for action in target.actions:
            if any(path.startswith(root) for path in action.outputs):
                if not all(path.startswith(root) for path in action.outputs):
                    raise ValueError(f'Action spans output configurations: {label}, {action.outputs}')
                actions.append(action)
            elif action.mnemonic in {'CppCompile', 'ObjcCompile'} and linked_objects.intersection(action.outputs):
                # Imported runtimes can legitimately be compiled in a
                # transitioned configuration. Admit only their linked files,
                # never an unrelated bootstrap compile with the same label.
                actions.append(action)
            else:
                excluded[action.mnemonic] += 1
        targets[label] = replace(target, actions=actions)
    return replace(model, targets=targets), root, dict(excluded)


def ninja_link_objects(build, reference, cfg):
    """Read direct and whole-archive object operands from the native Ninja build.

    A File API OBJECT_LIBRARY can be inserted into either an ordinary archive
    or the executable. Only the executable operands decide which must be
    nonlazy. For whole archives, inspect their Ninja ar inputs as well.
    """
    from initial_migration.reference_readers import ninja_words as words

    commands = json.loads((build / 'compile_commands.json').read_text())
    sources = {}
    for command in commands:
        output = os.path.normpath(os.path.join(command['directory'], command['output']))
        source = os.path.normpath(os.path.join(command['directory'], command['file']))
        sources[output] = os.path.relpath(source, reference.repo_root)

    def object_key(path):
        absolute = os.path.normpath(os.path.join(build, path))
        owner = re.search(r'CMakeFiles/([^/]+)\.dir/', path)
        if not owner or absolute not in sources:
            raise ValueError('Unmapped Ninja link object: ' + path)
        return owner[1], cfg.map_source(sources[absolute])

    def explicit_inputs(output, required_rule):
        query = subprocess.check_output(['ninja', '-C', str(build), '-t', 'query', output], text=True)
        inputs = []
        reading_inputs = False
        for entry in query.splitlines():
            if entry.startswith('  input:'):
                if required_rule not in entry:
                    raise ValueError(f'Unexpected Ninja producer for {output}: {entry.strip()}')
                reading_inputs = True
            elif entry.startswith('  outputs:'):
                reading_inputs = False
            elif reading_inputs:
                path = entry.strip()
                # Ninja implicit and order-only dependencies are not ar/link
                # operands; in particular they must not make an object direct.
                if not path.startswith(('| ', '|| ', '|@ ')):
                    inputs.append(path)
        if not inputs:
            raise ValueError('No explicit Ninja inputs for ' + output)
        return inputs

    executables = {
        name for name, target in reference.targets.items()
        if target.kind == 'executable' and not cfg.target_excluded(name)
    }
    variables = {}
    name = None
    with (build / 'build.ninja').open() as ninja:
        for line in ninja:
            if line.startswith('build '):
                name = None
                if 'EXECUTABLE_LINKER' not in line:
                    continue
                outputs, sep, _ = line[6:].partition(': ')
                if not sep:
                    raise ValueError('Malformed Ninja executable: ' + line[:240])
                for output in words(outputs):
                    candidate = Path(output).name
                    if candidate in executables:
                        if candidate in variables:
                            raise ValueError('Ambiguous Ninja executable: ' + candidate)
                        name = candidate
                        variables[name] = {'output': output}
            elif name and line.startswith('  '):
                key, sep, value = line.strip().partition(' = ')
                if sep and key in {'FLAGS', 'LINK_FLAGS', 'LINK_LIBRARIES', 'LINK_PATH'}:
                    variables[name][key] = words(value)
            elif not line.startswith('  ') and line.strip():
                name = None
    if executables - variables.keys():
        raise ValueError(f'Executable not found in build.ninja: {sorted(executables - variables.keys())}')

    result = {}
    for name, row in variables.items():
        inputs = explicit_inputs(row['output'], 'EXECUTABLE_LINKER')
        # CMake's Ninja executable rule writes the response file as
        # $in $LINK_PATH $LINK_LIBRARIES, following $FLAGS $LINK_FLAGS.
        operands = (row.get('FLAGS', []) + row.get('LINK_FLAGS', []) + inputs
                    + row.get('LINK_PATH', []) + row.get('LINK_LIBRARIES', []))
        search_dirs = [os.path.normpath(os.path.join(build, arg[2:]))
                       for arg in operands if arg.startswith('-L') and len(arg) > 2]
        direct = set()
        whole_objects = set()
        whole_archives = {}
        whole = False
        for arg in operands:
            if arg in WHOLE_START:
                if whole:
                    raise ValueError(f'{name}: nested Ninja --whole-archive')
                whole = True
            elif arg in WHOLE_END:
                if not whole:
                    raise ValueError(f'{name}: unmatched Ninja --no-whole-archive')
                whole = False
            elif arg in {'--start-lib', '-Wl,--start-lib', '--end-lib', '-Wl,--end-lib'}:
                raise ValueError(f'{name}: unsupported Ninja lazy object group {arg}')
            elif not arg.startswith('-') and arg.endswith('.o'):
                direct.add(object_key(arg))
            elif whole and (arg.endswith('.a') or arg.startswith('-l')):
                if arg.startswith('-l'):
                    filename = arg[3:] if arg.startswith('-l:') else 'lib' + arg[2:] + '.a'
                    candidates = {os.path.join(directory, filename) for directory in search_dirs
                                  if os.path.isfile(os.path.join(directory, filename))}
                    if len(candidates) != 1:
                        raise ValueError(f'{name}: Ninja {arg} has {len(candidates)} static archive candidates')
                    archive = candidates.pop()
                else:
                    archive = os.path.normpath(os.path.join(build, arg))
                archive = os.path.relpath(archive, build)
                if archive.startswith('../'):
                    raise ValueError(f'{name}: Ninja whole archive is outside the build: {archive}')
                if archive not in whole_archives:
                    members = explicit_inputs(archive, 'STATIC_LIBRARY_LINKER')
                    if any(not member.endswith('.o') for member in members):
                        raise ValueError(f'{name}: non-object Ninja archive inputs for {archive}')
                    whole_archives[archive] = {object_key(member) for member in members}
                whole_objects.update(whole_archives[archive])
        if whole:
            raise ValueError(f'{name}: unterminated Ninja --whole-archive')
        result[name] = {'direct': direct, 'whole': whole_objects, 'whole_archives': whole_archives}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--any2bazel', required=True, type=Path)
    parser.add_argument('--cmake-model', required=True, type=Path)
    parser.add_argument('--cmake-build', type=Path, help='Ninja build directory; defaults to the CMake model directory')
    parser.add_argument('--bazel-model', required=True, type=Path)
    parser.add_argument('--native-aquery', required=True, type=Path)
    parser.add_argument('--mri-aquery', type=Path)
    parser.add_argument('--bazel-output-base', type=Path, help='Source directories from the captured aquery')
    parser.add_argument('--config', default=Path(__file__).resolve().parent / 'any2bazel.json', type=Path)
    parser.add_argument('--assembly-aquery', type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    sys.path.insert(0, str(args.any2bazel / 'scripts'))
    from canonicalize import canonicalize_flags
    import config
    from diff import _diff_tu
    from model import TranslationUnit
    from reconstruct import reconstruct
    from serialize import load_model

    reference = load_model(str(args.cmake_model))
    actual = load_model(str(args.bazel_model))
    assert reference.repo_root == actual.repo_root
    cfg = config.load(str(args.config))
    build = (args.cmake_build or args.cmake_model.parent).resolve()
    evidence = ArchiveEvidence(args.native_aquery, args.assembly_aquery, args.mri_aquery,
                               cfg, build, reference.repo_root, args.bazel_output_base)
    link_evidence = {}
    for name, target in reference.targets.items():
        if target.kind == 'executable' and not cfg.target_excluded(name):
            label = cfg.target_map.get(name, ':' + name)
            raw_label = '//' + label if label.startswith(':') else label
            link_evidence[name] = evidence.link(raw_label, name)
    linked_objects = set().union(*(set(row['uses']) for row in link_evidence.values()))
    actual, output_root, other_configuration_actions = target_configuration(actual, cfg, linked_objects)
    root_prefix = reference.repo_root.rstrip('/') + '/'
    source_identities = {'cmake': {}, 'bazel': {}}
    repository_paths = [(source, destination) for source, destination in cfg.include_map
                        if source.startswith(('external/+contrib+', 'external/llvm++llvm+llvm-project'))]
    reference_views = reconstruct(reference)
    module_source_owners = defaultdict(list)
    for name, target in reference_views.items():
        if llvm_targets.target(name) and name not in llvm_targets.NATIVE_RUNTIMES:
            for tu in target.tus:
                identity = cfg.map_source(tu.source)
                if name not in module_source_owners[identity]:
                    module_source_owners[identity].append(name)

    # CMake adds this whitespace-only TU to choose a C++ linker for ICU's data
    # assembly. Bazel knows the linker directly; keep the omission explicit.
    empty_source = build / 'contrib/icu-cmake/empty.cpp'
    empty_contents = empty_source.read_bytes()
    if empty_contents.strip():
        raise ValueError('CMake ICU linker-selection source is no longer empty')
    empty_identity = cfg.map_source(os.path.relpath(empty_source, reference.repo_root))
    omitted_empty_sources = {('_icudata', empty_identity)}

    def flag_path(flag):
        value = flag.replace(root_prefix, '')
        # Relocate operands such as --embed-dir and path-valued definitions;
        # preserve the flag itself and every byte below the repository root.
        for source, destination in repository_paths:
            value = value.replace(source + '/', destination + '/')
        return value

    def owners(label, source):
        if label.startswith('@@llvm++llvm+llvm-project//'):
            # Build tools and fuzzers can still use the LLVM module. Its
            # bootstrap runtimes must not satisfy ClickHouse's runtime groups.
            return module_source_owners.get(cfg.map_source(source), [])
        repository = None
        if not label.startswith(':'):
            prefix = next((
                (('@@' + path.removeprefix('external/') + '//'), destination)
                for path, destination in repository_paths
                if path.startswith('external/+contrib+') and
                label.startswith('@@' + path.removeprefix('external/') + '//')
            ), None)
            if prefix is None:
                return []
            prefix, repository = prefix
            label = label.removeprefix(prefix)
        name = re.sub(r'__(compile|nasm|runtime_objects)_\d+$', '', label[1:])
        if '__runtime_objects_' in label and (repository != llvm_targets.PREFIX or
                                              name not in llvm_targets.NATIVE_RUNTIMES):
            return []
        return [name]

    def normalized(tu, side):
        # The generator makes absolute workspace paths relocatable. Preserve
        # flag/define contents and only remove this exact workspace prefix.
        identity = cfg.map_source(tu.source)
        previous = source_identities[side].setdefault(identity, tu.source)
        if previous != tu.source:
            raise ValueError(f'{side} source mapping collapses distinct files: {previous}, {tu.source}')
        return replace(
            tu,
            source=identity,
            defines=tuple(flag_path(d) for d in tu.defines),
            flags=tuple(flag_path(f) for f in tu.flags),
        )

    by_owner = defaultdict(lambda: defaultdict(list))
    objects = defaultdict(set)
    for label, target in reconstruct(actual).items():
        for tu in target.tus:
            names = owners(label, tu.source)
            if not names:
                # Only reference targets participate in the comparison. In
                # particular, LLVM module runtimes used as Rust symbol inputs
                # cannot stand in for ClickHouse's CMake runtime targets.
                continue
            value = normalized(tu, 'bazel')
            for name in names:
                by_owner[name][value.source].append(value)
        for action in actual.targets[label].actions:
            if action.mnemonic in {'CppCompile', 'ObjcCompile'} and '-c' in action.arguments:
                source = action.arguments[action.arguments.index('-c') + 1]
                for name in owners(label, source):
                    objects[name, cfg.map_source(source)].update(p for p in action.outputs if p.endswith('.o'))

    assembly_actions = 0
    if args.assembly_aquery:
        graph = json.loads(args.assembly_aquery.read_text())
        from extract_bazel import _build_path_index
        artifacts = _build_path_index(graph)
        labels = {t['id']: t['label'] for t in graph.get('targets', [])}
        for action in graph.get('actions', []):
            if action['mnemonic'] not in {'Assemble', 'PreprocessAssemble', 'NasmCompile'}:
                continue
            if not any(artifacts[p].startswith(output_root) or artifacts[p] in linked_objects
                       for p in action['outputIds']):
                continue
            label = labels[action['targetId']]
            argv = expanded_arguments(action)
            source = argv[argv.index('-c') + 1] if '-c' in argv else argv[-1]
            names = owners(label.removeprefix('//'), source)
            if not names:
                continue
            assert source.endswith(('.s', '.S', '.asm')), (label, source)
            defines, includes, flags = canonicalize_flags(argv, reference.repo_root, is_bazel=True)
            value = normalized(TranslationUnit(source, defines, includes, flags, 'ASM'), 'bazel')
            for name in names:
                by_owner[name][value.source].append(value)
                objects[name, value.source].update(artifacts[p] for p in action['outputIds'] if artifacts[p].endswith('.o'))
            assembly_actions += 1

    checked = 0
    missing = []
    differences = []
    variants = Counter()
    for name, target in reference_views.items():
        if cfg.target_excluded(name):
            continue
        for tu in target.tus:
            expected = normalized(tu, 'cmake')
            variants[expected.source] += 1
            if (name, expected.source) in omitted_empty_sources:
                continue
            candidates = by_owner[name].get(expected.source, [])
            if not candidates:
                missing.append({'target': name, 'source': expected.source})
                continue
            checked += 1
            options = [
                [d for d in _diff_tu(name, expected, candidate, cfg) if d.severity == 'error']
                for candidate in candidates
            ]
            differences.extend(min(options, key=len))
    build_includes = BuildIncludeEvidence(build, reference.repo_root, cfg)
    differences = [build_includes.classify(d) for d in differences]
    result = {
        'bazel_output_root': output_root,
        'other_configuration_actions': other_configuration_actions,
        'reference_compile_actions': sum(variants.values()),
        'matched_compile_actions': checked,
        'repeated_source_paths': sum(count > 1 for count in variants.values()),
        'assembly_actions': assembly_actions,
        'config_excluded_targets': sorted(cfg.exclude_targets),
        'missing_actions': missing,
        'omitted_empty_sources': [{'target': name, 'source': source,
                                   'bytes': len(empty_contents),
                                   'sha256': hashlib.sha256(empty_contents).hexdigest()}
                                  for name, source in sorted(omitted_empty_sources)],
        'difference_counts': dict(Counter(d['kind'] for d in differences if d['severity'] == 'error')),
        'informational_difference_counts': dict(Counter(d['kind'] for d in differences if d['severity'] != 'error')),
        'build_include_sources': build_includes.sources,
        'differences': differences,
        'converged': not missing and not any(d['severity'] == 'error' for d in differences),
    }
    ninja_objects = ninja_link_objects(build, reference, cfg)
    result['link_closures'] = audit_links(reference, actual, reference_views, objects, cfg, ninja_objects,
                                          omitted_empty_sources, link_evidence)
    result['converged'] = result['converged'] and all(row['passed'] for row in result['link_closures'].values())
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key not in {'missing_actions', 'differences', 'link_closures'}}))
    print('missing actions:', len(missing))
    print('link closure failures:', sum(not row['passed'] for row in result['link_closures'].values()))
    return 0 if result['converged'] else 1


def audit_links(reference, actual, reference_views, objects, cfg, ninja_objects,
                omitted_empty_sources, link_evidence):
    """Check native object coverage, whole-archive behavior and Rust imports.

    Object occurrences come from the raw link operands and recursively
    captured archive producers, not from File API expectations or depsets.
    """
    from canonicalize import canonicalize_link_flags

    results = {}
    library_kinds = {'static_library', 'object_library', 'interface_library'}
    for name, target in reference.targets.items():
        if target.kind != 'executable' or cfg.target_excluded(name):
            continue
        link = link_evidence[name]
        positions = link['uses']
        mandatory = {path for path, uses in positions.items() if any(required for _, required in uses)}
        direct = ninja_objects[name]['direct']
        whole_objects = ninja_objects[name]['whole']
        direct_owners = {owner for owner, _ in direct}
        whole_targets = {owner for owner, _ in whole_objects}
        ninja_required = direct | whole_objects
        known = {(owner, cfg.map_source(tu.source))
                 for owner in direct_owners | whole_targets for tu in reference_views[owner].tus}
        if ninja_required - known:
            raise ValueError(f'Ninja link inputs missing from reference compiles for {name}: {sorted(ninja_required - known)}')

        closure = set()
        pending = [name, *sorted(direct_owners | whole_targets)]
        required_rust = set()
        required_system = set()
        while pending:
            dependency = pending.pop()
            if dependency in closure:
                continue
            closure.add(dependency)
            for edge in reference.targets[dependency].deps:
                if edge.name in RUST_ARCHIVES:
                    required_rust.add(RUST_ARCHIVES[edge.name])
                elif edge.external and edge.name not in reference.targets:
                    required_system.add(cfg.dep_map.get(edge.name, edge.name))
                if edge.name in reference.targets and reference.targets[edge.name].kind in library_kinds:
                    pending.append(edge.name)
        missing = []
        lazy_required = []
        checked = 0
        mandatory_count = 0
        omitted_empty_objects = []
        for dependency in sorted(closure):
            if cfg.target_excluded(dependency):
                continue
            for tu in reference_views[dependency].tus:
                key = dependency, cfg.map_source(tu.source)
                if key in omitted_empty_sources:
                    omitted_empty_objects.append({'target': dependency, 'source': key[1]})
                    continue
                present = objects[key] & positions.keys()
                checked += 1
                if not present:
                    missing.append({'target': dependency, 'source': key[1]})
                elif key in ninja_required:
                    mandatory_count += 1
                    if not present & mandatory:
                        lazy_required.append({'target': dependency, 'source': key[1]})
        # Presence requires the actual ClickHouseRustSymbols output as a link
        # operand or a verified MRI ADDLIB. An archive name alone proves none
        # of these conditions.
        missing_rust = sorted(required_rust - link['rust'].keys())
        # Cargo builds Skim's bridge and CXX runtime into its staticlib. The
        # native port compiles them separately, so the processed Rust archive
        # alone cannot establish FFI link coverage.
        missing_rust_native = []
        skim_imports = link['rust'].get('skim', [])
        verified_skim_cargo = bool(skim_imports) and all('cargo_native_proof' in row for row in skim_imports)
        if 'skim' in required_rust and not verified_skim_cargo:
            for label in ['bazel/rust:skim_bridge', 'bazel/rust:cxx_runtime']:
                native = actual.targets.get(label)
                compiles = [a for a in native.actions if a.mnemonic == 'CppCompile'] if native else []
                if not compiles:
                    missing_rust_native.append({'target': label, 'reason': 'no native compile actions'})
                for action in compiles:
                    outputs = {path for path in action.outputs if path.endswith('.o')}
                    if not outputs or not outputs & positions.keys():
                        missing_rust_native.append({'target': label, 'outputs': sorted(outputs)})
        system_libraries = {arg[2:] for arg in link['argv'] if arg.startswith('-l')}
        missing_system = sorted(required_system - system_libraries)
        runtime_positions = [pos for (owner, _), paths in objects.items() if owner in whole_targets
                             for p in paths if p in positions for pos, required in positions[p] if required]
        glibc_positions = [pos for (owner, _), paths in objects.items() if owner == 'glibc-compatibility'
                           for p in paths if p in positions for pos, _ in positions[p]]
        runtime_order = not runtime_positions or not glibc_positions or max(runtime_positions) < min(glibc_positions)
        expected_flags = {flag for flag in reference_views[name].link_flags if not cfg.link_flag_ignored(flag)}
        actual_flags = set(canonicalize_link_flags(link['argv'], is_bazel=True))
        # Whole-archive retention is checked against actual mandatory objects
        # above. Every other reference link flag still has to be present.
        equivalent_archive_flags = {'-Wl,--whole-archive', '-Wl,--no-whole-archive'} if whole_targets else set()
        missing_flags = sorted(expected_flags - actual_flags - equivalent_archive_flags)
        results[name] = {
            'ninja_direct_objects': len(direct),
            'ninja_direct_object_targets': sorted(direct_owners),
            'reference_native_objects': checked,
            'required_nonlazy_objects': mandatory_count,
            'whole_archive_targets': sorted(whole_targets),
            'ninja_whole_archives': {archive: len(members) for archive, members
                                     in ninja_objects[name]['whole_archives'].items()},
            'rust_archives': sorted(required_rust),
            'observed_processed_rust_imports': dict(link['rust']),
            'rust_limits': 'Processed import providers and explicit native members only; Rust member byte/flag identity is not checked.',
            'resolved_object_files': len(positions),
            'resolved_object_occurrences': sum(map(len, positions.values())),
            'unresolved_operands': link['unresolved'],
            'custom_assembly': link['custom_assembly'],
            'missing_objects': missing,
            'omitted_empty_objects': omitted_empty_objects,
            'unexpectedly_lazy_objects': lazy_required,
            'missing_rust_archives': missing_rust,
            'missing_rust_native_objects': missing_rust_native,
            'missing_system_libraries': missing_system,
            'missing_link_flags': missing_flags,
            'runtimes_precede_glibc_compatibility': runtime_order,
            'passed': not missing and not lazy_required and not missing_rust and not missing_rust_native and not missing_system and not missing_flags and not link['unresolved'] and runtime_order,
        }
    return results


if __name__ == '__main__':
    sys.exit(main())
