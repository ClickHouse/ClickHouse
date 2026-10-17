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
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
import llvm_targets


def target_configuration(model, cfg):
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
            else:
                excluded[action.mnemonic] += 1
        targets[label] = replace(target, actions=actions)
    return replace(model, targets=targets), root, dict(excluded)


def ninja_direct_objects(build, repo_root):
    """Read actual Ninja link inputs independently of File API dependencies."""
    commands = json.loads((build / 'compile_commands.json').read_text())
    sources = {}
    for command in commands:
        output = os.path.normpath(os.path.join(command['directory'], command['output']))
        source = os.path.normpath(os.path.join(command['directory'], command['file']))
        sources[output] = os.path.relpath(source, repo_root)
    targets = subprocess.check_output(['ninja', '-C', str(build), '-t', 'targets', 'all'], text=True)
    result = {}
    for line in targets.splitlines():
        output, rule = line.rsplit(': ', 1)
        if 'EXECUTABLE_LINKER' not in rule:
            continue
        name = Path(output).name
        if name in result:
            raise ValueError('Ambiguous Ninja executable: ' + name)
        query = subprocess.check_output(['ninja', '-C', str(build), '-t', 'query', output], text=True)
        objects = set()
        reading_inputs = False
        for entry in query.splitlines():
            if entry.startswith('  input:'):
                reading_inputs = True
            elif entry.startswith('  outputs:'):
                reading_inputs = False
            elif reading_inputs and entry.strip().endswith('.o'):
                path = entry.strip().removeprefix('|| ').removeprefix('| ')
                owner = re.search(r'CMakeFiles/([^/]+)\.dir/', path)
                absolute = os.path.normpath(str(build / path))
                if not owner or absolute not in sources:
                    raise ValueError('Unmapped Ninja link object: ' + path)
                objects.add((owner[1], sources[absolute]))
        result[name] = objects
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--any2bazel', required=True, type=Path)
    parser.add_argument('--cmake-model', required=True, type=Path)
    parser.add_argument('--cmake-build', type=Path, help='Ninja build directory; defaults to the CMake model directory')
    parser.add_argument('--bazel-model', required=True, type=Path)
    parser.add_argument('--config', default=Path('any2bazel.json'), type=Path)
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
    actual, output_root, other_configuration_actions = target_configuration(actual, cfg)
    root_prefix = reference.repo_root.rstrip('/') + '/'
    source_identities = {'cmake': {}, 'bazel': {}}
    repository_paths = [(source, destination) for source, destination in cfg.include_map
                        if source.startswith(('external/+contrib+', 'external/llvm++llvm+llvm-project'))]
    reference_views = reconstruct(reference)
    module_source_owners = defaultdict(list)
    for name, target in reference_views.items():
        if llvm_targets.target(name):
            for tu in target.tus:
                identity = cfg.map_source(tu.source)
                if name not in module_source_owners[identity]:
                    module_source_owners[identity].append(name)

    # CMake adds this whitespace-only TU to choose a C++ linker for ICU's data
    # assembly. Bazel knows the linker directly; keep the omission explicit.
    build = (args.cmake_build or args.cmake_model.parent).resolve()
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
            # The module splits and shares LLVM/runtime libraries differently
            # from CMake. Match their exact source identities; audit_links still
            # requires those actual objects in every reference binary closure.
            return module_source_owners.get(cfg.map_source(source), [])
        if not label.startswith(':'):
            prefixes = ['@@' + source.removeprefix('external/') + '//'
                        for source, _ in repository_paths if source.startswith('external/+contrib+')]
            prefix = next((prefix for prefix in prefixes if label.startswith(prefix)), None)
            if prefix is None:
                return []
            label = label.removeprefix(prefix)
        return [re.sub(r'__(compile|nasm)_\d+$', '', label[1:])]

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
            value = normalized(tu, 'bazel')
            for name in owners(label, tu.source):
                by_owner[name][value.source].append(value)
        for action in actual.targets[label].actions:
            if action.mnemonic == 'CppCompile' and '-c' in action.arguments:
                source = action.arguments[action.arguments.index('-c') + 1]
                for name in owners(label, source):
                    objects[name, cfg.map_source(source)].update(p for p in action.outputs if p.endswith('.o'))

    assembly_actions = 0
    if args.assembly_aquery:
        graph = json.loads(args.assembly_aquery.read_text())
        from extract_bazel import _build_path_index
        artifacts = _build_path_index(graph)
        labels = {t['id']: t['label'] for t in graph['targets']}
        for action in graph['actions']:
            if action['mnemonic'] not in {'Assemble', 'PreprocessAssemble', 'NasmCompile'}:
                continue
            if not any(artifacts[p].startswith(output_root) for p in action['outputIds']):
                continue
            label = labels[action['targetId']]
            argv = action['arguments']
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
        'difference_counts': dict(Counter(d.kind for d in differences)),
        'differences': [asdict(d) for d in differences],
        'converged': not missing and not differences,
    }
    direct_objects = ninja_direct_objects(build, reference.repo_root)
    result['link_closures'] = audit_links(reference, actual, reference_views, objects, cfg, direct_objects, omitted_empty_sources)
    result['converged'] = result['converged'] and all(row['passed'] for row in result['link_closures'].values())
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({key: value for key, value in result.items() if key not in {'missing_actions', 'differences', 'link_closures'}}))
    print('missing actions:', len(missing))
    print('link closure failures:', sum(not row['passed'] for row in result['link_closures'].values()))
    return 0 if result['converged'] else 1


def audit_links(reference, actual, reference_views, objects, cfg, direct_objects, omitted_empty_sources):
    """Prove native object coverage, whole-archive behavior and Rust imports.

    hermetic-llvm uses --start-lib groups instead of archive files. Inspect
    their object members and whether each member is lazy, rather than treating
    the spelling of --whole-archive as proof of retention.
    """
    from canonicalize import canonicalize_link_flags

    rust_archives = {
        '_ch_rust_pco': 'pco',
        '_ch_rust_prql': 'prql',
        '_ch_rust_polyglot': 'polyglot',
        '_ch_rust_skim_rust': 'skim',
        '_ch_rust_wasmtime': 'wasmtime',
        'delta_kernel_ffi': 'delta_kernel_ffi',
        'chdig': 'chdig',
    }
    results = {}
    library_kinds = {'static_library', 'object_library', 'interface_library'}
    for name, target in reference.targets.items():
        if target.kind != 'executable' or cfg.target_excluded(name):
            continue
        bazel_name = cfg.target_map.get(name, ':' + name)
        candidates = [a for a in actual.targets[bazel_name].actions
                      if a.mnemonic == 'CppLink' and any(p.endswith('/' + name) for p in a.outputs)]
        assert len(candidates) == 1, (name, len(candidates))
        link = candidates[0]
        direct = {(owner, cfg.map_source(source)) for owner, source in direct_objects[name]}
        direct_owners = {owner for owner, _ in direct}
        known = {(owner, cfg.map_source(tu.source)) for owner in direct_owners for tu in reference_views[owner].tus}
        if direct - known:
            raise ValueError(f'Ninja link inputs missing from reference compiles for {name}: {sorted(direct - known)}')
        positions = {}
        mandatory = set()
        lazy = False
        for index, arg in enumerate(link.arguments):
            if arg == '-Wl,--start-lib':
                lazy = True
            elif arg == '-Wl,--end-lib':
                lazy = False
            elif arg.endswith('.o'):
                positions[arg] = index
                if not lazy:
                    mandatory.add(arg)

        whole_targets = set()
        whole = False
        for action in target.actions:
            if action.mnemonic != 'CppLink':
                continue
            for arg in action.arguments:
                if arg == '-Wl,--whole-archive':
                    whole = True
                elif arg == '-Wl,--no-whole-archive':
                    whole = False
                elif whole and (arg.endswith('.a') or arg.startswith('-l')):
                    library = arg[2:] if arg.startswith('-l') else Path(arg).name.removeprefix('lib').removesuffix('.a')
                    assert library in reference.targets, (name, library)
                    whole_targets.add(library)

        closure = set()
        pending = [name, *sorted(direct_owners)]
        required_rust = set()
        required_system = set()
        while pending:
            dependency = pending.pop()
            if dependency in closure:
                continue
            closure.add(dependency)
            for edge in reference.targets[dependency].deps:
                if edge.name in rust_archives:
                    required_rust.add(rust_archives[edge.name])
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
            must_retain = dependency == name or dependency in whole_targets or reference.targets[dependency].kind == 'object_library'
            for tu in reference_views[dependency].tus:
                key = dependency, cfg.map_source(tu.source)
                if key in omitted_empty_sources:
                    omitted_empty_objects.append({'target': dependency, 'source': key[1]})
                    continue
                present = objects[key] & positions.keys()
                checked += 1
                if not present:
                    missing.append({'target': dependency, 'source': key[1]})
                elif must_retain or key in direct:
                    mandatory_count += 1
                    if not present & mandatory:
                        lazy_required.append({'target': dependency, 'source': key[1]})
        # A declared action input need not be passed to the linker. Require the
        # archive on the actual command line, just as for native object files.
        linked_archives = {Path(p).name for p in link.arguments if p.endswith('.a')}
        missing_rust = sorted(n for n in required_rust if 'lib' + n + '_processed.a' not in linked_archives)
        # Cargo builds Skim's bridge and CXX runtime into its staticlib. The
        # native port compiles them separately, so the processed Rust archive
        # alone cannot establish FFI link coverage.
        missing_rust_native = []
        if 'skim' in required_rust:
            for label in ['bazel/rust:skim_bridge', 'bazel/rust:cxx_runtime']:
                native = actual.targets.get(label)
                compiles = [a for a in native.actions if a.mnemonic == 'CppCompile'] if native else []
                if not compiles:
                    missing_rust_native.append({'target': label, 'reason': 'no native compile actions'})
                for action in compiles:
                    outputs = {path for path in action.outputs if path.endswith('.o')}
                    if not outputs or not outputs & positions.keys():
                        missing_rust_native.append({'target': label, 'outputs': sorted(outputs)})
        system_libraries = {arg[2:] for arg in link.arguments if arg.startswith('-l')}
        missing_system = sorted(required_system - system_libraries)
        runtime_positions = [positions[p] for (owner, _), paths in objects.items() if owner in whole_targets for p in paths if p in positions]
        glibc_positions = [positions[p] for (owner, _), paths in objects.items() if owner == 'glibc-compatibility' for p in paths if p in positions]
        runtime_order = not runtime_positions or not glibc_positions or max(runtime_positions) < min(glibc_positions)
        expected_flags = {flag for flag in reference_views[name].link_flags if not cfg.link_flag_ignored(flag)}
        actual_flags = set(canonicalize_link_flags(list(link.arguments), is_bazel=True))
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
            'rust_archives': sorted(required_rust),
            'missing_objects': missing,
            'omitted_empty_objects': omitted_empty_objects,
            'unexpectedly_lazy_objects': lazy_required,
            'missing_rust_archives': missing_rust,
            'missing_rust_native_objects': missing_rust_native,
            'missing_system_libraries': missing_system,
            'missing_link_flags': missing_flags,
            'runtimes_precede_glibc_compatibility': runtime_order,
            'passed': not missing and not lazy_required and not missing_rust and not missing_rust_native and not missing_system and not missing_flags and runtime_order,
        }
    return results


if __name__ == '__main__':
    sys.exit(main())
