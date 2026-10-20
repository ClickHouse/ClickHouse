#!/usr/bin/env python3
"""Remove captured first-party C/C++ source compiler inputs that no TU includes.

Never treats a translation unit in srcs as available to sibling compiles.
A usable incoming edge is required for every retained textual implementation.
Opaque source expressions, generated/foreign TUs, forced or computed includes,
and unknown language modes make removal ineligible. No LLVM or sysroot file is
a candidate or inferred to be unused from an incomplete repository inventory.

Preview by default; --write applies only after input-list verification.
Source directories and BUILD paths are read
fresh so moving base/base into base does not hard-code old Bazel labels.
"""
import argparse
import ast
from collections import Counter, defaultdict
from copy import deepcopy
from pathlib import Path, PurePosixPath
import json
import posixpath
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from initial_migration.compact_build import ast_key
from initial_migration.first_party_packages import Build, native_directory
from initial_migration.repository_inputs import REPOSITORIES, ROOT
from initial_migration.scan_include_adjacency import classify, includes

CODE = {'.c', '.cc', '.cpp', '.cxx', '.c++', '.C'}
CC = {'ch_cc_library', 'cc_library', 'cc_binary', 'cc_test'}
CPUS = ('x86_64', 'aarch64', 'default')
LOCATION = re.compile(r'\$\((?:locations?|execpaths?|rootpaths?)\s+([^\s)]+)\)')
FORCED = {'-include', '-imacros', '-include-pch', '-include-pth', '-ivfsoverlay'}

def named(call):
    return getattr(call.func, 'id', '') if isinstance(call, ast.Call) else ''

def attrs(call):
    return {kw.arg: kw.value for kw in call.keywords}

def literal(node):
    return node.value if isinstance(node, ast.Constant) else None

def main_label(pkg, value):
    if not isinstance(value, str):
        return None
    if value.startswith('@clickhouse//'):
        value = value.removeprefix('@clickhouse')
    if value.startswith('@'):
        return None
    if value.startswith('//'):
        package, sep, path = value.removeprefix('//').partition(':')
        if not sep:
            return None
    else:
        package, path = pkg, value.removeprefix(':')
    result = posixpath.normpath(posixpath.join(package, path))
    return None if result.startswith(('../', '/')) else result

def inspect_root(root):
    listed = subprocess.check_output(['git', 'ls-files', '-s', '-z'], cwd=root)
    files = set()
    for record in listed.split(b'\0'):
        if not record:
            continue
        meta, filename = record.split(b'\t', 1)
        mode, sha, stage = meta.decode().split()
        name = filename.decode()
        if mode not in {'100644','100755'} or stage != '0':
            continue
        if any(name == owner or name.startswith(owner + '/') for owner in REPOSITORIES):
            continue
        files.add(name)
    missing = {name for name in files if not (root / name).is_file()}
    # Missing BUILD files are expected during a move. Any missing source or
    # header is not proof that it never includes a captured translation unit.
    missing_scannable = {name for name in missing if classify(name) != 'unrecognized_suffix'
                         and PurePosixPath(name).name not in {'BUILD', 'BUILD.bazel'}}
    return frozenset(files - missing), sorted(missing_scannable)

def expanded_files(build, node, cpu, chain=()):
    """Select exact source labels; None is opaque, not the empty set."""
    if node is None:
        return []
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return [node.value]
    if isinstance(node, ast.Name) and node.id in build.constants and node.id not in chain:
        return expanded_files(build, build.constants[node.id], cpu, (*chain, node.id))
    if isinstance(node, (ast.List, ast.Tuple)):
        all_values = [expanded_files(build, elt, cpu, chain) for elt in node.elts]
        return None if any(v is None for v in all_values) else [v for lst in all_values for v in lst]
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        a, b = expanded_files(build, node.left, cpu, chain), expanded_files(build, node.right, cpu, chain)
        return None if a is None or b is None else a + b
    if named(node) == 'glob':
        try:
            return ['//' + build.package + ':' + posixpath.relpath(file, build.package or '.')
                    for file in build.glob_values(node)]
        except (TypeError, ValueError):
            return None
    if named(node) == 'select' and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
        table = {literal(key): val for key, val in zip(node.args[0].keys, node.args[0].values)}
        valid = {'@platforms//cpu:x86_64', '@platforms//cpu:aarch64', '//conditions:default'}
        if set(table) - valid:
            return None
        key = '@platforms//cpu:' + cpu
        if key in table:
            return expanded_files(build, table[key], cpu, chain)
        if '//conditions:default' in table:
            return expanded_files(build, table['//conditions:default'], cpu, chain)
        return []  # no matching select means there is no compile in this config
    return None

def flags_hold(build, expr):
    """Find forced inputs including :=location flags and unrecognized copts."""
    if expr is None:
        return []
    out = []
    expanded = build.expand(expr)
    for value in ast.walk(expanded):
        text = literal(value)
        if isinstance(text, str):
            if any(text.startswith(opt) for opt in FORCED) or LOCATION.search(text):
                out.append(text)
    return out

def options_are_supported(build, opts):
    for node in ast.walk(build.expand(opts)) if opts is not None else ():
        if isinstance(node, ast.Call) and named(node) not in {
            'select', 'repository_path', 'repository_local_include', 'llvm_toolchain_path', 'cmake_prefix_maps',
        }:
            return False
    return True

def literal_nodes(build, expr):
    if expr is None:
        return []
    expanded = build.expand(expr)
    return [node for node in ast.walk(expanded) if isinstance(node, ast.Constant)
            and isinstance(node.value, str)]

def role(path):
    return 'tu' if PurePosixPath(path).suffix in CODE else 'header_or_template'

def scan_main(root, files):
    incoming = defaultdict(list)
    unresolved = defaultdict(list)
    all_cpp_basenames = {PurePosixPath(path).name for path in files if PurePosixPath(path).suffix in CODE}
    seen_files = set()
    errors = []
    for path in sorted(files):
        if classify(path) == 'unrecognized_suffix':
            continue
        raw = (root / path).read_bytes()
        facts = includes(raw, path)
        if facts is None:
            errors.append(path)
            continue
        seen_files.add(path)
        for line, kind, syntax, operand in facts:
            if syntax == 'computed':
                unresolved[path].append([line, kind, operand])
                continue
            base = posixpath.basename(posixpath.normpath(operand))
            if base in all_cpp_basenames:
                incoming[base].append([path, line, kind, syntax, operand])
    return incoming, unresolved, seen_files, errors

def rewrite_field(build, node, remove, chain=()):
    """Filter supported input lists only; globs/generated macros stay intact."""
    if isinstance(node, ast.Name) and node.id in build.constants and node.id not in chain:
        return rewrite_field(build, build.constants[node.id], remove, (*chain, node.id))
    if isinstance(node, (ast.List, ast.Tuple)):
        kept = [deepcopy(x) for x in node.elts if main_label(build.package, literal(x)) not in remove]
        return ast.List(elts=kept, ctx=ast.Load())
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        lhs = rewrite_field(build, node.left, remove, chain)
        rhs = rewrite_field(build, node.right, remove, chain)
        if isinstance(lhs, ast.List) and not lhs.elts:
            return rhs
        if isinstance(rhs, ast.List) and not rhs.elts:
            return lhs
        return ast.BinOp(left=lhs, op=ast.Add(), right=rhs)
    if named(node) == 'select' and len(node.args) == 1 and isinstance(node.args[0], ast.Dict):
        result = deepcopy(node)
        result.args[0].values = [rewrite_field(build, branch, remove, chain)
                                 for branch in node.args[0].values]
        return result
    return deepcopy(node)


def same_expression(node, text):
    return node is not None and ast_key(node) == ast_key(ast.parse(text, mode='eval').body)


def native_source_copies(root, files):
    """Map tracked C/C++ inputs only when _embedded_sources copies each byte."""
    tree = ast.parse((root / 'bazel/rules/codegen_actions.bzl').read_text())
    bodies = [stmt for stmt in tree.body if isinstance(stmt, ast.FunctionDef)
              and stmt.name == '_embedded_sources_impl']
    if len(bodies) != 1:
        return {}
    loops = [node for node in bodies[0].body if isinstance(node, ast.For)]
    if (len(loops) != 1 or len(loops[0].body) != 1 or loops[0].orelse
            or not same_expression(loops[0].iter, 'zip(ctx.files.srcs, ctx.outputs.outs)')
            or ast_key(loops[0].target) != ast_key(ast.parse(
                'for source, output in []:\n    pass').body[0].target)):
        return {}
    stmt = loops[0].body[0]
    if (not isinstance(stmt, ast.Expr) or not isinstance(stmt.value, ast.Call)
            or not same_expression(stmt.value.func, 'ctx.actions.run_shell')):
        return {}
    options = attrs(stmt.value)
    exact = {
        'command': repr('cp "$1" "$2"'),
        'inputs': '[source]',
        'outputs': '[output]',
        'arguments': '[source.path, output.path]',
    }
    if any(not same_expression(options.get(name), value) for name, value in exact.items()):
        return {}
    copies = {}
    tree = ast.parse((root / 'bazel/rules/codegen_rules.bzl').read_text())
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        fields = {key.value: value for key, value in zip(node.keys, node.values)
                  if isinstance(key, ast.Constant)}
        if literal(fields.get('kind')) != 'embedded_sources':
            continue
        try:
            sources = ast.literal_eval(fields['srcs'])
            outputs = ast.literal_eval(fields['outs'])
        except (KeyError, TypeError, ValueError):
            continue
        if len(sources) != len(outputs):
            raise ValueError('embedded_sources input/output counts differ')
        for src, out in zip(sources, outputs):
            source, output = main_label('bazel', src), main_label('bazel', out)
            if source not in files or PurePosixPath(source).suffix not in CODE:
                continue
            if output is None or output in files or output in copies:
                raise ValueError('Source copy overlaps existing file: ' + str(output))
            copies[output] = source
    return copies


def input_free_substitution(expr):
    """A literal or json.encode result cannot add an include to the template."""
    if isinstance(expr, ast.Constant) and isinstance(expr.value, str):
        return includes(expr.value.encode(), 'build_options substitution') == []
    return isinstance(expr, ast.Call) and same_expression(expr.func, 'json.encode')


def native_build_options_templates(root, files):
    """Read both build_options templates only when substitutions add no inputs."""
    source = ast.parse((root / 'bazel/rules/build_options.bzl').read_text())
    functions = {stmt.name: stmt for stmt in source.body if isinstance(stmt, ast.FunctionDef)}
    implementation = functions.get('_build_options_impl')
    substitutions = functions.get('_bazel_substitutions')
    if implementation is None or substitutions is None:
        return {}
    actions = [n for n in ast.walk(implementation) if isinstance(n, ast.Call)
               and same_expression(n.func, 'ctx.actions.expand_template')]
    if len(actions) != 1:
        return {}
    options = attrs(actions[0])
    expected = {
        'template': 'ctx.file.template',
        'output': 'ctx.outputs.out',
        'substitutions': 'ctx.attr.cmake_substitutions if ctx.attr.cmake_metadata else _bazel_substitutions(ctx)',
    }
    if any(not same_expression(options.get(key), value) for key, value in expected.items()):
        return {}
    initial = [stmt.value for stmt in substitutions.body if isinstance(stmt, ast.Assign)
               and any(isinstance(t, ast.Name) and t.id == 'substitutions' for t in stmt.targets)]
    if (len(initial) != 1 or not isinstance(initial[0], ast.Dict)
            or not all(input_free_substitution(value) for value in initial[0].values)):
        return {}
    for stmt in ast.walk(substitutions):
        if isinstance(stmt, ast.Call) and isinstance(stmt.func, ast.Attribute):
            # Unknown dictionary mutation cannot establish an input-free file.
            if same_expression(stmt.func.value, 'substitutions'):
                return {}
        if not isinstance(stmt, (ast.Assign, ast.AugAssign, ast.AnnAssign)):
            continue
        targets = stmt.targets if isinstance(stmt, ast.Assign) else [stmt.target]
        for target in targets:
            if isinstance(target, ast.Subscript) and same_expression(target.value, 'substitutions'):
                if not isinstance(stmt, ast.Assign) or not input_free_substitution(stmt.value):
                    return {}
    build = Build(root, root / 'bazel/BUILD.bazel', files, {'bazel'})
    rules = [rule for _, rule in build.source.rules() if named(rule) == 'build_options']
    if len(rules) != 1:
        return {}
    options = attrs(rules[0])
    for field, variable, inner in (
            ('template', 'BUILD_OPTIONS', 'template'),
            ('cmake_substitutions', 'CMAKE_METADATA', 'build_options')):
        expression = 'select({' + ', '.join(
            repr('@platforms//cpu:' + cpu) + ': ' + variable + '[' + repr(cpu) + '][' + repr(inner) + ']'
            for cpu in CPUS[:2]) + '})'
        if not same_expression(options.get(field), expression):
            return {}
    output = main_label('bazel', literal(options.get('out')))
    if output is None or output in files:
        return {}
    metadata = ast.parse((root / 'bazel/initial_migration/cmake_metadata.bzl').read_text())
    recorded = [s.value for s in metadata.body if isinstance(s, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == 'CMAKE_METADATA' for t in s.targets)]
    data = ast.parse((root / 'bazel/rules/build_options_data.bzl').read_text())
    settings = [s.value for s in data.body if isinstance(s, ast.Assign) and any(
        isinstance(t, ast.Name) and t.id == 'BUILD_OPTIONS' for t in s.targets)]
    if len(recorded) != 1 or len(settings) != 1 or not isinstance(settings[0], ast.Dict):
        return {}
    try:
        cmake = ast.literal_eval(recorded[0])
        templates = {}
        for key, fields in zip(settings[0].keys, settings[0].values):
            if not isinstance(fields, ast.Dict):
                return {}
            cpu = ast.literal_eval(key)
            fields = {ast.literal_eval(k): v for k, v in zip(fields.keys, fields.values)}
            template = 'bazel/' + ast.literal_eval(fields['template'])
            if (template not in files or includes((root / template).read_bytes(), template) != []
                    or not all(isinstance(value, str)
                               and includes(value.encode(), 'CMake build_options substitution') == []
                               for value in cmake[cpu]['build_options'].values())):
                return {}
            templates[cpu] = template
    except (KeyError, TypeError, ValueError):
        return {}
    return {output: templates} if set(templates) == set(CPUS[:2]) else {}


def generated_sources_allowed(root):
    """Do not reason about generated TUs if fixed flags can force header inputs."""
    forced = re.compile(
        r'(^|[\s"=\x27])(?:-include|-imacros|-ivfsoverlay|-fmodule-file|-fmodule-map-file)')
    return not any(forced.search(line)
                   for path in ('.bazelrc', 'bazel/rules/ch_cc_library.bzl', 'bazel/rules/repository_paths.bzl')
                   for line in (root / path).read_text().splitlines())


def run(root, output=None, write=False):
    files, missing = inspect_root(root)
    copies = native_source_copies(root, files) if generated_sources_allowed(root) else {}
    templates = native_build_options_templates(root, files) if generated_sources_allowed(root) else {}
    paths = sorted(p for p in root.rglob('BUILD.bazel')
                   if native_directory(p.parent.relative_to(root).as_posix()))
    packages = {p.parent.relative_to(root).as_posix() for p in root.rglob('BUILD.bazel')}
    incoming, unresolved, scanned, unknown = scan_main(root, files)
    code_by_base = defaultdict(set)
    for file in files:
        if PurePosixPath(file).suffix in CODE:
            code_by_base[PurePosixPath(file).name].add(file)
    counts = Counter()
    examples = defaultdict(list)
    stats = {'counts': counts, 'reasons': Counter(), 'witnesses': {}, 'access_common': [],
             'missing': missing, 'opaque_sources': Counter(), 'examples': examples,
             'changed_paths': [], 'native_source_copies': copies, 'build_options_templates': templates}
    for path in paths:
        if not path.exists():
            continue
        build = Build(root, path, files, packages)
        edits = []
        snapshots = {}
        original_inputs = {}
        for statement, rule in build.source.rules():
            if named(rule) not in CC:
                continue
            at = attrs(rule)
            field = at.get('additional_compiler_inputs')
            if field is None:
                continue
            counts['rules_with_inputs'] += 1
            name = literal(at['name'])
            key = f'//{build.package}:{name}'
            snapshots[name] = {kw.arg: ast_key(kw.value) for kw in rule.keywords
                               if kw.arg != 'additional_compiler_inputs'}
            nodes = literal_nodes(build, field)
            choices = {candidate: node.value for node in nodes
                       for candidate in [main_label(build.package, node.value)]
                       if candidate in files and PurePosixPath(candidate).suffix in CODE}
            if not choices:
                continue
            counts['rules_with_main_source_inputs'] += 1
            hold_all = []
            language = literal(at.get('language')) if 'language' in at else 'CXX'
            if language not in {'CXX', 'C'}:
                hold_all.append('unsupported_language')
            if missing:
                hold_all.append('missing_native_source_or_template')
            if flags_hold(build, at.get('copts')) or not options_are_supported(build, at.get('copts')):
                hold_all.append('forced_or_opaque_compiler_options')
            original_inputs[name] = {}
            for cpu in CPUS:
                values = expanded_files(build, field, cpu)
                original_inputs[name][cpu] = values
                if values is None:
                    hold_all.append('opaque_additional_inputs')
            active = set()
            for cpu in CPUS:
                raw_srcs = expanded_files(build, at.get('srcs'), cpu)
                if raw_srcs is None:
                    hold_all.append('opaque_srcs')
                    continue
                for value in raw_srcs:
                    code_file = main_label(build.package, value)
                    code_file = copies.get(code_file, code_file)
                    if code_file in templates:
                        if cpu not in templates[code_file]:
                            # Both verified selects in //bazel:build_options
                            # reject this CPU, so the dependent compile cannot run.
                            continue
                        code_file = templates[code_file][cpu]
                    if PurePosixPath(value).suffix in CODE:
                        if code_file not in files or code_file not in scanned:
                            hold_all.append('generated_or_foreign_source')
                            stats['opaque_sources'][value] += 1
                        else:
                            active.add(code_file)
                    elif code_file not in files:
                        # Any rule reference in srcs can produce C++ files.
                        hold_all.append('unknown_rule_src')
                        stats['opaque_sources'][value] += 1
            # All literal textual-code edges from any native header/template
            # are roots. Follow nested CPP inclusions to a fixed point through
            # the entire tracked main inventory, not only the input list being
            # edited. This deliberately over-retains ambiguous same basenames.
            reached = set(active)
            required_witnesses = {}
            pending_bases = set(incoming)
            while True:
                additions = set()
                for base in pending_bases:
                    hits = incoming[base]
                    protected = [h for h in hits if h[0] in reached or role(h[0]) != 'tu']
                    if protected:
                        additions.add(base)
                        required_witnesses[base] = protected
                        reached.update(code_by_base[base])
                if not additions:
                    break
                pending_bases.difference_update(additions)
            for src in reached:
                if unresolved.get(src):
                    hold_all.append('computed_include_in_selected_or_textual_source')
                    for line, kind, expr in unresolved[src]:
                        record = f'{src}:{line} #{kind} {expr}'
                        stats['opaque_sources'][record] += 1
            remove = set()
            for candidate, original in choices.items():
                base = PurePosixPath(candidate).name
                hits = incoming.get(base, ())
                counts['main_source_inputs'] += 1
                reason = None
                # A same-name hit in a public/template header might reach any TU;
                # preserve on ambiguity even if the actual directory differs.
                protected = required_witnesses.get(base, ())
                if hold_all:
                    reason = sorted(set(hold_all))[0]
                elif candidate not in scanned or candidate in unknown:
                    reason = 'unscanned_candidate'
                elif protected:
                    reason = 'header_or_selected_tu_witness'
                    stats['witnesses'][candidate] = protected[:8]
                else:
                    reason = 'only_other_tus_include_or_uncited'
                    remove.add(candidate)
                stats['reasons'][reason] += 1
                if len(examples[reason]) < 15:
                    examples[reason].append([key, candidate, hits[:3]])
                if build.package == 'src/Access/Common':
                    stats['access_common'].append([candidate, reason, hits])
            for cpu in CPUS:
                previous = original_inputs[name][cpu]
                if previous is not None:
                    original_inputs[name][cpu] = [
                        value for value in previous
                        if main_label(build.package, value) not in remove
                    ]
            counts['removable_main_source_inputs'] += len(remove)
            if not remove:
                continue
            after = rewrite_field(build, field, remove)
            if isinstance(after, ast.List) and not after.elts:
                attr = next(kw for kw in rule.keywords if kw.arg == 'additional_compiler_inputs')
                start, end = build.source.span(attr)
                if build.source.bytes[end:end+1] == b',':
                    end += 1
                edits.append((start, end, ''))
            else:
                edits.append((*build.source.span(field), ast.unparse(ast.fix_missing_locations(after))))
        if not edits:
            continue
        after = build.source.rewrite(edits)
        checked = Build(root, path, files, packages, after)
        for _, call in checked.source.rules():
            if named(call) in CC and (name := literal(attrs(call).get('name'))) in snapshots:
                got = {kw.arg: ast_key(kw.value) for kw in call.keywords
                       if kw.arg != 'additional_compiler_inputs'}
                if name in original_inputs:
                    for cpu in CPUS:
                        current = expanded_files(checked, attrs(call).get('additional_compiler_inputs'), cpu)
                        if current != original_inputs[name][cpu]:
                            raise ValueError(f'{path} {name} {cpu}: changed inputs outside proven removal set')
                if got != snapshots[name]:
                    raise ValueError(f'{path} {name} changed attrs besides additional_compiler_inputs')
        stats['changed_paths'].append(str(path.relative_to(root)))
        if write:
            formatted = subprocess.run([
                'buildifier', '-config=' + str(root / '.buildifier.json'),
                '-type=build', '-mode=fix', '-lint=fix', '-warnings=load',
            ], input=after, capture_output=True, text=True, check=True, cwd=root).stdout
            # buildifier may clean loads, but must retain the same C++ rules.
            formatted_build = Build(root, path, files, packages, formatted)
            for _, call in formatted_build.source.rules():
                name = literal(attrs(call).get('name'))
                if named(call) in CC and name in original_inputs:
                    for cpu in CPUS:
                        got = expanded_files(formatted_build, attrs(call).get('additional_compiler_inputs'), cpu)
                        if got != original_inputs[name][cpu]:
                            raise ValueError(f'{path} {name} {cpu}: buildifier changed compiler inputs')
            path.write_text(formatted)
        if output:
            destination = output / 'preview' / path.relative_to(root)
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text(after)
    if output:
        output.mkdir(parents=True, exist_ok=True)
        (output / 'audit.json').write_text(json.dumps(stats, indent=2, sort_keys=True))
    print('BUILD files', len(paths))
    print('COUNTS', dict(counts))
    print('REASONS', dict(stats['reasons']))
    print('MISSING', len(missing))
    print('CHANGED', len(stats['changed_paths']))
    print('ACCESS_COMMON', Counter(row[1] for row in stats['access_common']))
    return stats

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    if args.output and (args.output.resolve() == args.root.resolve() or args.root.resolve() in args.output.resolve().parents):
        parser.error('--output must be outside the repo')
    run(args.root, args.output, write=args.write)
