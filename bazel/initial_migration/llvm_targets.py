"""Build ClickHouse's LLVM libraries from its CMake graph and pinned sources."""

import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
from initial_migration.reference_readers import read_file_api, ninja_edges

PREFIX = 'contrib/llvm-project'
RAW_REPOSITORY = 'clickhouse_llvm_project'
TOOLCHAIN_ROOT = 'external/llvm++llvm+llvm-project'
NATIVE_RUNTIMES = {
    'cxx', 'cxxabi', 'unwind', 'clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc',
}
RUNTIME_ROOT_SOURCES = {
    'contrib/libunwind-cmake/unwind-override.c',
    'contrib/compiler-rt-cmake/xray_reject_modes.cpp',
    'contrib/libllvmlibc-cmake/string/mem_functions.cpp',
    'contrib/libllvmlibc-cmake/string/memmem.cpp',
}
RUNTIME_ROOT_INPUTS = RUNTIME_ROOT_SOURCES | {
    'contrib/libllvmlibc-cmake/string/x86_64_mem_functions.cpp',
    'contrib/libllvmlibc-cmake/string/aarch64_mem_functions.cpp',
    'contrib/libllvmlibc-cmake/string/x86_cpu_dispatch.h',
}

# The toolchain supplies these headers in its own search directories. Importing
# another copy breaks libc++'s include_next chain before it reaches libc headers.
TOOLCHAIN_INCLUDE_DIRECTORIES = {
    PREFIX + '/libcxx/include',
    PREFIX + '/libcxx/include/c++/v1',
    PREFIX + '/libcxxabi/include',
}

def include_mappings(repository):
    """Canonical header namespaces provided by the module's CcInfo targets."""
    generated = 'bazel-out/k8-opt/bin/' + repository + '/'
    mappings = {
        generated + path: PREFIX + '/' + path
        for path in ['llvm/include', 'libcxx/include', 'libcxxabi/include',
                     'libunwind/include', 'compiler-rt/include', 'libc']
    }
    mappings.update({
        generated + 'libc/_virtual_includes/libc_headers': PREFIX + '/libc',
        generated + 'libcxx/libcxx_headers_include_search_directory': PREFIX + '/libcxx/include',
        generated + 'libcxxabi/libcxxabi_headers_include_search_directory': PREFIX + '/libcxxabi/include',
        generated + 'third-party/siphash/_virtual_includes/siphash': PREFIX + '/third-party/siphash/include',
    })
    for arch in ['x86_64', 'aarch64']:
        build = 'build_bazel_reference_' + arch + '/' + PREFIX + '/'
        mappings[build + 'libcxx/include/c++/v1'] = PREFIX + '/libcxx/include'
        mappings[build + 'llvm/include'] = PREFIX + '/llvm/include'
    return [{'from': source, 'to': destination} for source, destination in sorted(mappings.items())]


def update_any2bazel_mappings(parity, targets):
    """Record each embedded LLVM source from the same CMake compile groups."""
    from initial_migration.repository_inputs import comparison_root

    raw_root = comparison_root(PREFIX)
    expected_root = 'external/+contrib+' + RAW_REPOSITORY
    if raw_root != expected_root:
        raise ValueError('Embedded LLVM repository has not been registered: ' + raw_root)

    # map_source() in any2bazel drops the remainder after the matched prefix.
    # Each from path must name a complete source file, never an LLVM directory.
    sources = {}
    arches = set()
    for name, variants in targets.items():
        for arch, value in variants.items():
            arches.add(arch)
            for group in value.get('compileGroups', []):
                for index in group['sourceIndexes']:
                    source = value['sources'][index]['path']
                    if os.path.normpath(source) != source:
                        raise ValueError(f'{arch} {name}: invalid embedded LLVM source {source}')
                    generated = 'build_bazel_reference_' + arch + '/contrib/compiler-rt-cmake/outline_atomic_helpers.dir/'
                    if source.startswith(PREFIX + '/') and (name in NATIVE_RUNTIMES or source.startswith(PREFIX + '/llvm/')):
                        actual = raw_root + source.removeprefix(PREFIX)
                    elif name in NATIVE_RUNTIMES and source in RUNTIME_ROOT_SOURCES:
                        # Real ClickHouse repository files already retain their
                        # source identity in Bazel. Do not remap them to LLVM.
                        continue
                    elif name == 'clang_rt_builtins' and source.startswith(generated):
                        actual = ('bazel-out/k8-opt/bin/' + raw_root + '/gen/' + arch
                                  + '/compiler-rt-outline/' + Path(source).name)
                    else:
                        raise ValueError(f'{arch} {name}: invalid embedded LLVM source {source}')
                    previous = sources.setdefault(actual, source)
                    if previous != source:
                        raise ValueError(f'LLVM source identity collision: {actual}: {previous}, {source}')

    includes = {raw_root: PREFIX}
    for arch in sorted(arches):
        generated = 'bazel-out/k8-opt/bin/' + raw_root + '/gen/' + arch
        # Existing CMake mappings distinguish target-local generated headers
        # from source headers, except the shared LLVM include namespace.
        includes[generated] = '@build/' + arch + '/' + PREFIX
        includes[generated + '/llvm/include'] = PREFIX + '/llvm/include'
        # Match include_mappings() for CMake's copy of __config_site and
        # __assertion_handler. declare_from_references() verifies both copies.
        includes[generated + '/libcxx/include/c++/v1'] = PREFIX + '/libcxx/include'

    for field, added in (('source_map', sources), ('include_map', includes)):
        rows = parity['ignore'][field]
        existing = {}
        for row in rows:
            previous = existing.setdefault(row['from'], row['to'])
            if previous != row['to']:
                raise ValueError(f'{field} collision for {row["from"]}: {previous}, {row["to"]}')
        for source, destination in added.items():
            previous = existing.get(source)
            if previous is not None and previous != destination:
                raise ValueError(f'{field} collision for {source}: {previous}, {destination}')
        # Replace prior entries owned by this generator, preserving all
        # toolchain mappings for libc++, compiler-rt and build-time TableGen.
        if field == 'source_map':
            owned = (raw_root + '/', 'bazel-out/k8-opt/bin/' + raw_root + '/gen/')
        else:
            owned = (raw_root, 'bazel-out/k8-opt/bin/' + raw_root + '/gen/')
        rows[:] = [row for row in rows if not row['from'].startswith(owned)]
        rows.extend({'from': source, 'to': destination}
                    for source, destination in sorted(added.items()))

def generated_target(path):
    if path.startswith(PREFIX + '/') and not path.startswith(PREFIX + '/llvm/'):
        return '@llvm-project//llvm'
    if path.startswith('contrib/compiler-rt-cmake/outline_atomic_helpers.dir/'):
        return '@llvm-project//compiler-rt:builtins'
    return None


def target(name):
    return {
        'llvm-tblgen': '@llvm-project//llvm:llvm-tblgen',
        'llvm-min-tblgen': '@llvm-project//llvm:llvm-min-tblgen',
        '_fuzzer': '@llvm-project//compiler-rt:fuzzer_main',
        '_fuzzer_no_main': '@llvm-project//compiler-rt:fuzzer',
    }.get(name)


def include_dependencies(relative):
    """None for non-LLVM includes; () if the toolchain already supplies them."""
    if not relative.startswith(PREFIX + '/'):
        return None
    if relative.startswith((PREFIX + '/llvm/', PREFIX + '/third-party/siphash/')):
        return None
    project = relative.removeprefix(PREFIX + '/').split('/')[0]
    return {
        'libc': ('@llvm-project//libc:libc_headers',),
        'libcxx': (),
        'libcxxabi': (),
        'libunwind': ('@llvm-project//libunwind:headers',),
        'compiler-rt': ('@llvm-project//compiler-rt:headers',),
    }[project]


def native_target(name, value):
    return (name.startswith('LLVM') or name in NATIVE_RUNTIMES) and value.get('type') == 'STATIC_LIBRARY'


# CMake builds the TableGen executables separately. Bazel uses the LLVM module's
# tools; none of these llvm/lib headers belong to a ClickHouse LLVM component.
_UNBUILT_HEADERS = (
    'CAS/*.h', 'DWARFLinker/**/*.h', 'DWARFCFIChecker/Registers.h',
    'CodeGen/MIRParser/MILexer.h', 'ExecutionEngine/IntelJITProfiling/*.h',
    'ExecutionEngine/Interpreter/Interpreter.h', 'ExecutionEngine/JITLink/*.h',
    'ExecutionEngine/MCJIT/MCJIT.h', 'ExecutionEngine/Orc/TargetProcess/OrcRTBootstrap.h',
    'FileCheck/FileCheckImpl.h', 'ObjCopy/**/*.h', 'Support/rpmalloc/*.h', 'TableGen/*.h',
    'Target/AArch64/Disassembler/*.h', 'Target/X86/AsmParser/*.h',
    'Target/X86/Disassembler/X86DisassemblerDecoder.h', 'Target/X86/MCA/X86CustomBehaviour.h',
)

# regex_impl.h declares the C functions in regcomp.c and the C++ calls in Regex.cpp.
_COMPONENT_HEADERS = {
    'LLVMAsmPrinter': {'srcs': ('CodeGen/AsmPrinter/*.h',)},
    'LLVMSupport__compile_0': {'srcs': ('Support/DebugOptions.h', 'Support/regex_impl.h')},
    'LLVMSupport__compile_1': {'srcs': (
        'Support/regengine.inc', 'Support/regex2.h', 'Support/regex_impl.h', 'Support/regutils.h',
    )},
    'LLVMX86Desc': {
        'srcs': ('Target/X86/MCTargetDesc/X86IntelInstPrinter.h',),
        'hdrs': (
            'Target/X86/MCTargetDesc/X86ATTInstPrinter.h',
            'Target/X86/MCTargetDesc/X86InstPrinterCommon.h', 'Target/X86/MCTargetDesc/X86MCExpr.h',
        ),
    },
}
_PLATFORM_HEADERS = {
    'LLVMSupport__compile_0': (('Support/Unix/*.h', 'Support/Unix/*.inc'), ('Support/Windows/*.inc',)),
    'LLVMTargetParser': (('TargetParser/Unix/Host.inc',), ('TargetParser/Windows/Host.inc',)),
}


def component_header_exclusions():
    patterns = set(_UNBUILT_HEADERS)
    for attrs in _COMPONENT_HEADERS.values():
        for files in attrs.values():
            patterns.update(files)
    for unix, windows in _PLATFORM_HEADERS.values():
        patterns.update(unix + windows)
    return tuple('llvm/lib/' + p for p in sorted(patterns))


def component_headers(name, attrs):
    """Declare llvm/lib implementation inputs on their native CMake compile groups."""
    from initial_migration.build_file import Concat, Glob, Select, select

    def inputs(patterns):
        paths = ['llvm/lib/' + p for p in patterns]
        return Glob(paths) if any('*' in p for p in paths) else paths

    def combine(old, value):
        if not old:
            return value
        if not value:
            return old
        if isinstance(old, list) and isinstance(value, list):
            return old + value
        if isinstance(old, Select) and isinstance(value, Select) and old.values.keys() == value.values.keys():
            return select({k: combine(v, value.values[k]) for k, v in old.values.items()})
        if isinstance(old, Concat):
            return Concat([*old.values[:-1], combine(old.values[-1], value)])
        return Concat([old, value])

    for field, patterns in _COMPONENT_HEADERS.get(name, {}).items():
        value = inputs(patterns)
        if name == 'LLVMX86Desc':
            value = Select({'@platforms//cpu:x86_64': value, '@platforms//cpu:aarch64': []})
        attrs[field] = combine(attrs.get(field), value)
    if name in _PLATFORM_HEADERS:
        unix, windows = _PLATFORM_HEADERS[name]
        attrs['srcs'] = combine(attrs.get('srcs'), Select({
            '@platforms//os:windows': inputs(windows),
            '//conditions:default': inputs(unix),
        }))


def declare_from_references(root, references):
    """Declare embedded LLVM targets before BUILD files are serialized.

    The CMake File API, expanded trace and Ninja graph are the inputs. Config
    values come from set() records at each configure_file(), never from the
    host on which this generator runs.
    """
    from initial_migration.build_file import BuildFile, Concat, Glob, select
    from initial_migration.repository_inputs import REPOSITORIES, cmake_has_source_prefix_map, source_label

    repository = REPOSITORIES[PREFIX]
    if repository['name'] != RAW_REPOSITORY or repository.get('module'):
        raise ValueError('Embedded LLVM must use the pinned raw repository')

    targets = {}
    models = {}
    graphs = {}
    for arch, reference in references.items():
        reference = Path(reference)
        saved = reference / 'cmake-file-api.tar.gz'
        model, graph = read_file_api(
            saved if saved.is_file() else reference / '.cmake/api/v1/reply',
            include_target=lambda name: name.startswith('LLVM') or name in NATIVE_RUNTIMES)
        models[arch] = model['paths']
        graphs[arch] = graph
        for name, value in graph.items():
            if native_target(name, value):
                targets.setdefault(name, {})[arch] = value
        absent = NATIVE_RUNTIMES - {name for name, value in graph.items() if native_target(name, value)}
        if absent:
            raise ValueError(f'{arch}: missing CMake runtime libraries {sorted(absent)}')
    owners = {name: PREFIX for name in targets}
    build = BuildFile(PREFIX, owners)
    header_patterns = [
        subdir + '/**/*' + suffix
        for subdir in ('llvm/include', 'llvm/lib', 'third-party/siphash/include')
        for suffix in ('.h', '.hh', '.hpp', '.hxx', '.inc', '.def', '.ipp', '.inl')
    ]
    # LLVM puts zos_wrappers on the system include path only when building for OS390.
    zos_wrappers = 'llvm/include/llvm/Support/SystemZ/zos_wrappers/*.h'
    build.rule('filegroup', 'llvm_source_headers', {'srcs': Concat([
        Glob(header_patterns, (*component_header_exclusions(), zos_wrappers)),
        select({
            '@platforms//os:linux': [],
            '//conditions:default': Glob([zos_wrappers]),
        }),
    ])})
    build.rule('filegroup', 'llvm_tablegen_inputs', {
        'srcs': Glob(['llvm/include/**/*.td', 'llvm/lib/Target/**/*.td']),
    })
    # Headers include libc++'s extensionless standard headers and libc sources
    # textually included by ClickHouse's mem_functions.cpp.
    build.rule('filegroup', 'runtime_source_inputs', {
        'srcs': Glob([
            'libcxx/include/**', 'libcxx/src/**',
            'libcxxabi/include/**', 'libcxxabi/src/**',
            'libunwind/include/**', 'libunwind/src/**',
            'compiler-rt/include/**', 'compiler-rt/lib/**', 'libc/**',
            'third-party/siphash/include/**',
        ], exclude=('**/BUILD', '**/BUILD.bazel')),
    })
    for library in ('libcxx', 'libcxxabi'):
        build.rule('filegroup', 'runtime_' + library + '_headers', {
            'srcs': Glob([library + '/include/**'], exclude=('**/BUILD', '**/BUILD.bazel')),
        })
    build.rule('cc_library', 'libcxxabi_headers', {
        'hdrs': Glob(['libcxxabi/include/*.h']),
        'includes': ['libcxxabi/include'],
    })
    for path in RUNTIME_ROOT_INPUTS:
        if not (root / path).is_file():
            raise ValueError('Missing ClickHouse runtime input: ' + path)
    runtime_generated = {}
    outline_generated = {}
    outline_commands = {}
    generated = {}
    for arch, reference in references.items():
        reference = Path(reference)
        source_root = models[arch]['source']
        build_root = models[arch]['build']
        cmake_file = source_root + '/contrib/llvm-project-cmake/CMakeLists.txt'
        values = {}
        outputs = []
        runtime_outputs = []
        templates = set()
        trace_path = reference / 'trace.jsonl'
        if trace_path.is_file():
            trace_file = trace_path.open('rt')
        else:
            trace_file = gzip.open(reference / 'trace.jsonl.gz', 'rt')
        with trace_file as trace:
            for line in trace:
                if not line.startswith('{'):
                    continue
                record = json.loads(line)
                if (record.get('file') == source_root + '/contrib/libcxx-cmake/CMakeLists.txt'
                        and record.get('cmd', '').lower() == 'configure_file'):
                    args = record['args']
                    relative_source = Path(args[0]).relative_to(source_root).as_posix()
                    relative_dest = Path(args[1]).relative_to(build_root + '/' + PREFIX).as_posix()
                    expected_sources = {PREFIX + '/libcxx/include/__config_site',
                                        PREFIX + '/libcxx/include/__assertion_handler'}
                    if (len(args) != 3 or args[2].upper() != 'COPYONLY'
                            or relative_source not in expected_sources
                            or relative_dest != 'libcxx/include/c++/v1/' + Path(relative_source).name):
                        raise ValueError(f'{arch}: unsupported CMake runtime configure_file: {args}')
                    out = 'gen/' + arch + '/' + relative_dest
                    build.rule('copy_file', 'runtime_header_' + arch + '_' + Path(relative_source).name, {
                        'src': ':' + relative_source.removeprefix(PREFIX + '/'), 'out': out,
                    })
                    runtime_outputs.append(':' + out)
                if record.get('file') != cmake_file:
                    continue
                args = record.get('args', [])
                command = record.get('cmd', '').lower()
                if command == 'set' and len(args) > 1:
                    values[args[0]] = ';'.join(args[1:])
                elif command == 'configure_file':
                    source = Path(args[0]).relative_to(source_root).as_posix()
                    destination = Path(args[1]).relative_to(build_root + '/' + PREFIX).as_posix()
                    template = (root / source).read_text()
                    substitutions = {}
                    for key in re.findall(r'@([A-Za-z_][A-Za-z_0-9]*)@', template):
                        if key not in values:
                            raise ValueError(cmake_file + ': unresolved ' + key)
                        substitutions['@' + key + '@'] = values[key]
                    output = 'gen/' + arch + '/' + destination
                    rule = 'configure_' + arch + '_' + Path(destination).name.replace('.', '_').replace('-', '_')
                    build.rule('expand_template', rule, {
                        'template': '@clickhouse//:' + source,
                        'out': output,
                        'substitutions': substitutions,
                    })
                    templates.add(source)
                    outputs.append(':' + output)
                elif command == 'file' and args and args[0].upper() == 'WRITE':
                    prefix = build_root + '/' + PREFIX + '/llvm/'
                    if not args[1].startswith(prefix):
                        continue
                    output = 'gen/' + arch + '/' + args[1].removeprefix(build_root + '/' + PREFIX + '/')
                    rule = 'configure_' + arch + '_' + Path(output).name.replace('.', '_')
                    # write_file joins content with a newline without appending
                    # one; preserving the final empty item preserves CMake's.
                    build.rule('write_file', rule, {'out': output, 'content': ''.join(args[2:]).split('\n')})
                    outputs.append(':' + output)
        if len(templates) != 3 or len(outputs) != 10:
            raise ValueError(f'{arch}: expected 3 LLVM templates and 7 file(WRITE) outputs, found {len(outputs)}')
        if len(runtime_outputs) != 2 or len(set(runtime_outputs)) != 2:
            raise ValueError(f'{arch}: expected both CMake COPYONLY libc++ headers: {runtime_outputs}')
        runtime_generated[arch] = runtime_outputs

        ninja_path = reference / 'build.ninja'
        if not ninja_path.is_file():
            ninja_path = reference / 'build.ninja.gz'
        for edge in ninja_edges(ninja_path):
            if edge['rule'] != 'CUSTOM_COMMAND' or len(edge['outputs']) != 1 or 'command' not in edge:
                continue
            node = edge['outputs'][0]
            if not node.startswith((PREFIX + '/llvm/',
                                    'contrib/compiler-rt-cmake/outline_atomic_helpers.dir/outline_atomic_')):
                continue
            command = edge['command'].strip()
            pieces = command.split(' && ')
            words = shlex.split(pieces[-1])
            tool_name = Path(words[0]).name
            if node.startswith('contrib/compiler-rt-cmake/outline_atomic_helpers.dir/'):
                source = source_root + '/' + PREFIX + '/compiler-rt/lib/builtins/aarch64/lse.S'
                destination = build_root + '/' + node
                if tool_name != 'cmake' or words[1:] != ['-E', 'create_symlink', source, destination]:
                    raise ValueError(f'{arch} {node}: unexpected CMake outline assembly command {command}')
                identity = 'build_bazel_reference_' + arch + '/' + node
                if identity in outline_commands:
                    raise ValueError(f'{arch}: duplicate CMake outline generator {node}')
                outline_commands[identity] = source.removeprefix(source_root + '/')
                continue
            if tool_name not in {'llvm-tblgen', 'llvm-min-tblgen'}:
                raise ValueError(f'{arch} {node}: unsupported LLVM generator {command}')
            tool = '@llvm-project//llvm:' + tool_name
            inputs = [':llvm_tablegen_inputs']
            anchor = ':llvm/include/llvm/CodeGen/ValueTypes.td'
            output = 'gen/' + arch + '/' + node.removeprefix(PREFIX + '/')
            action_outputs = [output]
            if '-gen-register-info' in words:
                # LLVM RegisterInfoEmitter::run returns four additional
                # files; Ninja tracks only the main .inc. RBE must
                # upload every file that the main .inc includes.
                action_outputs.extend(output.removesuffix('.inc') + suffix for suffix in (
                    'Enums.inc', 'MCDesc.inc', 'Header.inc', 'TargetDesc.inc'))
            translated = ['$(execpath ' + tool + ')']
            for word in words[1:]:
                if word == build_root + '/' + node:
                    translated.append('$(location :' + output + ')')
                elif word.startswith(source_root + '/' + PREFIX + '/'):
                    path = word.removeprefix(source_root + '/' + PREFIX + '/')
                    if path.endswith('.td'):
                        label = ':' + path
                        inputs.append(label)
                        translated.append('$(execpath ' + label + ')')
                    else:
                        relative = os.path.relpath(path, os.path.dirname(anchor[1:]))
                        translated.append('$$(dirname $(execpath ' + anchor + '))/' + relative)
                elif os.path.isabs(word):
                    raise ValueError(f'{arch} {node}: undeclared TableGen input {word}')
                else:
                    translated.append(shlex.quote(word))
            if anchor not in inputs:
                inputs.append(anchor)
            rule = 'tablegen_' + arch + '_' + hashlib.sha256(node.encode()).hexdigest()[:16]
            build.rule('genrule', rule, {
                'srcs': inputs, 'outs': action_outputs, 'tools': [tool],
                'cmd': ' '.join(translated),
            })
            outputs.extend(':' + filename for filename in action_outputs)
        generated[arch] = outputs
        if len(outputs) < 45:
            raise ValueError(f'{arch}: incomplete LLVM TableGen graph ({len(outputs)} outputs)')

    build.rule('filegroup', 'llvm_generated_headers', {
        'srcs': select({'@platforms//cpu:' + a: out for a, out in generated.items()}),
    })
    build.rule('filegroup', 'runtime_generated_headers', {
        'srcs': select({'@platforms//cpu:' + a: out for a, out in runtime_generated.items()}),
    })
    build.rule('cc_library', 'llvm_headers', {
        'hdrs': [':llvm_source_headers'],
        'textual_hdrs': [':llvm_generated_headers'],
        'includes': select({
            '@platforms//cpu:' + a: ['llvm/include', 'gen/' + a + '/llvm/include', 'third-party/siphash/include']
            for a in references
        }),
    })
    builtins_group_counts = {}
    for name, variants in targets.items():
        compiled = {}
        for arch, value in variants.items():
            source_root = models[arch]['source']
            build_root = models[arch]['build']
            groups = []
            for group in value.get('compileGroups', []):
                sources = [value['sources'][i]['path'] for i in group['sourceIndexes']]
                source_labels = []
                for source in sources:
                    outline_prefix = 'build_bazel_reference_' + arch + '/contrib/compiler-rt-cmake/outline_atomic_helpers.dir/'
                    if (name == 'clang_rt_builtins' and arch == 'aarch64'
                            and source.startswith(outline_prefix)):
                        if outline_commands.get(source) != PREFIX + '/compiler-rt/lib/builtins/aarch64/lse.S':
                            raise ValueError(f'{arch}: no recorded Ninja symlink command for {source}')
                        if source not in outline_generated:
                            original = ':' + 'compiler-rt/lib/builtins/aarch64/lse.S'
                            output = 'gen/' + arch + '/compiler-rt-outline/' + Path(source).name
                            build.rule('copy_file', 'runtime_outline_' + Path(source).name.replace('.', '_'), {
                                'src': original, 'out': output,
                            })
                            # BuildFile.target resolves unqualified labels in
                            # ClickHouse before assigning their repository.
                            outline_generated[source] = '@' + RAW_REPOSITORY + '//:' + output
                        source_labels.append(outline_generated[source])
                    elif (source.startswith(PREFIX + '/llvm/') or
                          (name in NATIVE_RUNTIMES and
                           (source.startswith(PREFIX + '/') or source in RUNTIME_ROOT_SOURCES))):
                        source_labels.append(source_label(source))
                    else:
                        raise ValueError(f'{arch} {name}: non-LLVM source {source}')
                copts = []
                for fragment in group.get('compileCommandFragments', []):
                    for flag in shlex.split(fragment['fragment']):
                        if flag.startswith(('--gcc-toolchain=', '--ld-path=', '-ffile-prefix-map=')):
                            continue
                        if source_root in flag:
                            raise ValueError(f'{arch} {name}: undeclared compiler input {flag}')
                        copts.append(flag)
                deps = [] if name in NATIVE_RUNTIMES else ['@' + RAW_REPOSITORY + '//:llvm_headers']
                native_cxx_includes = []
                for inc in group.get('includes', []):
                    path = os.path.normpath(inc['path'])
                    is_generated = path.startswith(build_root + '/')
                    relative = path.removeprefix(build_root + '/').removeprefix(source_root + '/')
                    if (name not in NATIVE_RUNTIMES and group['language'] in {'C', 'ASM'}
                            and relative in TOOLCHAIN_INCLUDE_DIRECTORIES):
                        native_cxx_includes.append([
                            '-isystem' if inc.get('isSystem') else '-I',
                            'generated' if is_generated else 'source',
                            relative.removeprefix(PREFIX + '/'),
                        ])
                        continue
                    runtime_deps = None if name in NATIVE_RUNTIMES else include_dependencies(relative)
                    if runtime_deps is not None:
                        deps.extend(runtime_deps)
                        continue
                    if not relative.startswith(PREFIX + '/'):
                        raise ValueError(f'{arch} {name}: unexpected include {path}')
                    if is_generated:
                        directory = '$(GENDIR)/' + PREFIX + '/gen/' + arch + '/' + relative.removeprefix(PREFIX + '/')
                    else:
                        # Keep CMake's spelling after validating its normalized
                        # repository above. Clang retains ../ in __FILE__.
                        directory = inc['path'].removeprefix(source_root + '/') if name in NATIVE_RUNTIMES else relative
                    copts += ['-isystem' if inc.get('isSystem') else '-I', directory]
                groups.append({
                    'srcs': source_labels,
                    'copts': copts,
                    'cmake_source_prefix_map': cmake_has_source_prefix_map(
                        group.get('compileCommandFragments', []), source_root),
                    'cmake_reference_source_root': source_root,
                    'hdrs': (['@' + RAW_REPOSITORY + '//:runtime_source_inputs',
                              '@' + RAW_REPOSITORY + '//:runtime_generated_headers'] +
                             [source_label(p) for p in sorted(RUNTIME_ROOT_INPUTS)]
                             if name in NATIVE_RUNTIMES else []),
                    'textual_hdrs': [], 'additional_compiler_inputs': [],
                    'native_cxx_includes': native_cxx_includes,
                    'local_defines': [d['define'] if name in NATIVE_RUNTIMES else shlex.quote(d['define'])
                                      for d in group.get('defines', [])],
                    'deps': list(dict.fromkeys(deps)),
                    'language': group['language'],
                    'cmake_runtime': name in NATIVE_RUNTIMES,
                })
            ids = {target['id']: n for n, target in graphs[arch].items()}
            depends = [ids[d['id']] for d in value.get('dependencies', [])]
            if name == 'libllvmlibc' and 'glibc-rseq' in depends:
                if len(groups) != 1:
                    raise ValueError(f'{arch} libllvmlibc must have one math object group')
                # Native links require LLVM math before glibc-compatibility.
                # glibc-compatibility owns the private dependency on rseq.
                groups[0]['runtime_link_deps'] = ['@clickhouse//base/glibc-compatibility']
            compiled[arch] = {
                'groups': groups,
                'deps': [':' + d for d in depends if d in targets],
                'kind': 'STATIC_LIBRARY',
                'linkopts': ['--unwindlib=none'] if name == 'unwind' else [],
                'alwayslink': name in {'clang_rt_builtins', 'clang_rt_xray', 'libllvmlibc'},
            }
        build.target(name, compiled)
        if name == 'clang_rt_builtins':
            for arch, target in compiled.items():
                groups = target['groups']
                if not groups or any(not group['srcs'] for group in groups):
                    raise ValueError(f'{arch} {name}: Rust localization requires nonempty native compile groups')
                builtins_group_counts[arch] = len(groups)
    if len(outline_generated) != 125 or outline_generated.keys() != outline_commands.keys():
        raise ValueError(f'CMake AArch64 reference outline sources do not match recorded Ninja: '
                         f'{len(outline_generated)} sources, {len(outline_commands)} generators')

    if set(builtins_group_counts) != set(references):
        raise ValueError('Rust localization requires LLVM builtins groups for every architecture')
    (root / 'bazel/rules/rust_localization_data.bzl').write_text(
        'CLANG_RT_BUILTINS_GROUP_COUNTS = ' + json.dumps(builtins_group_counts, indent=4) + '\n')
    print('Embedded LLVM:', len(targets), 'CMake libraries;',
          ', '.join(a + ' ' + str(len(o)) + ' generated headers' for a, o in generated.items()))
    return build, targets, templates | RUNTIME_ROOT_INPUTS
