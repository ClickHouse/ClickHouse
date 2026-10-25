"""Translate CMake's generator invocations into declared Bazel actions.

Compilation and linking are never delegated to Ninja or CMake. Untranslated
custom commands stay visible in the coverage inventory.
"""

import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import tarfile
from initial_migration import llvm_targets
from initial_migration.build_file import ARCHES, starlark
from initial_migration.cmake_metadata import read_metadata
from initial_migration.repository_inputs import source_label, source_path
from initial_migration.reference_readers import read_ninja

TOOLS = {'protoc', 'grpc_cpp_plugin', 'flatc', 'llvm-tblgen', 'llvm-min-tblgen'}
GOOGLEAPIS_REVISION = 'e60db19f11f94175ac682c5898cce0f77cc508ea'
GOOGLEAPIS_ARCHIVE = 'contrib/google-cloud-cpp-cmake/googleapis/' + GOOGLEAPIS_REVISION + '.tar.gz'
GOOGLEAPIS_OUTPUT = 'external/googleapis/src/googleapis_download/'
GCP_OUTPUT = 'contrib/google-cloud-cpp-cmake/'
_GOOGLEAPIS_DIRECTORY = re.compile(
    r'gen/(?:' + '|'.join(ARCHES) + r')/('
    + re.escape(GCP_OUTPUT.rstrip('/')) + '|' + re.escape(GOOGLEAPIS_OUTPUT.rstrip('/'))
    + r')(?=/|$|[\s"\'])')


def googleapis_path(value):
    """Bazel configures generated Google APIs files under its target output root."""
    if isinstance(value, str):
        return _GOOGLEAPIS_DIRECTORY.sub(r'gen/\1', value)
    return value


def common_googleapis_rules(rules):
    """Use one protoc invocation for each identical recorded Google API schema."""
    remaining, recorded = {}, {}
    for name, attrs in rules.items():
        outs = [googleapis_path(path) for path in attrs['outs']]
        if outs == attrs['outs']:
            remaining[name] = attrs
            continue
        if any(old == new for old, new in zip(attrs['outs'], outs, strict=True)):
            raise ValueError('Google APIs generator also produced unrelated files: ' + name)
        arch = attrs['outs'][0].split('/', 2)[1]
        if arch not in ARCHES:
            raise ValueError('Unknown Google APIs generator architecture: ' + name)
        normalized = {
            key: [googleapis_path(item) for item in value] if isinstance(value, list)
            else googleapis_path(value)
            for key, value in attrs.items()
        }
        variants = recorded.setdefault(outs[0], {})
        if arch in variants:
            raise ValueError('Duplicate Google APIs output for ' + arch + ': ' + outs[0])
        variants[arch] = normalized

    shared = {}
    for output, variants in recorded.items():
        if set(variants) != set(ARCHES) or variants[ARCHES[0]] != variants[ARCHES[1]]:
            raise ValueError('Google APIs generators differ across target platforms: ' + output)
        shared[output] = variants[ARCHES[0]]

    for output, attrs in shared.items():
        if output.startswith('gen/' + GOOGLEAPIS_OUTPUT):
            name = 'extract_googleapis'
        elif output.endswith('.grpc.pb.cc'):
            continue
        elif output.startswith('gen/' + GCP_OUTPUT) and output.endswith('.pb.cc'):
            stem = output.removesuffix('.pb.cc')
            grpc = shared.get(stem + '.grpc.pb.cc')
            if (grpc is None or attrs['outs'] != [stem + '.pb.cc', stem + '.pb.h']
                    or grpc['outs'] != [stem + '.grpc.pb.cc', stem + '.grpc.pb.h']
                    or attrs['srcs'] != grpc['srcs']
                    or set(attrs['tools']) != {'@clickhouse_google_protobuf//:protoc'}
                    or set(grpc['tools']) != {
                        '@clickhouse_google_protobuf//:protoc',
                        '@clickhouse_grpc//:grpc_cpp_plugin',
                    }
                    or '--cpp_out' not in attrs['cmd']
                    or '--cpp_out' not in grpc['cmd'] or '--grpc_out' not in grpc['cmd']):
                raise ValueError('Google APIs protobuf and gRPC generators disagree: ' + output)
            # The recorded gRPC command already produces the C++ outputs.
            attrs = dict(grpc, outs=attrs['outs'] + grpc['outs'])
            name = 'generate_googleapis_' + stem.removeprefix('gen/' + GCP_OUTPUT).replace('/', '_')
        else:
            raise ValueError('Unsupported Google APIs generator output: ' + output)
        if name in remaining:
            raise ValueError('Duplicate Google APIs target: ' + name)
        remaining[name] = attrs
    return remaining


def googleapis_source_map(sources):
    """Map both CMake proto names to the same any2bazel source identity."""
    result = {}
    for source, identity in sources.items():
        if identity.endswith('.pb.cc'):
            for arch in ARCHES:
                prefix = '@build/' + arch + '/' + GCP_OUTPUT
                if identity.startswith(prefix):
                    identity = '@build/' + GCP_OUTPUT + identity.removeprefix(prefix)
                    source = googleapis_path(source)
                    break
        if source in result and result[source] != identity:
            raise ValueError('Conflicting any2bazel source identities for: ' + source)
        result[source] = identity
    return result


def non_vendor_license_paths(manifest, substitutions):
    """Return the inputs not supplied by @clickhouse_rust_vendor//:licenses."""
    paths = {row[2].lstrip('/') for row in manifest}
    for replacement in substitutions.values():
        if replacement is None:
            continue
        path = replacement[1].lstrip('/')
        if path.startswith('contrib/rust_vendor/') and path not in paths:
            raise ValueError(f'CMake selected a different Rust license: {path}')
        paths.add(path)
    return sorted(path for path in paths if not path.startswith('contrib/rust_vendor/'))


def render_license(attrs, license_inputs):
    """Reuse the recorded license-to-file mapping where the reference agrees."""
    fields = []
    for key, value in attrs.items():
        if key == 'srcs' and value == list(license_inputs.values()):
            rendered = 'list(_LICENSE_INPUTS.values())'
        elif key == 'source_paths' and value == list(license_inputs):
            rendered = 'list(_LICENSE_INPUTS)'
        else:
            rendered = starlark(value, 4)
        fields.append('    ' + json.dumps(key) + ': ' + rendered + ',\n')
    return '{\n' + ''.join(fields) + '  }'


def render_rules(rules):
    """Record identical generators once; retain rule and input order for both architectures."""
    from initial_migration.package_codegen import partition
    rules, package_rules = partition(rules)
    first_arch, second_arch = ARCHES
    shared = {}
    references = {}
    for name, attrs in rules.items():
        second_name = name.replace(first_arch, second_arch)
        if name == second_name or second_name not in rules:
            continue
        normalized = []
        for arch, record in ((first_arch, attrs), (second_arch, rules[second_name])):
            prefix = 'gen/' + arch + '/'
            normalized.append({
                key: [item.replace(prefix, 'gen/{arch}/') for item in value]
                if isinstance(value, list) else value.replace(prefix, 'gen/{arch}/')
                for key, value in record.items()
            })
        if normalized[0] != normalized[1]:
            continue
        # Names for shared commands are the output file, not an opaque action hash.
        output = normalized[0]['outs'][0].removeprefix('gen/{arch}/')
        if output in shared:
            raise ValueError('Duplicate first codegen output: ' + output)
        shared[output] = normalized[0]
        references[name] = (output, first_arch)
        references[second_name] = (output, second_arch)
    first_licenses = rules['generate_' + first_arch + '_licenses']
    license_inputs = dict(zip(first_licenses['source_paths'], first_licenses['srcs'], strict=True))
    text = [
        'load("//bazel/rules:codegen_actions.bzl", "codegen_for_arch")\n',
        'load("//bazel/rules:repository_paths.bzl", "repository_path")\n\n',
        '_SHARED_CODEGEN = ' + starlark(shared) + '\n\n',
        '_LICENSE_INPUTS = ' + starlark(license_inputs) + '\n\n',
        *(name + ' = ' + starlark(attrs) + '\n\n' for name, attrs in package_rules.items()),
        'CODEGEN_RULES = {\n',
    ]
    for name, attrs in rules.items():
        if name in references:
            output, arch = references[name]
            rendered = 'codegen_for_arch(_SHARED_CODEGEN[' + json.dumps(output) + '], ' + json.dumps(arch) + ')'
        elif attrs.get('kind') == 'licenses':
            rendered = render_license(attrs, license_inputs)
        else:
            rendered = starlark(attrs, 2)
        text.append('  ' + json.dumps(name) + ': ' + rendered + ',\n')
    text.append('}\n')
    return ''.join(text)


class Codegen:
    def __init__(self, graph):
        self.graph = graph
        self.root = graph.build.parent
        self.cmake, = re.findall(r'^CMAKE_COMMAND:INTERNAL=(.+)$',
                                 (graph.build / 'CMakeCache.txt').read_text(), re.MULTILINE)
        self.nodes = read_ninja(graph.build / 'build.ninja')
        self.rules = {}
        self.handled = set()
        self.googleapis = {}
        self.googleapis_used = set()
        with tarfile.open(self.root / GOOGLEAPIS_ARCHIVE) as archive:
            for member in archive.getmembers():
                if member.isfile() and member.name.endswith('.proto'):
                    self.googleapis[member.name.split('/', 1)[1]] = archive.extractfile(member).read().decode()
        self.tool_paths = {}
        for target in graph.targets.values():
            if target['name'] in TOOLS:
                for artifact in target.get('artifacts', []):
                    self.tool_paths[str(graph.build / artifact['path'])] = target['name']
        # CMake's cross configuration builds these separately for the host.
        for output, node in self.nodes.items():
            for word in shlex.split(node.get('command', '')):
                build_index = word.find(str(graph.build))
                if build_index >= 0 and Path(word).name in TOOLS:
                    self.tool_paths[word[build_index:]] = Path(word).name

    def label(self, path):
        return source_label(path)

    def proto_inputs(self, name, import_dirs=()):
        pending = [name]
        inputs = set()
        seen = set()
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            if not import_dirs and name in self.googleapis:
                self.googleapis_used.add(name)
                generated = str(self.graph.build / GOOGLEAPIS_OUTPUT / name)
                inputs.add(self.label(self.graph.path(generated)))
                source_text = self.googleapis[name]
            else:
                roots = import_dirs or (self.root / 'contrib/google-protobuf/src',)
                source = next((root / name for root in roots if (root / name).is_file()), None)
                if source is None:
                    raise ValueError('Unresolved proto import: ' + name)
                inputs.add(self.label(source.relative_to(self.root).as_posix()))
                source_text = source.read_text()
            pending.extend(re.findall(r'^[ \t]*import\s+(?:public\s+|weak\s+)?"([^"]+)"', source_text, re.MULTILINE))
        return inputs

    def shell_path(self, value):
        # A shell variable keeps paths valid after a generator's working-dir
        # change. Bazel expands locations before the command executes.
        root_index = value.find(str(self.root) + '/')
        if root_index >= 0:
            value = value[:root_index] + os.path.normpath(value[root_index:])
        for path, tool in sorted(self.tool_paths.items(), key=lambda x: -len(x[0])):
            if path in value:
                return '"' + value.replace(path, '$$execroot/$(execpath //:' + tool + ')') + '"'
        value = value.replace(str(self.graph.build), '$$execroot/$(BINDIR)/bazel/gen/' + self.graph.arch)
        value = value.replace(str(self.root) + '/', '$$execroot/')
        if '$$execroot' in value:
            return '"' + value + '"'
        return shlex.quote(value)

    def translate(self, node):
        command = node.get('command', '')
        if node['outputs'] == ['contrib/krb5-cmake/include/krb5/krb5.h']:
            # dependency_inputs creates this public header beside _krb5.
            self.handled.update(node['outputs'])
            return
        if node['outputs'] == ['contrib/libhdfs3-cmake/crc_iscsi_v_pcl.o']:
            source = self.label('contrib/libhdfs3/src/common/crc_iscsi_v_pcl.asm')
            output = 'gen/' + self.graph.arch + '/' + node['outputs'][0]
            self.rules['generate_' + self.graph.arch + '_hdfs_crc'] = dict(
                srcs=[source],
                outs=[output, output + '.asm'],
                tools=['//bazel/rules:prepare_hdfs_crc', '@nasm//:nasm'],
                cmd='$(execpath //bazel/rules:prepare_hdfs_crc) $(execpath ' + source + ') '
                    '$(location ' + output + '.asm) && '
                    '$(execpath @nasm//:nasm) -f elf64 -g -F dwarf -D LINUX '
                    '-o $(location ' + output + ') $(location ' + output + '.asm)',
            )
            self.handled.update(node['outputs'])
            return
        if '/embed_sql_console_resources.cmake' in command or '/embed_clickstack_resources.cmake' in command:
            self.web_resources(node)
            return
        tools = sorted({tool for path, tool in self.tool_paths.items() if path in command})
        if not tools and any(Path(p).suffix not in {'.h', '.hpp', '.inc', '.c', '.cc', '.cpp', '.S', '.et'} for p in node['outputs']):
            return
        segments = []
        extra_inputs = []
        replaced_inputs = set()
        for part in command.split(' && '):
            words = shlex.split(part)
            if words[:1] == ['cd']:
                directory = self.shell_path(words[1])
                segments += ['mkdir -p ' + directory, 'cd ' + directory]
            elif words[:3] == [self.cmake, '-E', 'make_directory']:
                segments.append('mkdir -p ' + ' '.join(self.shell_path(w) for w in words[3:]))
            elif words[:2] == [self.cmake, '-E'] and words[2] in {'copy', 'copy_if_different', 'create_symlink'}:
                # The build consumes file contents, so a declared copy also
                # preserves CMake's generated assembly/header symlink inputs.
                segments.append('cp ' + ' '.join(self.shell_path(w) for w in words[3:]))
                extra_inputs.extend(words[3:-1])
            elif words[0] in self.tool_paths:
                segments.append(' '.join(self.shell_path(w) for w in words))
            elif words[0] == 'perl':
                script_index = next(i for i, w in enumerate(words[1:], 1) if not w.startswith('-'))
                script = Path(words[script_index]).name
                args = words[script_index + 1:]
                if script in {'gen.pl', 'gen-map.pl'}:
                    tool = 'krb5_gen' if script == 'gen.pl' else 'krb5_gen_map'
                    tools.append(tool)
                    segments.append('"$$execroot/$(execpath //:' + tool + ')" ' + ' '.join(self.shell_path(w) for w in args))
                elif script == 'compile_et':
                    # compile_et.sh invokes these two canonical AWK programs.
                    # The -d argument overrides its configured search path.
                    source = args[-1]
                    basename = Path(source).stem
                    directory = args[args.index('-d') + 1]
                    for extension in ['h', 'c']:
                        program = directory + '/et_' + extension + '.awk'
                        extra_inputs.append(program)
                        segments.append('awk -f ' + self.shell_path(program) + ' outfile=' + shlex.quote(basename + '.' + extension)
                                        + (' textdomain= localedir=' if extension == 'c' else '') + ' ' + self.shell_path(source))
                    replaced_inputs.add(words[script_index])
                else:
                    return
            elif words[0] == 'cat' and '>' in words:
                extra_inputs.extend(words[1:words.index('>')])
                segments.append(' '.join('>' if w == '>' else self.shell_path(w) for w in words))
            elif words[:5] == [self.cmake, '-E', 'env', 'bash', '-c']:
                inner = shlex.split(words[5])
                if not inner[0].endswith('/openldap/build/mkversion'):
                    return
                extra_inputs.append(inner[0])
                segments.append('SOURCE_DATE_EPOCH=1 bash ' + ' '.join('>' if w == '>' else self.shell_path(w) for w in inner))
            else:
                return
        if not any(not s.startswith(('cd ', 'mkdir ')) for s in segments):
            return
        inputs = set()
        own_outputs = {os.path.normpath(self.graph.build / path) for path in node['outputs']}
        for path in node['normal_inputs'] + extra_inputs:
            full = os.path.normpath(self.graph.build / path)
            # A later command in this action may copy an earlier output.
            if full in replaced_inputs or full in own_outputs:
                continue
            producer = self.nodes.get(full) or self.nodes.get(os.path.relpath(full, self.graph.build))
            if full in self.tool_paths or producer is None and not Path(full).is_file():
                continue
            if producer is not None and producer['rule'] == 'phony':
                continue
            inputs.add(self.label(self.graph.path(full)))
            archive_prefix = str(self.graph.build / GOOGLEAPIS_OUTPUT) + '/'
            if full.startswith(archive_prefix) and full.endswith('.proto'):
                inputs.update(self.proto_inputs(full.removeprefix(archive_prefix)))
        # TableGen, protoc and flatc read included schemas beyond their main
        # input. CMake's initial Ninja graph does not enumerate those includes.
        words = shlex.split(command)
        include_dirs = []
        args = iter(words)
        for word in args:
            if word.startswith('-I'):
                include_dirs.append(Path(os.path.normpath(word[2:] or next(args))))
        source_dirs = [d for d in include_dirs if d.is_relative_to(self.root) and not d.is_relative_to(self.graph.build)]
        schemas = [Path(word) for word in words if word.endswith('.proto')]
        # protoc resolves imports in -I order. Keep generated Google APIs schemas
        # on the archive-backed lookup above; their contents are not source files.
        root = next((d for d in source_dirs if len(schemas) == 1 and d in schemas[0].parents), None)
        if 'protoc' in tools and root is not None and include_dirs == source_dirs:
            inputs.update(self.proto_inputs(schemas[0].relative_to(root).as_posix(), source_dirs))
        else:
            suffixes = {'.td'} if any('tblgen' in t for t in tools) else {'.proto', '.fbs'}
            for directory in source_dirs:
                inputs.update(self.label(p.relative_to(self.root).as_posix())
                              for p in directory.rglob('*') if p.suffix in suffixes and p.is_file())
        # FlatBuffers schema imports are resolved beside the input schemas.
        for path in list(inputs):
            if path.endswith('.fbs'):
                inputs.update(self.label(p.relative_to(self.root).as_posix())
                              for p in (self.root / source_path(path)).parent.glob('*.fbs'))
        outputs = ['gen/' + self.graph.arch + '/' + p for p in node['outputs']]
        mkdirs = sorted({str(Path('$(BINDIR)/bazel') / p).rsplit('/', 1)[0] for p in outputs})
        script = 'execroot=$$PWD\nmkdir -p ' + ' '.join(mkdirs) + '\n' + ' && '.join(segments)
        if tools == ['flatc'] and node['outputs'][0] == 'contrib/flatbuffers/generated/File_generated.h':
            arrow = self.root / 'contrib/arrow'
            schemas = sorted((arrow / 'format').glob('*.fbs')) + [arrow / 'cpp/src/arrow/ipc/feather.fbs']
            directory = Path(node['outputs'][0]).parent
            argv = shlex.split(command.rsplit(' && ', 1)[-1])
            tail = ['-o', str(self.graph.build / directory), *map(str, schemas)]
            if (argv[-len(tail):] != tail
                    or node['outputs'] != [str(directory / (s.stem + '_generated.h')) for s in schemas]):
                raise ValueError('Arrow IPC schema filegroup disagrees with the native flatc command')
            inputs = {'@clickhouse_arrow//:ipc_metadata_schemas'}
            script = ('"$(execpath //:flatc)" ' + shlex.join(argv[1:-len(tail)])
                      + ' -o "$(RULEDIR)/' + str(Path(outputs[0]).parent) + '" $(SRCS)')
        name = 'generate_' + self.graph.arch + '_' + hashlib.sha256(node['outputs'][0].encode()).hexdigest()[:16]
        self.rules[name] = dict(srcs=sorted(inputs), outs=outputs, tools=['//:' + t for t in tools], cmd=script)
        self.handled.update(node['outputs'])

    def web_resources(self, node):
        output = node['outputs'][0]
        sql_console = 'SQLConsole' in output
        namespace = 'SQLConsole' if sql_console else 'ClickStack'
        source = 'contrib/sql-console/dist-standalone' if sql_console else 'contrib/clickstack/out'
        compressed = 'sql_console_compressed' if sql_console else 'clickstack_compressed'
        files = sorted(p for p in (self.root / source).rglob('*') if p.is_file())
        byproducts = [str(Path(output).parent / compressed / (p.relative_to(self.root / source).as_posix() + '.gz'))
                      for p in files]
        node['outputs'].extend(byproducts)
        for path in byproducts:
            self.nodes[path] = node
        prefix = 'gen/' + self.graph.arch + '/'
        attrs = dict(
            srcs=['@clickhouse_sql_console//:dist_standalone'] if sql_console else [
                self.label(p.relative_to(self.root).as_posix()) for p in files
            ],
            outs=[prefix + p for p in node['outputs']],
            tools=['//bazel/rules:embed_resources'],
            cmd='execroot=$$PWD\n$(execpath //bazel/rules:embed_resources) --source=' + self.shell_path(str(self.root / source))
                + ' --output=$(BINDIR)/bazel/' + prefix + output
                + ' --namespace=' + namespace + ' --compressed=' + compressed,
        )
        resources = read_metadata(self.root)[self.graph.arch]['resource_gzip_mtimes']
        if namespace in resources:
            attrs['cmake_condition'] = '//bazel:use_cmake_metadata_' + self.graph.arch
            attrs['cmake_cmd'] = ' --cmake-gzip-mtime=' + str(resources[namespace])
        self.rules['generate_' + self.graph.arch + '_' + namespace] = attrs
        self.handled.update(node['outputs'])

    def licenses(self, output):
        manifest = json.loads((self.root / 'bazel/rules/license_manifest.json').read_text())
        substitutions = {}
        metadata_name = read_metadata(self.root)[self.graph.arch].get(
            'license_substitutions', 'license_substitutions_' + self.graph.arch + '.json',
        )
        metadata_file = self.root / 'bazel/initial_migration' / metadata_name
        attrs = {'kind': 'licenses', 'outs': ['gen/' + self.graph.arch + '/' + output]}
        if metadata_file.is_file():
            substitutions = json.loads(metadata_file.read_text())
            attrs['cmake_metadata'] = '//bazel/initial_migration:' + metadata_name
            attrs['cmake_condition'] = '//bazel:use_cmake_metadata_' + self.graph.arch
        attrs['source_paths'] = non_vendor_license_paths(manifest, substitutions)
        # The reference inventory reads the toolchain LLVM license, including
        # when ClickHouse LLVM objects are compiled in @clickhouse_llvm_project.
        attrs['srcs'] = [
            '@llvm-project//:LICENSE.TXT' if path == llvm_targets.PREFIX + '/LICENSE.TXT' else self.label(path)
            for path in attrs['source_paths']
        ]
        self.rules['generate_' + self.graph.arch + '_licenses'] = attrs
        self.handled.add(output)

    def embedded_sources(self):
        if self.graph.embed_copies:
            prefix = 'gen/' + self.graph.arch + '/embedded/'
            sources = sorted(self.graph.embed_copies)
            self.rules['generate_' + self.graph.arch + '_embedded_sources'] = dict(
                kind='embedded_sources',
                srcs=[self.label(source) for source in sources],
                outs=[prefix + source for source in sources],
            )

    def generate(self):
        seen = set()
        for output, node in list(self.nodes.items()):
            if node['rule'] != 'CUSTOM_COMMAND' or id(node) in seen:
                continue
            seen.add(id(node))
            if all(llvm_targets.generated_target(path) for path in node['outputs']):
                continue
            self.translate(node)
        if self.googleapis_used:
            prefix = 'gen/' + self.graph.arch + '/' + GOOGLEAPIS_OUTPUT
            self.rules['extract_googleapis_' + self.graph.arch] = dict(
                srcs=[self.label(GOOGLEAPIS_ARCHIVE)],
                outs=[prefix + n for n in sorted(self.googleapis_used)],
                tools=['//bazel/rules:extract_archive'],
                cmd='$(execpath //bazel/rules:extract_archive) --archive=$(execpath ' + self.label(GOOGLEAPIS_ARCHIVE) + ')'
                    + ' --root=$(BINDIR)/bazel/' + prefix
                    + ' --strip-prefix=googleapis-' + GOOGLEAPIS_REVISION + ' '
                    + ' '.join(shlex.quote(n) for n in sorted(self.googleapis_used)),
            )

    def target_generated_inputs(self, name):
        found = set()
        seen = set()
        pending = ['cmake_object_order_depends_target_' + name]
        while pending:
            path = pending.pop()
            if path in seen:
                continue
            seen.add(path)
            if path in self.handled:
                found.update(self.nodes[path]['outputs'])
            elif path in self.nodes and self.nodes[path]['rule'] == 'phony':
                pending.extend(self.nodes[path]['inputs'])
        return [self.label(self.graph.path(str(self.graph.build / p))) for p in sorted(found)]
