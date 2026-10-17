"""Translate CMake's generator invocations into declared Bazel actions.

Compilation and linking are never delegated to Ninja or CMake. Untranslated
custom commands stay visible in the coverage inventory.
"""

import hashlib
import os
from pathlib import Path
import re
import shlex
import tarfile
import llvm_targets
from repository_inputs import source_label, source_path

TOOLS = {'protoc', 'grpc_cpp_plugin', 'flatc'}
GOOGLEAPIS_REVISION = 'e60db19f11f94175ac682c5898cce0f77cc508ea'
GOOGLEAPIS_ARCHIVE = 'contrib/google-cloud-cpp-cmake/googleapis/' + GOOGLEAPIS_REVISION + '.tar.gz'
GOOGLEAPIS_OUTPUT = 'external/googleapis/src/googleapis_download/'


def read_ninja(path):
    nodes = {}
    current = None
    for line in path.read_text().replace('$\n', '').splitlines():
        if line.startswith('build '):
            outputs, rest = line[6:].split(': ', 1)
            words = rest.split()
            current = {
                'outputs': outputs.split(' | ')[0].split(),
                'rule': words[0],
                'inputs': [w for w in words[1:] if w not in {'|', '||'}],
                'normal_inputs': [w for w in words[1:words.index('||') if '||' in words else len(words)] if w != '|'],
            }
            for output in current['outputs']:
                nodes[output] = current
        elif current is not None and line.startswith('  COMMAND = '):
            current['command'] = line.removeprefix('  COMMAND = ').replace('$$', '$')
    return nodes


class Codegen:
    def __init__(self, graph):
        self.graph = graph
        self.root = graph.build.parent
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

    def proto_inputs(self, name):
        pending = [name]
        inputs = set()
        seen = set()
        while pending:
            name = pending.pop()
            if name in seen:
                continue
            seen.add(name)
            generated = str(self.graph.build / GOOGLEAPIS_OUTPUT / Path(name))
            label = self.label(self.graph.path(generated))
            if name in self.googleapis:
                self.googleapis_used.add(name)
                inputs.add(label)
                source_text = self.googleapis[name]
            else:
                source = self.root / 'contrib/google-protobuf/src' / name
                if not source.is_file():
                    raise ValueError('Unresolved Google API import: ' + name)
                inputs.add(self.label(source.relative_to(self.root).as_posix()))
                source_text = source.read_text()
            pending.extend(re.findall(r'^import\s+(?:public\s+|weak\s+)?"([^"]+)"', source_text, re.MULTILINE))
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
        if node['outputs'] == ['contrib/libhdfs3-cmake/crc_iscsi_v_pcl.o']:
            source = self.label('contrib/libhdfs3/src/common/crc_iscsi_v_pcl.asm')
            output = 'gen/' + self.graph.arch + '/' + node['outputs'][0]
            self.rules['generate_' + self.graph.arch + '_hdfs_crc'] = dict(
                srcs=[source],
                outs=[output, output + '.asm'],
                tools=['//bazel:prepare_hdfs_crc', '@nasm//:nasm'],
                cmd='$(execpath //bazel:prepare_hdfs_crc) $(execpath ' + source + ') '
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
            elif words[:3] == ['/usr/bin/cmake', '-E', 'make_directory']:
                segments.append('mkdir -p ' + ' '.join(self.shell_path(w) for w in words[3:]))
            elif words[:2] == ['/usr/bin/cmake', '-E'] and words[2] in {'copy', 'copy_if_different', 'create_symlink'}:
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
            elif words[:5] == ['/usr/bin/cmake', '-E', 'env', 'bash', '-c']:
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
        for index, word in enumerate(words):
            directory = None
            if word == '-I' and index + 1 < len(words):
                directory = words[index + 1]
            elif word.startswith('-I') and len(word) > 2:
                directory = word[2:]
            if directory and directory.startswith(str(self.root)) and not directory.startswith(str(self.graph.build)):
                directory = Path(os.path.normpath(directory))
                suffixes = {'.td'} if any('tblgen' in t for t in tools) else {'.proto', '.fbs'}
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
        self.rules['generate_' + self.graph.arch + '_' + namespace] = dict(
            srcs=[self.label(p.relative_to(self.root).as_posix()) for p in files],
            outs=[prefix + p for p in node['outputs']],
            tools=['//bazel:embed_resources'],
            cmd='execroot=$$PWD\n$(execpath //bazel:embed_resources) --source=' + self.shell_path(str(self.root / source))
                + ' --output=$(BINDIR)/bazel/' + prefix + output
                + ' --namespace=' + namespace + ' --compressed=' + compressed,
        )
        self.handled.update(node['outputs'])

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
                tools=['//bazel:extract_archive'],
                cmd='$(execpath //bazel:extract_archive) --archive=$(execpath ' + self.label(GOOGLEAPIS_ARCHIVE) + ')'
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
