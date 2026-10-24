"""Map reference-checkout inputs to pinned Bazel source repositories."""

import ast
from dataclasses import dataclass
from functools import cache
import json
import os
from pathlib import Path
import posixpath
import re
import shlex
import subprocess
import tarfile

ROOT = Path(__file__).resolve().parents[2]
REPOSITORIES = json.loads((ROOT / 'bazel/rules/contrib_repositories.json').read_text())
EXPORTS = {path: set() for path in REPOSITORIES}
# rules_cc's allowed header extensions (cc/private/cc_helper.bzl).
CC_HEADER_SUFFIXES = ('.h', '.hh', '.hpp', '.ipp', '.hxx', '.h++',
                      '.inc', '.inl', '.tlh', '.tli', '.H', '.tcc')
# Retain textual and generated-header source classes even when a macro
# computes an include name. Ordinary build/test data is retained only when a
# scanned include actually names it.
TEXTUAL_SOURCE_SUFFIXES = (*CC_HEADER_SUFFIXES, '.def', '.defs', '.th', '.td', '.tbl', '.tbi',
                           '.tpp', '.icc', '.ixx', '.cuh', '.msh', '.txx', '.in', '.proto', '.fbs',
                           '.capnp', '.thrift', '.rl', '.gperf', '.grammar')


@dataclass(frozen=True)
class HeaderDirectory:
    directory: str


def input_key(value):
    return value.directory if isinstance(value, HeaderDirectory) else value


def archive_link_status(members, removed=()):
    """Resolve links inside an archive after the Bazel overlay removes files."""
    removed = set(removed)
    statuses = {}
    for name, (kind, link) in members.items():
        if kind not in {'symlink', 'hardlink'} or name in removed:
            continue
        path = name
        seen = set()
        for _ in range(64):
            if path.startswith('/') or path == '..' or path.startswith('../'):
                status = 'outside'
                break
            if path in seen:
                status = 'cycle'
                break
            seen.add(path)
            parts = path.split('/')
            replaced = False
            for count in range(1, len(parts) + 1):
                prefix = '/'.join(parts[:count])
                if prefix in removed:
                    status = 'removed'
                    break
                member = members.get(prefix)
                if member is None:
                    status = 'missing'
                    break
                node_kind, target = member
                if node_kind in {'symlink', 'hardlink'}:
                    if node_kind == 'symlink':
                        target = posixpath.join(posixpath.dirname(prefix), target)
                    path = posixpath.normpath(posixpath.join(target, *parts[count:]))
                    replaced = True
                    break
                if count != len(parts) and node_kind != 'directory':
                    status = 'not-directory'
                    break
                if count == len(parts):
                    status = node_kind
            if not replaced:
                break
        else:
            status = 'cycle'
        statuses[name] = status
    return statuses


@cache
def archived_sources(owner):
    """Match the pinned http_archive, including Git export-ignore attributes."""
    spec = REPOSITORIES[owner]
    with subprocess.Popen(['git', '-C', str(ROOT / owner), 'archive', '--format=tar',
                           spec['commit']], stdout=subprocess.PIPE) as process:
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            members = {}
            for member in archive:
                kind = ('file' if member.isfile() else 'directory' if member.isdir()
                        else 'symlink' if member.issym() else 'hardlink' if member.islnk()
                        else 'nonregular')
                members[member.name.rstrip('/')] = (kind, member.linkname)
        if process.wait():
            raise ValueError('Cannot list pinned repository ' + owner + ' at ' + spec['commit'])
    removed = set(spec['remove_build_files']) | {'.bazelignore'}
    resolved = archive_link_status(members, removed)
    return tuple(sorted(
        name for name, (kind, _) in members.items()
        if name not in removed and Path(name).name not in {'BUILD', 'BUILD.bazel'}
        and (kind == 'file' or resolved.get(name) == 'file')
    ))


@cache
def directory_sources(directory):
    """List CMake source inputs; native generated BUILD files are not CMake inputs."""
    owner = repository(directory)
    if owner:
        relative = directory.removeprefix(owner).lstrip('/')
        prefix = relative + '/' if relative else ''
        return tuple(owner + '/' + name for name in archived_sources(owner)
                     if name.startswith(prefix))
    result = []
    start = ROOT / directory
    from initial_migration.first_party_packages import native_directory
    if (start != ROOT and not native_directory(directory)
            and any((start / build).is_file() for build in ('BUILD', 'BUILD.bazel'))):
        return ()
    for parent, dirs, names in os.walk(start):
        # The initial CMake capture precedes native package rendering. Still
        # respect Bazel infrastructure and independent Rust packages.
        dirs[:] = sorted(d for d in dirs
                         if native_directory((Path(parent) / d).relative_to(ROOT).as_posix())
                         or not any((Path(parent) / d / build).is_file()
                                    for build in ('BUILD', 'BUILD.bazel')))
        for name in sorted(names):
            if name not in {'BUILD', 'BUILD.bazel'} and (Path(parent) / name).is_file():
                result.append((Path(parent) / name).relative_to(ROOT).as_posix())
    return tuple(sorted(result))


@cache
def package_sources(owner):
    """List files for the initial repository-wide CMake capture."""
    if owner:
        # The archive's BUILD files are removed before Bazel adds the overlay.
        return (*archived_sources(owner), 'BUILD.bazel')
    names = subprocess.check_output(
        ['git', '-C', str(ROOT), 'ls-files', '-z', '--cached', '--others', '--exclude-standard'],
    ).decode().split('\0')
    from initial_migration.first_party_packages import native_directory
    packages = {
        name.rsplit('/', 1)[0] for name in names
        if '/' in name and Path(name).name in {'BUILD', 'BUILD.bazel'}
        and not native_directory(name.rsplit('/', 1)[0])
    }
    return tuple(name for name in names if name
                 and Path(name).name not in {'BUILD', 'BUILD.bazel'}
                 and not any(name.startswith(p + '/') for p in packages))


@cache
def package_source_files(owner):
    return frozenset(package_sources(owner))


def included_source_files(owner, group):
    """Resolve literal C #includes or NASM %includes within a pinned repository."""
    nasm = group['language'] == 'ASM_NASM'
    prefix = owner + '/'
    files = package_source_files(owner)
    flags = group['copts']
    includes = [arg.removeprefix(prefix) for flag, arg in zip(flags, flags[1:])
                if flag in {'-I', '-isystem'} and arg.startswith(prefix)]
    pending = [source_path(label).removeprefix(prefix) for label in group['srcs']]
    visited, included = set(), set()
    while pending:
        source = pending.pop()
        if source in visited:
            continue
        visited.add(source)
        text = subprocess.check_output(
            ['git', '-C', str(ROOT / owner), 'show', REPOSITORIES[owner]['commit'] + ':' + source],
        ).decode()
        for line in text.splitlines():
            if nasm:
                line = line.partition(';')[0].strip()
                if not re.search(r'%\s*(?:include|depend|pathsearch)\b|\bincbin\b', line, re.I):
                    continue
                literal = re.fullmatch(r'''%\s*include\s+(["'])([^"']+)\1''', line, re.I)
                directories = ['', *includes]
            else:
                if not re.match(r'^\s*#\s*include\b', line):
                    continue
                literal = re.match(r'^\s*#\s*include\s*(["<])([^">]+)[">]', line)
                directories = [posixpath.dirname(source), *includes] if literal and literal[1] == '"' else includes
            if not literal:
                raise ValueError(f'{owner}/{source}: unsupported include: {line}')
            operand = literal[2]
            path = next((candidate for directory in directories
                         if (candidate := posixpath.normpath(posixpath.join(directory, operand))) in files), None)
            if path is None:
                if nasm:
                    raise ValueError(f'{owner}/{source}: NASM include not found: {operand}')
                continue  # C library and compiler headers come from the toolchain.
            included.add(path)
            pending.append(path)
    return sorted(included)


@cache
def textual_source_names(owner):
    """Names present in literal includes of a pinned repository.

    Compare basenames conservatively: an include in another directory keeps
    every source of that name. Callers must also retain TEXTUAL_SOURCE_SUFFIXES
    for macro-computed includes.
    """
    suffixes = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm',
                *TEXTUAL_SOURCE_SUFFIXES)
    directive = re.compile(r'(?m)^[ \t]*[%#][ \t]*(include_next|include|import|embed)\b([^\r\n]*)')
    assembly = re.compile(r'(?mi)^[ \t]*\.(include|incbin)\b([^\r\n]*)')
    literal = re.compile(r"""^[ \t]*(?:"([^"\n]+)"|<([^>\n]+)>|'([^'\n]+)')(.*)$""")
    included = set()

    def scan(path, raw):
        text = raw.decode('utf-8', errors='replace')
        text = text.replace('\\\r\n', '').replace('\\\n', '')
        expressions = (directive, assembly) if path.lower().endswith(('.s', '.asm', '.s.in', '.asm.in')) else (directive,)
        for expression in expressions:
            for match in expression.finditer(text):
                operand = match[2].strip()
                parsed = literal.match(operand)
                if parsed:
                    included.add(Path(next(s for s in parsed.groups()[:3] if s is not None)).name)

    if owner is None:
        paths = subprocess.check_output(
            ['git', 'ls-files', '-z', '--', 'src', 'base', 'programs', 'contrib/*-cmake/**',
             'bazel/config', 'bazel/rules', 'bazel/initial_migration'], cwd=ROOT,
        ).decode().split('\0')
        for path in paths:
            source = ROOT / path
            if path.endswith(suffixes) and source.is_file():
                scan(path, source.read_bytes())
        return frozenset(included)
    spec = REPOSITORIES[owner]
    with subprocess.Popen(['git', '-C', str(ROOT / owner), 'archive', '--format=tar',
                           spec['commit']], stdout=subprocess.PIPE) as process:
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            for member in archive:
                if not member.isfile() or not member.name.endswith(suffixes):
                    continue
                scan(member.name, archive.extractfile(member).read())
        if process.wait():
            raise ValueError('Cannot scan pinned includes for ' + owner + ' at ' + spec['commit'])
    return frozenset(included)



def repository(path):
    prefix = '/'.join(path.split('/')[:2])
    return prefix if prefix in REPOSITORIES else None


def configuration_owner(path):
    prefix = '/'.join(path.split('/')[:2]).removesuffix('-cmake')
    if prefix not in REPOSITORIES:
        raise ValueError('No repository owns configured input: ' + path)
    return prefix


def source_label(path):
    if path.startswith('bazel/gen/') and path.split('/', 3)[-1] == 'contrib/krb5-cmake/include/krb5/krb5.h':
        return '@clickhouse_krb5//:config/contrib/krb5-cmake/include/krb5/krb5.h'
    if path.startswith('bazel/config/'):
        _, _, arch, relative = path.split('/', 3)
        if relative.startswith('contrib/'):
            owner = configuration_owner(relative)
            return '@' + REPOSITORIES[owner]['name'] + '//:config/' + arch + '/' + relative
    prefix = repository(path)
    if prefix:
        relative = path.removeprefix(prefix + '/')
        if REPOSITORIES[prefix].get('module') == 'llvm':
            if '/' in relative:
                project, rest = relative.split('/', 1)
                return '@llvm-project//' + project + ':' + rest
            return '@llvm-project//:' + relative
        EXPORTS[prefix].add(relative)
        return '@' + REPOSITORIES[prefix]['name'] + '//:' + relative
    if path.startswith('rust/workspace/'):
        return '//rust/workspace:' + path.removeprefix('rust/workspace/')
    if path.startswith('bazel/initial_migration/'):
        return '//bazel/initial_migration:' + path.removeprefix('bazel/initial_migration/')
    if path.startswith('bazel/rules/'):
        return '//bazel/rules:' + path.removeprefix('bazel/rules/')
    if path.startswith('bazel/'):
        return '//bazel:' + path.removeprefix('bazel/')
    return '//:' + path


def source_path(label):
    for prefix, spec in REPOSITORIES.items():
        repo_label = '@' + spec['name'] + '//:'
        if label.startswith(repo_label):
            return prefix + '/' + label.removeprefix(repo_label)
    main_label = label.removeprefix('@clickhouse')
    if main_label.startswith('//') and ':' in main_label:
        package, name = main_label[2:].split(':', 1)
        return (package + '/' if package else '') + name
    return label.removeprefix('//:')


def comparison_root(prefix):
    # any2bazel stores concrete action paths. This is the root module's
    # extension identity under the pinned Bazel version; build rules themselves
    # obtain paths from Label.workspace_root in repository_paths.bzl.
    if REPOSITORIES[prefix].get('module') == 'llvm':
        return 'external/llvm++llvm+llvm-project'
    return 'external/+contrib+' + REPOSITORIES[prefix]['name']


_SOURCE_ROOT = re.compile(
    r'(?:(?<=\$\$execroot/)|(?<=\$\(GENDIR\)/)|(?<![\w./:@-]))('
    + '|'.join(re.escape(p) for p in sorted(REPOSITORIES, key=len, reverse=True))
    + r')(?=/|$|[\s\"\'])'
)
_SHELL_SOURCE_ROOT = re.compile(r'(?<=\$\$execroot/)(' + '|'.join(
    re.escape(p) for p in sorted(REPOSITORIES, key=len, reverse=True)) + r')(?=/|$|[\s\"\'])')


def execution_path(value, shell=False, raw_llvm=False):
    """Render source paths using Bazel's repository mapping, never canonical names."""
    if value.startswith(('-ffile-prefix-map=', '-fdebug-prefix-map=', '-fmacro-prefix-map=')):
        option, original, replacement = value.split('=', 2)
        return (json.dumps(option + '=') + ' + ' + execution_path(original, shell=shell, raw_llvm=raw_llvm)
                + ' + ' + json.dumps('=' + replacement))
    if value.startswith('bazel/config/'):
        _, _, arch, relative = value.split('/', 3)
        if relative.startswith('contrib/'):
            owner = configuration_owner(relative)
            return ('"$(GENDIR)/" + repository_path(' + json.dumps(owner) + ') + '
                    + json.dumps('/config/' + arch + '/' + relative))
    parts = []
    offset = 0
    for match in (_SHELL_SOURCE_ROOT if shell else _SOURCE_ROOT).finditer(value):
        if match.start() > offset:
            parts.append(json.dumps(value[offset:match.start()]))
        if (not raw_llvm and match[0] == 'contrib/llvm-project' and re.match(
                r'/(libc(?:xx(?:abi)?)?|libunwind|compiler-rt)(?:/|$)',
                value[match.end():])):
            parts.append('llvm_toolchain_path()')
        else:
            parts.append('repository_path(' + json.dumps(match[0]) + ')')
        offset = match.end()
    if not parts:
        return json.dumps(value)
    if offset < len(value):
        parts.append(json.dumps(value[offset:]))
    return ' + '.join(parts)


def cmake_has_source_prefix_map(fragments, source_root):
    """Check the CMake map before translating Bazel repository names."""
    flags = [flag for fragment in fragments
             for flag in shlex.split(fragment['fragment'])
             if flag.startswith('-ffile-prefix-map=')]
    expected = '-ffile-prefix-map=' + str(source_root) + '/='
    if any(flag != expected for flag in flags):
        raise ValueError('Unexpected CMake file prefix map: ' + repr(flags))
    return bool(flags)


def write_module():
    (ROOT / 'bazel/contrib.MODULE.bazel').write_text(
        'contrib = use_extension("//bazel/rules:repositories.bzl", "contrib")\n'
        'use_repo(\n    contrib,\n' + ''.join(
            '    ' + json.dumps(name) + ',\n' for name in sorted(
                [spec['name'] for spec in REPOSITORIES.values() if not spec.get('module')]
                + ['clickhouse_cargo_sources'])) + ')\n')


def write_overlays(builds, shared_inputs, system_repositories):
    write_module()
    # A source compiled within its own repository needs an explicit export only
    # when another target or generator consumes that file across the boundary.
    local_sources = set().union(*(build.local_sources for build in builds.values()))
    external_inputs = set().union(*(build.external_inputs for build in builds.values()))
    def collect(value):
        if isinstance(value, dict):
            for item in value.values():
                collect(item)
        elif isinstance(value, list):
            for item in value:
                collect(item)
        elif isinstance(value, str) and value.startswith('@'):
            external_inputs.add(value)
    collect(shared_inputs)
    # A hand-written .bzl rule can consume a file directly without going
    # through BuildFile.label(), for example OpenSSL and Delta codegen inputs.
    # Parse complete strings: Bazel source file labels may contain spaces or
    # brackets. An incomplete label stem keeps the affected repo's exports.
    dynamic_repositories = set()
    for path in (ROOT / 'bazel').rglob('*.bzl'):
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.Constant) and isinstance(node.value, str):
                value = node.value
                if value.startswith('@clickhouse_') and '//:' in value:
                    repo, _, name = value.partition('//:')
                    if not name or '{' in name or '$' in name:
                        dynamic_repositories.add(repo)
                    else:
                        external_inputs.add(value)
    for label in local_sources - external_inputs:
        for prefix, spec in REPOSITORIES.items():
            repository_label = '@' + spec['name'] + '//:'
            if label.startswith(repository_label):
                EXPORTS[prefix].discard(label.removeprefix(repository_label))
                break
    # Graph construction visits source directories before cc_library header
    # dependencies replace the captured include directories. Export only files
    # still used across repositories (not those transient source_label calls).
    for prefix in builds:
        if prefix not in REPOSITORIES:
            continue
        repo = '@' + REPOSITORIES[prefix]['name']
        if repo in dynamic_repositories:
            continue
        label_prefix = repo + '//:'
        EXPORTS[prefix].intersection_update(
            value.removeprefix(label_prefix) for value in external_inputs if value.startswith(label_prefix)
        )
    directory = ROOT / 'bazel/dependencies'
    directory.mkdir(exist_ok=True)
    EXPORTS['contrib/delta-kernel-rs'].update(['ffi/Cargo.toml', 'ffi/cbindgen.toml'])
    EXPORTS['contrib/openssl'].add('include/openssl/ssl.h')
    (directory / 'BUILD.bazel').write_text('exports_files(glob(["*.BUILD.bazel"]))\n')
    for prefix, spec in REPOSITORIES.items():
        if spec.get('module'):
            continue
        build = builds.get(prefix)
        text = build.load_statements() if build else ''
        features = ', features = ["external_include_paths"]' if prefix in system_repositories else ''
        text += 'package(default_visibility = ["//visibility:public"]' + features + ')\n'
        exports = EXPORTS[prefix] - {'BUILD', 'BUILD.bazel'}
        if exports:
            text += 'exports_files(' + json.dumps(sorted(exports), indent=2) + ')\n'
        if build:
            text += '\n' + build.text()
        (directory / (spec['name'] + '.BUILD.bazel')).write_text(text)
    roots = {path: '@' + spec['name'] + '//:BUILD.bazel' for path, spec in REPOSITORIES.items()}
    (ROOT / 'bazel/rules/repository_labels.bzl').write_text(
        'REPOSITORIES = {\n' + ''.join(
            '    ' + json.dumps(path) + ': Label(' + json.dumps(label) + '),\n'
            for path, label in roots.items()) + '}\n')
