"""Map reference-checkout inputs to pinned Bazel source repositories."""

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

ROOT = Path(__file__).resolve().parents[1]
REPOSITORIES = json.loads((ROOT / 'bazel/contrib_repositories.json').read_text())
EXPORTS = {path: set() for path in REPOSITORIES}
# rules_cc's allowed header extensions (cc/private/cc_helper.bzl).
CC_HEADER_SUFFIXES = ('.h', '.hh', '.hpp', '.ipp', '.hxx', '.h++',
                      '.inc', '.inl', '.tlh', '.tli', '.H', '.tcc')


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
    """List the same source inputs as Bazel glob, respecting root packages."""
    owner = repository(directory)
    if owner:
        relative = directory.removeprefix(owner).lstrip('/')
        prefix = relative + '/' if relative else ''
        return tuple(owner + '/' + name for name in archived_sources(owner)
                     if name.startswith(prefix))
    result = []
    start = ROOT / directory
    if start != ROOT and any((start / build).is_file() for build in ('BUILD', 'BUILD.bazel')):
        return ()
    for parent, dirs, names in os.walk(start):
        # Each child BUILD declares a Bazel package that a parent glob cannot
        # traverse. contrib http_archives instead remove their BUILD files.
        dirs[:] = sorted(d for d in dirs
                         if not any((Path(parent) / d / build).is_file()
                                    for build in ('BUILD', 'BUILD.bazel')))
        for name in sorted(names):
            if name not in {'BUILD', 'BUILD.bazel'} and (Path(parent) / name).is_file():
                result.append((Path(parent) / name).relative_to(ROOT).as_posix())
    return tuple(sorted(result))


@cache
def textual_source_names(owner):
    """Names present in literal includes of a pinned repo, or None for macros.

    Compare basenames conservatively: even an include in another directory
    keeps every source of that name. Unresolved includes keep every source.
    """
    spec = REPOSITORIES[owner]
    suffixes = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm',
                *CC_HEADER_SUFFIXES, '.def', '.in')
    directive = re.compile(r'(?m)^[ \t]*#[ \t]*(include_next|include|import|embed)\b([^\r\n]*)')
    literal = re.compile(r'^[ \t]*(?:"([^"\n]+)"|<([^>\n]+)>)(.*)$')
    included = set()
    unresolved = False
    with subprocess.Popen(['git', '-C', str(ROOT / owner), 'archive', '--format=tar',
                           spec['commit']], stdout=subprocess.PIPE) as process:
        with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
            for member in archive:
                if not member.isfile() or not member.name.endswith(suffixes):
                    continue
                text = archive.extractfile(member).read().decode('utf-8', errors='replace')
                text = text.replace('\\\r\n', '').replace('\\\n', '')
                for match in directive.finditer(text):
                    operand = match[2].strip()
                    parsed = literal.match(operand)
                    if parsed:
                        included.add(Path(parsed[1] or parsed[2]).name)
                    elif operand and not operand.startswith(('//', '/*')):
                        unresolved = True
        if process.wait():
            raise ValueError('Cannot scan pinned includes for ' + owner + ' at ' + spec['commit'])
    return None if unresolved else frozenset(included)



def repository(path):
    prefix = '/'.join(path.split('/')[:2])
    return prefix if prefix in REPOSITORIES else None


def configuration_owner(path):
    prefix = '/'.join(path.split('/')[:2]).removesuffix('-cmake')
    if prefix not in REPOSITORIES:
        raise ValueError('No repository owns configured input: ' + path)
    return prefix


def source_label(path):
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
    if path.startswith('bazel/'):
        return '//bazel:' + path.removeprefix('bazel/')
    return '//:' + path


def source_path(label):
    for prefix, spec in REPOSITORIES.items():
        repo_label = '@' + spec['name'] + '//:'
        if label.startswith(repo_label):
            return prefix + '/' + label.removeprefix(repo_label)
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
        '# Generated by bazel/generate_build.py.\n'
        'contrib = use_extension("//bazel:repositories.bzl", "contrib")\n'
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
    for label in local_sources - external_inputs:
        for prefix, spec in REPOSITORIES.items():
            repository_label = '@' + spec['name'] + '//:'
            if label.startswith(repository_label):
                EXPORTS[prefix].discard(label.removeprefix(repository_label))
                break
    directory = ROOT / 'bazel/dependencies'
    directory.mkdir(exist_ok=True)
    EXPORTS['contrib/delta-kernel-rs'].add('ffi/Cargo.toml')
    EXPORTS['contrib/openssl'].add('include/openssl/ssl.h')
    (directory / 'BUILD.bazel').write_text('exports_files(glob(["*.BUILD.bazel"]))\n')
    for prefix, spec in REPOSITORIES.items():
        if spec.get('module'):
            continue
        text = '# Generated by bazel/generate_build.py.\n'
        build = builds.get(prefix)
        if build:
            text += build.load_statements()
        features = ', features = ["external_include_paths"]' if prefix in system_repositories else ''
        text += 'package(default_visibility = ["//visibility:public"]' + features + ')\n'
        text += 'exports_files(' + json.dumps(sorted(EXPORTS[prefix] | {'BUILD.bazel'}), indent=2) + ')\n'
        if build:
            text += '\n' + build.text()
        (directory / (spec['name'] + '.BUILD.bazel')).write_text(text)
    roots = {path: '@' + spec['name'] + '//:BUILD.bazel' for path, spec in REPOSITORIES.items()}
    (ROOT / 'bazel/repository_labels.bzl').write_text(
        '# Generated by bazel/generate_build.py.\nREPOSITORIES = {\n' + ''.join(
            '    ' + json.dumps(path) + ': Label(' + json.dumps(label) + '),\n'
            for path, label in roots.items()) + '}\n')
