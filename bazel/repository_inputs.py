"""Map reference-checkout inputs to pinned Bazel source repositories."""

import hashlib
import json
from pathlib import Path
import re
import shlex

ROOT = Path(__file__).resolve().parents[1]
REPOSITORIES = json.loads((ROOT / 'bazel/contrib_repositories.json').read_text())
EXPORTS = {path: set() for path in REPOSITORIES}
HEADER_GROUPS = {path: {} for path in REPOSITORIES}


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


def header_group(directory):
    prefix = repository(directory)
    if not prefix:
        return None
    name = 'headers_' + hashlib.sha256(directory.encode()).hexdigest()[:16]
    HEADER_GROUPS[prefix][name] = directory.removeprefix(prefix).lstrip('/')
    return '@' + REPOSITORIES[prefix]['name'] + '//:' + name


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
        if prefix == 'contrib/lz4':
            text += 'load("@clickhouse//bazel:lz4_cmake.bzl", "declare_lz4_cmake_objects")\n'
        features = ', features = ["external_include_paths"]' if prefix in system_repositories else ''
        text += 'package(default_visibility = ["//visibility:public"]' + features + ')\n'
        text += 'exports_files(' + json.dumps(sorted(EXPORTS[prefix] | {'BUILD.bazel'}), indent=2) + ')\n'
        for name, relative in sorted(HEADER_GROUPS[prefix].items()):
            pattern = relative + '/**' if relative else '**'
            text += ('filegroup(\n    name = ' + json.dumps(name) + ',\n'
                     '    srcs = glob([' + json.dumps(pattern) + '], '
                     'exclude = ["**/BUILD", "**/BUILD.bazel"], allow_empty = True),\n)\n')
        if build:
            text += '\n' + build.text()
        if prefix == 'contrib/lz4':
            text += 'declare_lz4_cmake_objects()\n'
        (directory / (spec['name'] + '.BUILD.bazel')).write_text(text)
    roots = {path: '@' + spec['name'] + '//:BUILD.bazel' for path, spec in REPOSITORIES.items()}
    (ROOT / 'bazel/repository_paths.bzl').write_text(
        '# Generated by bazel/generate_build.py.\n_REPOSITORIES = {\n' + ''.join(
            '    ' + json.dumps(path) + ': Label(' + json.dumps(label) + '),\n'
            for path, label in roots.items()) + '}\n\n'
        'def repository_path(path):\n'
        '    return _REPOSITORIES[path].workspace_root\n\n'
        'def llvm_toolchain_path():\n'
        '    return Label("@llvm-project//:BUILD.bazel").workspace_root\n\n'
        + _PREFIX_MAPS_BZL)


_PREFIX_MAPS_BZL = '''def cmake_prefix_maps(arches):
    """Use original CMake source names, and saved build names in metadata mode."""
    sources = [
        "-ffile-prefix-map=" + label.workspace_root + "/=" + path + "/"
        for path, label in _REPOSITORIES.items()
    ] + [
        "-ffile-prefix-map=" + llvm_toolchain_path() + "/" + name + "/=contrib/llvm-project/" + name + "/"
        for name in ["libc", "libcxx", "libcxxabi", "libunwind", "compiler-rt"]
    ] + [
        "-ffile-prefix-map=$(GENDIR)/" + llvm_toolchain_path() + "/" + name + "/" + name +
        "_headers_include_search_directory/=contrib/llvm-project/" + name + "/include/"
        for name in ["libcxx", "libcxxabi"]
    ]
    source_settings = {"//conditions:default": []}
    generated_settings = {"//conditions:default": []}
    embedded_settings = {"//conditions:default": []}
    for arch in arches:
        cpu = str(Label("@platforms//cpu:" + arch))
        source_settings[cpu] = sources
        build = "build_bazel_reference_" + arch + "/"
        generated = [
            "-ffile-prefix-map=bazel/config/" + arch + "/=" + build,
            "-ffile-prefix-map=$(GENDIR)/bazel/gen/" + arch + "/=" + build,
        ] + [
            "-ffile-prefix-map=$(GENDIR)/" + label.workspace_root + "/config/" + arch + "/=" + build
            for label in _REPOSITORIES.values()
        ] + [
            "-ffile-prefix-map=$(GENDIR)/" + repository_path("contrib/llvm-project") +
            "/gen/" + arch + "/=" + build + "contrib/llvm-project/",
        ]
        setting = str(Label("@clickhouse//bazel:use_cmake_metadata_" + arch))
        generated_settings[setting] = generated
        # #embed copies retain source-tree names even inside bazel/gen. Keep
        # this more specific map after the general generated-directory map.
        embedded_settings[cpu] = ["-ffile-prefix-map=$(GENDIR)/bazel/gen/" + arch + "/embedded/="]
    return select(source_settings) + select(generated_settings) + select(embedded_settings)

def cmake_native_compile_flags(groups):
    """Apply the recorded headers and the sysroot only for Clang compilations."""
    choices = {"//conditions:default": []}
    for arch, group in groups.items():
        options = []
        if group["language"] in ["C", "CXX", "ASM"]:
            gcc_root = repository_path("contrib/sysroot") + "/linux-" + arch
            options = [
                "--sysroot=" + gcc_root + "/" + arch + "-linux-gnu/libc",
                "--gcc-toolchain=" + gcc_root,
            ]
        if group["language"] == "CXX":
            # The module already supplies public libc++ headers. Do not let
            # the GCC sysroot add a second C++ standard library.
            options.append("-nostdinc++")
        for flag, kind, directory in group["includes"]:
            raw = repository_path("contrib/llvm-project")
            if kind == "generated":
                raw = "$(GENDIR)/" + raw + "/gen/" + arch
            options.extend([flag, raw + "/" + directory])
        choices[str(Label("@platforms//cpu:" + arch))] = options
    return select(choices)
'''
