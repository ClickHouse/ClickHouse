#!/usr/bin/env python3
"""Place native ClickHouse targets in the source directories that own them.

The CMake capture is repository-wide. This is its final BUILD rendering pass,
after external repositories and generated files have been assigned their own
repositories. This pass never edits CMake source files or the CMake link order.
"""
import argparse
import ast
from collections import Counter, defaultdict
from copy import deepcopy
from functools import cache
import json
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from initial_migration.compact_build import Source, compact
from initial_migration.repository_inputs import CC_HEADER_SUFFIXES, REPOSITORIES, ROOT
from initial_migration.simplify_globs import GlobFiles

CPUS = ('x86_64', 'aarch64')
COMPILERS = {'ch_cc_library', 'cc_library', 'cc_test', 'cc_binary'}
TUS = ('.c', '.cc', '.cpp', '.cxx', '.c++', '.C', '.s', '.S', '.asm', '.ASM')
TEXTUAL = ('.inc', '.inl', '.ipp', '.tpp', '.icc', '.def', '.defs', '.th', '.td',
           '.tbl', '.tbi', '.ixx', '.cuh', '.msh')
LABEL_FIELDS = {
    'srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs', 'deps',
    'implementation_deps', 'actual', 'tools', 'data', 'template', 'src', 'refs',
    'static_library', 'shared_library', 'interface_library', 'binary',
}
LOCATION = re.compile(r'\$\((locations?|execpaths?|rootpaths?)\s+([^\s)]+)\)')
MAIN_LABEL = re.compile(r'^(?P<repo>@clickhouse)?//(?P<pkg>[^:]*):(?P<name>[^\s]*)$')


def quoted(value):
    return ast.Constant(value=value)


def expr(value):
    return ast.parse(value, mode='eval').body


def list_expr(values):
    return ast.List(elts=[quoted(value) for value in values], ctx=ast.Load())


def render(node):
    return ast.unparse(ast.fix_missing_locations(node))


def call_name(node):
    return getattr(node.func, 'id', '') if isinstance(node, ast.Call) else ''


def attributes(node):
    return {kw.arg: kw.value for kw in node.keywords}


def logical_name(name):
    return re.sub(r'__(?:compile|runtime_objects)_\d+$', '', name)


def physical(package, value):
    return posixpath.join(package, value) if package else value


def relative_label(package, name, current='', repo=''):
    if package == current and not repo:
        return ':' + name
    return repo + '//' + package + ':' + name


def git_inventory(root):
    """Native CMake inputs, regardless of generated BUILD package boundaries."""
    listing = subprocess.check_output(
        ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=root,
    ).decode().split('\0')
    return tuple(sorted(
        name for name in dict.fromkeys(listing)
        if name and PurePosixPath(name).name not in {'BUILD', 'BUILD.bazel'}
        and (root / name).is_file()
        and not any(name == owner or name.startswith(owner + '/') for owner in REPOSITORIES)
    ))


def native_directory(directory):
    return directory == 'src' or directory == 'base' or directory == 'programs' or (
        directory.startswith(('src/', 'base/', 'programs/')) or
        directory.startswith('contrib/') and directory.split('/')[1] not in {
            value.split('/')[1] for value in REPOSITORIES
        }
    )


class Build:
    def __init__(self, root, path, inventory, packages, text=None):
        self.path = path
        self.package = path.parent.relative_to(root).as_posix().removeprefix('.')
        self.text = path.read_text() if text is None else text
        self.source = Source(self.text)
        self.constants = {stmt.targets[0].id: stmt.value for stmt in self.source.module.body
                          if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1
                          and isinstance(stmt.targets[0], ast.Name)}
        self.inventory = tuple(
            posixpath.relpath(name, self.package or '.')
            for name in inventory if physical(self.package, '') == '' or
            name.startswith(self.package + '/')
            if not any(other and other != self.package and other.startswith(self.package + '/' if self.package else '')
                       and name.startswith(other + '/') for other in packages)
        )
        self.globs = GlobFiles(self.inventory)
        self.loads = [stmt for stmt in self.source.module.body
                      if isinstance(stmt, ast.Expr) and call_name(stmt.value) == 'load']

    def expand(self, node, stack=()):
        class Expand(ast.NodeTransformer):
            def visit_Name(visitor, name):
                if name.id in self.constants:
                    if name.id in stack:
                        raise ValueError('Cyclic constant: ' + name.id)
                    return self.expand(self.constants[name.id], (*stack, name.id))
                return name
        return Expand().visit(deepcopy(node))

    def glob_values(self, node):
        opts = attributes(node)
        if not node.args or set(opts) - {'exclude', 'allow_empty', 'exclude_directories'}:
            raise ValueError('Unsupported glob: ' + render(node))
        patterns = ast.literal_eval(node.args[0])
        excludes = ast.literal_eval(opts['exclude']) if 'exclude' in opts else []
        return tuple(physical(self.package, item) for item in self.globs.result(patterns, excludes))

    def values(self, node, cpu):
        if node is None:
            return []
        if isinstance(node, ast.Name) and node.id in self.constants:
            return self.values(self.constants[node.id], cpu)
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return [node.value]
        if isinstance(node, (ast.List, ast.Tuple)):
            return [item for elt in node.elts for item in self.values(elt, cpu)]
        if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
            return self.values(node.left, cpu) + self.values(node.right, cpu)
        if call_name(node) == 'glob':
            return ['//' + self.package + ':' + posixpath.relpath(file, self.package or '.')
                    for file in self.glob_values(node)]
        if call_name(node) == 'select' and isinstance(node.args[0], ast.Dict):
            table = {ast.literal_eval(k): v for k, v in zip(node.args[0].keys, node.args[0].values)}
            if set(table) <= {'@platforms//cpu:' + arch for arch in CPUS} | {'//conditions:default'}:
                return self.values(table.get('@platforms//cpu:' + cpu, table.get('//conditions:default')), cpu)
            # Metadata choices can select alternate generated files. Preserve
            # all branches separately in validation, never infer a file order.
            return [value for key, branch in table.items() for value in self.values(branch, cpu)]
        return []


class Migration:
    def __init__(self, root, stage='all', from_generator=False):
        self.root = root
        self.stage = stage
        self.from_generator = from_generator
        self.files = git_inventory(root)
        self.file_set = set(self.files)
        self.existing = sorted({
            path.parent.relative_to(root).as_posix().removeprefix('.')
            for name in subprocess.check_output(
                ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
                 '--', 'BUILD', 'BUILD.bazel', '**/BUILD', '**/BUILD.bazel'], cwd=root,
            ).decode().split('\0')
            if name and (path := root / name).is_file()
        })
        main_packages = [p for p in self.existing if not p or native_directory(p)]
        # Regeneration rendered a new flat CMake graph into root BUILD; the old
        # generated packages must not hide native files from that capture.
        if from_generator:
            main_packages = ['']
        self.builds = [Build(root, root / pkg / 'BUILD.bazel', self.files,
                             [p for p in self.existing if not native_directory(p)] if from_generator else self.existing)
                       for pkg in main_packages]
        self.root_build = next(build for build in self.builds if not build.package)
        self.packages = set(self.existing)
        self.target_packages = {}
        self.aliases = {}
        self.exports = defaultdict(set)
        self.header_packages = set()
        self.rows = defaultdict(list)
        self.loads = defaultdict(dict)
        self.stats = Counter()
        self.compiles = {}
        self.textual_names = set()
        self.needed_rules = {}
        self._identify()

    def selected(self, package):
        if self.stage == 'base':
            return package.startswith(('base/', 'contrib/')) or package == 'base'
        return native_directory(package)

    @cache
    def nearest(self, filename):
        name = physical('', filename)
        directory = posixpath.dirname(name)
        while directory:
            if (self.root / directory / 'CMakeLists.txt').is_file():
                return directory
            directory = posixpath.dirname(directory)
        return ''

    def source_filename(self, value, old_package):
        if value.startswith(':'):
            value = '//' + old_package + value
        if not value.startswith(('@', '//')):
            value = '//' + old_package + ':' + value
        m = MAIN_LABEL.match(value)
        if not m:
            return None
        name = physical(m['pkg'], m['name'])
        if name.startswith('bazel/gen/'):
            # Copies for #embed preserve first-party source filenames.
            m = re.match(r'bazel/gen/(?:x86_64|aarch64)/embedded/(.*)', name)
            return m[1] if m else None
        return name if name in self.file_set else None

    def _identify(self):
        candidates = defaultdict(set)
        for build in self.builds:
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                kind = call_name(call)
                if not build.package and kind == 'alias':
                    actual = attrs.get('actual')
                    if isinstance(actual, ast.Constant) and isinstance(actual.value, str) and actual.value.startswith('@'):
                        self.aliases[name] = actual.value
                        continue
                if kind not in COMPILERS:
                    continue
                key = (build.package, logical_name(name))
                for cpu in CPUS:
                    for value in build.values(attrs.get('srcs'), cpu):
                        src = self.source_filename(value, build.package)
                        if src and src.endswith(TUS):
                            candidates[key].add(src)
                if build.package:
                    self.target_packages[build.package, name] = build.package
        # Determine owners for a logical library, not one flag variant at a
        # time. Generated-only variants stay beside their ordinary sources.
        owners = {}
        for key, values in candidates.items():
            source_dirs = [posixpath.dirname(value) for value in values]
            common = posixpath.commonpath(source_dirs)
            owner = self.nearest(physical(common, '_source_.cc'))
            owners[key] = owner if self.selected(owner) else key[0]
        for build in self.builds:
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                if not build.package and name in self.aliases:
                    continue
                package = owners.get((build.package, logical_name(name)), build.package)
                # Published Poco include interfaces have no compiled files.
                if name.startswith('poco_') and name.endswith('_headers'):
                    part = {
                        'foundation': 'Foundation', 'net': 'Net', 'net_ssl': 'NetSSL_OpenSSL',
                        'json': 'JSON', 'xml': 'XML', 'redis': 'Redis', 'util': 'Util',
                    }.get(name[len('poco_'):-len('_headers')])
                    if part:
                        package = 'base/poco/' + part
                if name in {'arrow_cmake_headers', 'librdkafka_cmake_headers', 'openssl_cmake_headers'}:
                    package = 'contrib/' + name.split('_')[0] + '-cmake'
                if name == 'clickhouse' or name == 'unit_tests_dbms':
                    package = ''
                if not self.selected(package):
                    package = build.package
                self.target_packages[build.package, name] = package
                self.packages.add(package)
        # Existing globs for source wrappers belong in each wrapper directory,
        # even if the directory contains only installed/configuration headers.
        for build in self.builds:
            for node in ast.walk(build.source.module):
                if call_name(node) != 'glob':
                    continue
                for file in build.glob_values(node):
                    parts = file.split('/')
                    if len(parts) < 3:
                        continue
                    if parts[0] == 'contrib' and '/'.join(parts[:2]) not in REPOSITORIES:
                        package = '/'.join(parts[:2])
                        if self.selected(package):
                            self.packages.add(package)
        self.packages = {p for p in self.packages if p is not None}
        # These declarations were hidden in declare_inputs() at the root.
        # Their files already have natural owners; Rust consumes DefaultInfo.
        self.aliases.update({
            'linux_aarch64_hwcap': '@clickhouse_sysroot//:linux_aarch64_hwcap',
            'krb5_gen_map': '@clickhouse_krb5//:gen_map',
            'krb5_gen': '@clickhouse_krb5//:gen',
            'cmake_cargo_config': '//contrib/corrosion-cmake:config.toml.in',
        })
        self.target_packages['', 'delta_cbindgen_sources'] = 'contrib/corrosion-cmake'
        self.target_packages['', 'openssl_configured_headers'] = 'contrib/openssl-cmake'
        self.packages.update({'contrib/corrosion-cmake', 'contrib/openssl-cmake'})

    @cache
    def file_package(self, filename):
        packages = (p for p in self.packages if p and filename.startswith(p + '/'))
        return max(packages, key=len, default='')

    @cache
    def mapped(self, value, current, old_package='', cross_repository=False, source_file=True):
        """Resolve main labels, record exact cross-package file exports."""
        if value.startswith(':'):
            value = '//' + old_package + value
        elif not value.startswith(('@', '//')):
            if not source_file:
                return value
            value = '//' + old_package + ':' + value
        m = MAIN_LABEL.fullmatch(value)
        if not m:
            return value
        repo = m['repo'] or ('@clickhouse' if cross_repository else '')
        pkg, name = m['pkg'], m['name']
        if not pkg and name in self.aliases:
            self.stats['removed_alias_references'] += 1
            actual = self.aliases[name]
            if actual.startswith('//'):
                return self.mapped(actual, current, '', cross_repository)
            return actual
        target = self.target_packages.get((pkg, name))
        if target is not None:
            return relative_label(target, name, current, repo)
        file = physical(pkg, name)
        if file in self.file_set:
            owner = self.file_package(file)
            relative = file.removeprefix(owner + '/') if owner else file
            if owner != current or repo:
                self.exports[owner].add(relative)
                return relative_label(owner, relative, current, repo)
            return relative
        return relative_label(pkg, name, current, repo)

    def location(self, value, current, old_package='', cross_repository=False):
        return LOCATION.sub(
            lambda m: '$(' + m[1] + ' ' + self.mapped(m[2], current, old_package, cross_repository) + ')',
            value,
        )

    def file_values(self, build, node, current, field, headers):
        """Rewrite one original glob after declaring new package boundaries."""
        values = build.glob_values(node)
        selected = []
        same_package_glob = build.package == current and field in {'srcs', 'hdrs', 'textual_hdrs'}
        for file in values:
            owner = self.file_package(file)
            suffix = PurePosixPath(file).suffix
            if field in {'hdrs', 'textual_hdrs'} and file.endswith((*CC_HEADER_SUFFIXES, *TEXTUAL)):
                if owner != current:
                    self.header_packages.add(owner)
                    headers.add(owner)
                    continue
            if field == 'additional_compiler_inputs':
                # Never feed unrelated source/metadata files to C++ actions.
                # Kept textual translation units are discovered by includes.
                if suffix not in (*CC_HEADER_SUFFIXES, *TEXTUAL) and (
                        not file.endswith(TUS) or PurePosixPath(file).name not in self.textual_names):
                    self.stats['dropped_metadata_or_unincluded_tus'] += 1
                    continue
                if owner != current and file.endswith((*CC_HEADER_SUFFIXES, *TEXTUAL)):
                    self.header_packages.add(owner)
                    headers.add(owner)
                    continue
            if not (same_package_glob and owner == current):
                old_name = file.removeprefix(build.package + '/') if build.package else file
                selected.append(self.mapped('//' + build.package + ':' + old_name, current, build.package))
        if same_package_glob:
            # The glob automatically stops at new child packages. Preserve
            # the original file order in srcs and the original exclusions.
            if field == 'srcs' and selected:
                return list_expr([self.mapped('//' + build.package + ':' +
                                               (file.removeprefix(build.package + '/') if build.package else file),
                                               current, build.package) for file in values])
            if selected:
                return ast.BinOp(left=node, op=ast.Add(), right=list_expr(selected))
            return node
        # Local globs remain true globs when the old expression names only
        # files owned by the destination; transform literal patterns by prefix.
        if (values and len(selected) == len(values) and
                all(self.file_package(file) == current for file in values)):
            prefix = posixpath.relpath(build.package or '.', current or '.')
            opts = attributes(node)
            patterns = ast.literal_eval(node.args[0])
            excluded = ast.literal_eval(opts['exclude']) if 'exclude' in opts else []
            combined = [posixpath.normpath(posixpath.join(prefix, p)) for p in (*patterns, *excluded)]
            if all(not p.startswith('../') and p != '..' for p in combined):
                args = list_expr(combined[:len(patterns)])
                kwargs = [ast.keyword(arg='exclude', value=list_expr(combined[len(patterns):]))] if excluded else []
                return ast.Call(func=ast.Name(id='glob', ctx=ast.Load()), args=[args], keywords=kwargs)
        return list_expr(selected)

    def transform(self, node, build, current, field, headers):
        node = build.expand(node)

        class Visitor(ast.NodeTransformer):
            def visit_Call(visitor, call):
                if call_name(call) == 'glob':
                    return self.file_values(build, call, current, field, headers)
                if call_name(call) == 'select':
                    out = call
                    # Select keys are not source labels; only process values.
                    out.args[0].values = [visitor.visit(branch) for branch in call.args[0].values]
                    return out
                return visitor.generic_visit(call)

            def visit_Constant(visitor, item):
                if not isinstance(item.value, str):
                    return item
                value = item.value
                if field in LABEL_FIELDS:
                    value = self.mapped(value, current, build.package)
                else:
                    value = self.location(value, current, build.package)
                return quoted(value)
        out = Visitor().visit(node)
        if field in {'local_includes', 'includes'} and current != build.package:
            # Keep the same normalized directories and their order. The
            # rules_cc helper accepts .. within the execution root.
            prefix = posixpath.relpath(build.package or '.', current or '.') + '/'

            class Includes(ast.NodeTransformer):
                def visit_List(visitor, items):
                    return ast.List(elts=[
                        ast.BinOp(left=quoted(prefix), op=ast.Add(), right=elt)
                        for elt in items.elts
                    ], ctx=ast.Load())
            out = Includes().visit(out)
        return out

    def prepare(self):
        from initial_migration.repository_inputs import textual_source_names
        self.textual_names = textual_source_names(None)
        if any(call_name(stmt.value) == 'declare_inputs' for stmt in self.root_build.source.module.body
               if isinstance(stmt, ast.Expr)):
            self.rows['contrib/corrosion-cmake'].append(expr_stmt(
                'filegroup(name="delta_cbindgen_sources",srcs=['
                '"@clickhouse_delta_kernel_rs//:cbindgen_sources",'
                '"@clickhouse_rust_vendor//:sources","config.toml.in"])'))
            self.rows['contrib/openssl-cmake'].append(expr_stmt(
                'filegroup(name="openssl_configured_headers",srcs=glob(["common/include/**"]))'))
        for build in self.builds:
            for load in build.loads:
                key = render(load)
                for pkg in self.packages:
                    if not pkg or native_directory(pkg):
                        self.loads[pkg][key] = load
            for stmt, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                if not build.package and name in self.aliases:
                    continue
                current = self.target_packages[build.package, name]
                headers = set()
                kw = []
                for item in call.keywords:
                    if item.arg == 'name':
                        value = deepcopy(item.value)
                    else:
                        value = self.transform(item.value, build, current, item.arg, headers)
                    kw.append(ast.keyword(arg=item.arg, value=value))
                if headers:
                    deps = next((item for item in kw if item.arg == 'deps'), None)
                    refs = list_expr([relative_label(owner, 'headers', current) for owner in sorted(headers)])
                    if deps:
                        deps.value = ast.BinOp(left=deps.value, op=ast.Add(), right=refs)
                    else:
                        kw.append(ast.keyword(arg='deps', value=refs))
                rebuilt = ast.Expr(value=ast.Call(func=deepcopy(call.func), args=deepcopy(call.args), keywords=kw))
                self.rows[current].append(rebuilt)
                if current != build.package:
                    self.stats['moved_targets'] += 1
        for package in self.header_packages:
            self.rows[package].append(expr_stmt(
                'cc_library(name="headers", hdrs=glob(%s), textual_hdrs=glob(%s))' % (
                    json.dumps(['**/*' + suffix for suffix in CC_HEADER_SUFFIXES if suffix not in TEXTUAL]),
                    json.dumps(['**/*' + suffix for suffix in TEXTUAL]),
                )))
            self.loads[package]['headers'] = expr_stmt('load("@rules_cc//cc:cc_library.bzl", "cc_library")')
        data = self.rewrite_consumers()
        data.update(self.render_builds())
        self.verify(data)
        return data

    def normalize_source(self, value, package):
        if value.startswith(':'):
            return physical(package, value[1:])
        match = MAIN_LABEL.fullmatch(value)
        if match:
            return physical(match['pkg'], match['name'])
        return value if value.startswith('@') else physical(package, value)

    def verify(self, rendered):
        """Verify TU order and compile/link options on both target CPUs."""
        new_builds = {
            package: Build(self.root, self.root / package / 'BUILD.bazel',
                           self.files, self.packages,
                           rendered[self.root / package / 'BUILD.bazel'])
            for package in self.rows
        }
        rules = {}
        for package, build in new_builds.items():
            for _, call in build.source.rules():
                attrs = attributes(call)
                name = ast.literal_eval(attrs['name'])
                key = package, name
                if key in rules:
                    raise ValueError('Duplicate native declaration: ' + str(key))
                rules[key] = build, call, attrs
        for original in self.builds:
            for _, call in original.source.rules():
                before = attributes(call)
                name = ast.literal_eval(before['name'])
                if not original.package and name in self.aliases:
                    continue
                package = self.target_packages[original.package, name]
                rebuilt, new_call, after = rules[package, name]
                if call_name(call) != call_name(new_call):
                    raise ValueError('Native rule changed kind: ' + name)
                for cpu in CPUS:
                    for field in ('srcs',):
                        old_srcs = [self.normalize_source(value, original.package)
                                    for value in original.values(before.get(field), cpu)]
                        new_srcs = [self.normalize_source(value, rebuilt.package)
                                    for value in rebuilt.values(after.get(field), cpu)]
                        old_tus = [v for v in old_srcs if v.endswith(TUS)]
                        new_tus = [v for v in new_srcs if v.endswith(TUS)]
                        if old_tus != new_tus:
                            raise ValueError(f'{name} {cpu} changed ordered translation units: ' +
                                             repr((old_tus[:10], new_tus[:10])))
                for field in before.keys() - LABEL_FIELDS - {
                        'name', 'local_includes', 'includes',
                        'copts', 'linkopts', 'cmd', 'env'}:
                    if ast.dump(original.expand(before[field])) != ast.dump(rebuilt.expand(after[field])):
                        raise ValueError(f'{name} changed attribute {field}')
                self.stats['verified_native_rules'] += 1
        for arch in ('', '_aarch64'):
            path = self.root / ('bazel/rules/cmake_link_data' + arch + '.bzl')
            if path not in rendered:
                continue
            old = next(ast.literal_eval(stmt.value) for stmt in ast.parse(path.read_text()).body
                       if isinstance(stmt, ast.Assign) and stmt.targets[0].id == 'CMAKE_LINK')
            new = next(ast.literal_eval(stmt.value) for stmt in ast.parse(rendered[path]).body
                       if isinstance(stmt, ast.Assign) and stmt.targets[0].id == 'CMAKE_LINK')
            expected = dict(old, refs=[self.mapped(ref, None, '', source_file=False) for ref in old['refs']])
            if new != expected:
                raise ValueError(f'{path}: altered a CMake object count, archive or link sequence')
            self.stats['verified_link_plans'] += 1

    def rewrite_consumers(self):
        """Rewrite concrete labels in BUILD, module, rule, and runtime data."""
        output = {}
        native_builds = {build.path for build in self.builds}
        filenames = subprocess.check_output(
            ['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard',
             '--', '*.bzl', '*.BUILD.bazel', '*.MODULE.bazel', 'BUILD', 'BUILD.bazel',
             '**/BUILD', '**/BUILD.bazel', 'MODULE.bazel'], cwd=self.root,
        ).decode().split('\0')
        for filename in dict.fromkeys(filenames):
            path = self.root / filename
            if not filename or not path.is_file() or path in native_builds or not (
                    filename.endswith(('.bzl', '.BUILD.bazel', '.MODULE.bazel')) or
                    path.name in {'BUILD', 'BUILD.bazel', 'MODULE.bazel'}):
                continue
            if filename == 'bazel/rules/root_inputs.bzl':
                continue
            try:
                source = Source(path.read_text())
            except SyntaxError:
                continue
            edits = []
            cross_repo = filename.startswith('bazel/dependencies/') and filename.endswith('.BUILD.bazel')
            for node in ast.walk(source.module):
                if not isinstance(node, ast.Constant) or not isinstance(node.value, str):
                    continue
                value = node.value
                if MAIN_LABEL.fullmatch(value) and (not cross_repo or value.startswith('@clickhouse//')):
                    value = self.mapped(value, None, '', cross_repo, source_file=False)
                else:
                    value = LOCATION.sub(
                        lambda m: (m[0] if cross_repo and not m[2].startswith('@clickhouse//') else
                                   '$(' + m[1] + ' ' + self.mapped(m[2], None, '', cross_repo) + ')'),
                        value)
                if value != node.value:
                    edits.append((*source.span(node), json.dumps(value)))
            if edits:
                result = source.rewrite(edits)
                ast.parse(result, filename=filename)
                output[path] = result
        return output

    def render_builds(self):
        rendered = {}
        packages = set(self.rows) | set(self.exports)
        for package in sorted(packages):
            if package and not native_directory(package):
                continue
            loads = dict(self.loads[package])
            # Source generation stays in //bazel; this temporary root macro
            # will be removed when its dynamic source exports are expanded.
            lines = [render(stmt) for stmt in loads.values()
                     if not any(isinstance(n, ast.Constant) and n.value == 'declare_inputs' for n in ast.walk(stmt))]
            names = {call_name(row.value) for row in self.rows[package] if isinstance(row, ast.Expr)}
            if 'cc_library' in names and not any('"cc_library"' in text or "'cc_library'" in text for text in lines):
                lines.append('load("@rules_cc//cc:cc_library.bzl", "cc_library")')
            lines = list(dict.fromkeys(lines))
            lines.append('package(default_visibility = ["//visibility:public"])')
            exported = self.exports.get(package)
            if exported:
                lines.append('exports_files(' + json.dumps(sorted(exported)) + ')')
            lines += [render(row) for row in self.rows[package]]
            text = compact('\n\n'.join(lines) + '\n')
            ast.parse(text)
            rendered[self.root / package / 'BUILD.bazel'] = text
        return rendered


def expr_stmt(value):
    return ast.Expr(value=expr(value))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--stage', choices=('base', 'all'), default='all')
    parser.add_argument('--write', action='store_true')
    args = parser.parse_args()
    migration = Migration(args.root, args.stage)
    result = migration.prepare()
    print(json.dumps(dict(migration.stats, files=len(result)), indent=2))
    if args.write:
        for path, value in result.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(value)


if __name__ == '__main__':
    main()
