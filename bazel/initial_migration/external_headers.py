"""Declare header-only dependencies beside their pinned source files."""

from collections import defaultdict
from pathlib import PurePosixPath

from initial_migration.repository_inputs import (
    CC_HEADER_SUFFIXES, HeaderDirectory, REPOSITORIES, repository, source_path,
)


# These repositories provide C/C++ headers but no compiled C/C++ sources in
# the configured ClickHouse graphs. Rust, resource, and license repositories
# have separate consumers and must not become cc_library targets.
HEADER_ONLY_NAMES = frozenset({
    'aws_c_mqtt', 'darts_clone', 'datasketches_cpp', 'fast_float', 'fp16',
    'heks', 'jwt_cpp', 'libdivide', 'magic_enum', 'mapbox_geometry',
    'miniselect', 'morton_nd', 'msgpack_c', 'nlohmann_json', 'pocketfft',
    'rapidjson', 'sparsehash_c11', 'sqids_cpp', 'sz3', 'usearch', 'wagyu',
    'wyhash', 'xsimd',
})
HEADER_ONLY_REPOSITORIES = {
    owner: spec['name'].removeprefix('clickhouse_')
    for owner, spec in REPOSITORIES.items()
    if spec['name'].removeprefix('clickhouse_') in HEADER_ONLY_NAMES
}

_GENERATED_DIRECTORIES = ('config/x86_64/', 'config/aarch64/', 'gen/x86_64/', 'gen/aarch64/')
_INPUT_FIELDS = ('srcs', 'hdrs', 'textual_hdrs', 'additional_compiler_inputs')
_COMPILED_SUFFIXES = ('.c', '.cc', '.cpp', '.cxx', '.C', '.s', '.S', '.asm', '.ASM')


def _source_owner(path):
    """Select a pinned header-only repository, excluding configured outputs."""
    owner = repository(path)
    if owner not in HEADER_ONLY_REPOSITORIES:
        return None
    relative = path.removeprefix(owner).lstrip('/')
    if relative.startswith(_GENERATED_DIRECTORIES):
        return None
    return owner


def _parent_directories(directories):
    """Keep one ancestor instead of emitting overlapping recursive globs."""
    directories = sorted(set(directories))
    return tuple(directory for directory in directories
                 if not any(parent == '.' or directory.startswith(parent + '/')
                            for parent in directories if parent != directory))


class HeaderLibraries:
    """Collect public source directories once, then replace covered inputs."""

    def __init__(self, targets):
        directories = defaultdict(set)
        for arch_targets in targets.values():
            for target in arch_targets.values():
                for group in target.get('groups', []):
                    if group.get('cmake_runtime') or group.get('language') not in {'C', 'CXX', 'ASM'}:
                        continue
                    for field in _INPUT_FIELDS:
                        for item in group.get(field, ()):
                            if isinstance(item, HeaderDirectory):
                                # An additional input directory is not a
                                # declaration of public header ownership.
                                if field != 'hdrs':
                                    continue
                                owner = _source_owner(item.directory)
                                if owner:
                                    relative = item.directory.removeprefix(owner).lstrip('/')
                                    directories[owner].add(relative or '.')
                            elif isinstance(item, str):
                                path = source_path(item)
                                owner = _source_owner(path)
                                if not owner:
                                    continue
                                if field == 'srcs' and path.endswith(_COMPILED_SUFFIXES):
                                    raise ValueError(
                                        f'{owner} is no longer header-only: compiled source {item}')
                                if field in {'srcs', 'hdrs', 'textual_hdrs'} and path.endswith(CC_HEADER_SUFFIXES):
                                    relative = path.removeprefix(owner + '/')
                                    directories[owner].add(str(PurePosixPath(relative).parent))
        self.directories = {owner: _parent_directories(values) for owner, values in directories.items()}

    def dependency(self, label):
        """Return the library for an actual, observed source header label."""
        if not isinstance(label, str):
            return None
        path = source_path(label)
        owner = _source_owner(path)
        if not owner:
            return None
        relative = path.removeprefix(owner + '/')
        if HEADER_ONLY_REPOSITORIES[owner] == 'wyhash' and ('/' in relative or not relative.endswith('.h')):
            return None
        sparsehash_textual = (HEADER_ONLY_REPOSITORIES[owner] == 'sparsehash_c11'
                              and PurePosixPath(relative).parent == PurePosixPath('sparsehash')
                              and not PurePosixPath(relative).suffix)
        if not sparsehash_textual and (not path.endswith(CC_HEADER_SUFFIXES) or not any(
                directory == '.' or relative.startswith(directory + '/')
                for directory in self.directories.get(owner, ()))):
            return None
        return '@' + REPOSITORIES[owner]['name'] + '//:' + HEADER_ONLY_REPOSITORIES[owner]

    def use(self, fields, owner):
        """Replace native compiler inputs already exported by a header library."""
        # Keep the current compiler input order. Generated headers, data, and
        # nonstandard textual includes remain explicit compiler inputs.
        deps = set(fields['deps'])
        for field in _INPUT_FIELDS:
            original = fields[field]
            kept = []
            for item in original:
                dependency = self.dependency(item)
                if dependency and _source_owner(source_path(item)) != owner:
                    if dependency not in deps:
                        fields['deps'].append(dependency)
                        deps.add(dependency)
                else:
                    kept.append(item)
            fields[field] = kept

    def declare(self, builds, create_build):
        """Add one logical cc_library to each observed dependency repository."""
        # Import after build_file has loaded; build_file supplies the Glob
        # renderer used for both native libraries and these header libraries.
        from initial_migration.build_file import Concat, Glob, Select

        for owner, directories in sorted(self.directories.items()):
            if owner not in builds:
                builds[owner] = create_build(owner)
            build = builds[owner]
            name = HEADER_ONLY_REPOSITORIES[owner]
            if name in build.names:
                raise ValueError(f'Cannot declare header-only library @{REPOSITORIES[owner]["name"]}//:{name}: '
                                 'a target with that name already exists')
            patterns = [
                ('' if directory == '.' else directory + '/') + '**/*' + suffix
                for directory in directories
                for suffix in CC_HEADER_SUFFIXES
            ]
            attrs = {'hdrs': Glob(patterns, ('**/BUILD', '**/BUILD.bazel'))}
            if name == 'wyhash':
                # old_versions/ contains superseded wyhash algorithms.
                attrs.update(hdrs=Glob(['*.h']), includes=['.'])
            if name == 'sparsehash_c11':
                attrs['textual_hdrs'] = Glob(['sparsehash/*'], ['sparsehash/*.*'])
            if name == 'usearch':
                # contrib/usearch-cmake exports FP16 everywhere and SimSIMD on the two
                # supported Linux platforms. Its headers include simsimd/simsimd.h.
                build.loads.setdefault('@bazel_skylib//lib:selects.bzl', set()).add('selects')
                build.rule('selects.config_setting_group', 'simsimd_supported', {
                    'match_any': ['@platforms//cpu:x86_64', '@platforms//cpu:aarch64'],
                })
                simsimd_defines = [
                    'SIMSIMD_DYNAMIC_DISPATCH', 'SIMSIMD_NATIVE_BF16=0',
                    'USEARCH_CAN_COMPILE_BF16=0', 'USEARCH_CAN_COMPILE_FLOAT16', 'USEARCH_USE_SIMSIMD',
                ]
                attrs['defines'] = Concat([['USEARCH_USE_FP16LIB'], Select({
                    ':simsimd_supported': simsimd_defines, '//conditions:default': [],
                })])
                attrs['deps'] = Concat([['@clickhouse_fp16//:fp16'], Select({
                    ':simsimd_supported': ['@clickhouse_simsimd//:_simsimd'], '//conditions:default': [],
                })])
            build.rule('cc_library', name, attrs)
