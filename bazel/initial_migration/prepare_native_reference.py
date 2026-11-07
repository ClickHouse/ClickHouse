#!/usr/bin/env python3
"""Prepare declared ClickHouse source and ARM64 tools for native CMake RBE."""

import argparse
import hashlib
import json
import lzma
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
import zipfile

PORTS = 'https://ports.ubuntu.com/ubuntu-ports/'
NINJA_URL = 'https://github.com/ninja-build/ninja/releases/download/v1.12.1/ninja-linux-aarch64.zip'
NINJA_SHA256 = '5c25c6570b0155e95fce5918cb95f1ad9870df5768653afe128db822301a05a1'
# glibc, Perl, and zlib are supplied by the Ubuntu 22.04 image. Keep glibc
# out of LD_LIBRARY_PATH so Ubuntu's loader and libc remain from one image.
ARM_PACKAGES = (
    'git', 'xxd', 'make', 'patch', 'liberror-perl', 'libpcre2-8-0',
    'libexpat1', 'libcurl3-gnutls', 'libnghttp2-14', 'libidn2-0',
    'librtmp1', 'libssh-4', 'libpsl5', 'libnettle8', 'libgnutls30',
    'libldap-2.5-0', 'libgssapi-krb5-2', 'libunistring2', 'libhogweed6',
    'libgmp10', 'libp11-kit0', 'libtasn1-6', 'libffi8', 'libkrb5-3',
    'libk5crypto3', 'libkrb5support0', 'libkeyutils1', 'libsasl2-2',
)
# Cargo invokes the native compiler without ClickHouse's target sysroot for
# its host build scripts. Stage the Ubuntu host files separately from PATH and
# LD_LIBRARY_PATH; native_cmake_reference.py installs only missing host files.
HOST_PACKAGES = (
    'binutils', 'binutils-aarch64-linux-gnu', 'binutils-common',
    'libbinutils', 'libctf0', 'libctf-nobfd0',
    'libc6-dev', 'linux-libc-dev', 'libgcc-11-dev', 'libgcc-s1',
)
SOURCE_ARCHIVE_VERSION = 2
TOOLS_ARCHIVE_VERSION = 3
SAFE_GIT_CONFIG = '[core]\n\trepositoryformatversion = 0\n\tfilemode = true\n\tbare = false\n'
ROOT_NON_CMAKE_FILES = (
    'BUILD.bazel', 'MODULE.bazel', 'MODULE.bazel.lock',
    '.bazelrc', '.buildifier.json', '.gitignore', 'README.bazel.md', 'any2bazel.json',
)
ROOT_DIFF_EXCLUDES = (':!bazel/**',) + tuple(':!' + name for name in ROOT_NON_CMAKE_FILES)


def run(argv, cwd, *, data=None):
    return subprocess.check_output(argv, cwd=cwd, input=data)


def git(cwd, *args):
    return run(['git', '-c', 'core.hooksPath=/dev/null', *args], cwd)


def sha256_file(path):
    result = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            result.update(block)
    return result.hexdigest()


def download(url, output, expected=None):
    if output.is_file() and (expected is None or sha256_file(output) == expected):
        return sha256_file(output)
    temporary = output.with_name(output.name + '.part')
    request = urllib.request.Request(url, headers={'User-Agent': 'ClickHouse-native-CMake-reference'})
    with urllib.request.urlopen(request, timeout=120) as source, temporary.open('wb') as destination:
        shutil.copyfileobj(source, destination, 4 * 1024 * 1024)
    actual = sha256_file(temporary)
    if expected is not None and actual != expected:
        temporary.unlink()
        raise ValueError(f'SHA256 mismatch for {url}: {actual} != {expected}')
    temporary.replace(output)
    return actual


def tracked_repositories(root):
    """List original indexed sources, excluding only root Bazel inputs and credentials."""
    pending = [(Path('.'), root)]
    while pending:
        relative, repo = pending.pop(0)
        head = git(repo, 'rev-parse', '--verify', 'HEAD').decode().strip()
        timestamp = int(git(repo, 'log', '-1', '--format=%ct', head))
        try:
            ref = git(repo, 'symbolic-ref', '-q', 'HEAD').decode().strip()
        except subprocess.CalledProcessError as error:
            if error.returncode != 1:
                raise
            ref = None
        diff_args = ['diff', '--binary', 'HEAD', '--', '.']
        if relative == Path('.'):
            diff_args += ROOT_DIFF_EXCLUDES
        diff = git(repo, *diff_args)
        files = []
        for entry in git(repo, 'ls-files', '--stage', '-z').split(b'\0'):
            if not entry:
                continue
            mode, object_id, stage_and_path = entry.split(b' ', 2)
            stage, raw_name = stage_and_path.split(b'\t', 1)
            name = Path(os.fsdecode(raw_name))
            if stage != b'0' or name.is_absolute() or '..' in name.parts:
                raise ValueError(f'Unmerged or invalid indexed source in {repo}: {name}')
            if mode == b'160000':
                subrepo = repo / name
                observed = git(subrepo, 'rev-parse', '--verify', 'HEAD').decode().strip()
                if observed != object_id.decode():
                    raise ValueError(f'Git link {relative / name}: index={object_id.decode()} checkout={observed}')
                pending.append((relative / name, subrepo))
            elif (name.name != '.bazelrc.local'
                  and name.parts[:1] != ('native_reference_inputs',)
                  and name.parts[:2] != ('bazel', 'native_reference_inputs')
                  and (relative != Path('.') or
                       (name.parts[:1] != ('bazel',) and name.as_posix() not in ROOT_NON_CMAKE_FILES))):
                files.append((name, int(mode, 8)))
        yield {'path': relative.as_posix(), 'head': head, 'ref': ref, 'timestamp': timestamp,
               'diff': hashlib.sha256(diff).hexdigest()}, repo, files


def safe_git_copy(repo, target, record):
    """Recreate a shallow original HEAD and index from its real commit and tree objects."""
    target.mkdir(parents=True)
    (target / 'config').write_text(SAFE_GIT_CONFIG)
    (target / 'refs').mkdir()
    head = record['head']
    ref = record['ref']
    if ref:
        git(repo, 'check-ref-format', ref)
        ref_file = target / ref
        ref_file.parent.mkdir(parents=True, exist_ok=True)
        ref_file.write_text(head + '\n')
        (target / 'HEAD').write_text('ref: ' + ref + '\n')
    else:
        (target / 'HEAD').write_text(head + '\n')
    (target / 'shallow').write_text(head + '\n')
    (target / 'objects/pack').mkdir(parents=True)
    # Existing packs, index stat fields, remote refs, config and hooks all
    # vary between clones. Only the actual commit and its tree are required
    # by CMake Git version discovery and git ls-files license discovery.
    objects = {head}
    for row in git(repo, 'ls-tree', '-r', '-t', '-z', head).split(b'\0'):
        if row:
            _, kind, value = row.split(b' ', 2)
            object_id, _ = value.split(b'\t', 1)
            if kind in (b'blob', b'tree'):
                objects.add(object_id.decode())
    objects.add(git(repo, 'rev-parse', head + '^{tree}').decode().strip())
    pack = target / 'objects/pack/native-reference.pack'
    object_list = ''.join(value + '\n' for value in sorted(objects)).encode()
    with pack.open('wb') as output:
        subprocess.run(['git', '-c', 'core.hooksPath=/dev/null', 'pack-objects', '--stdout',
                        '--window=0', '--threads=1', '--no-reuse-delta', '--no-reuse-object'],
                       cwd=repo, input=object_list, stdout=output, check=True)
    with pack.open('rb') as input_pack:
        subprocess.run(['git', 'index-pack', '--stdin'], cwd=target.parent,
                       stdin=input_pack, stdout=subprocess.DEVNULL, check=True)
    pack.unlink()
    # read-tree writes a real Git index for this commit, with zero stat fields.
    git(target.parent, 'read-tree', head)
    observed = git(target.parent, 'rev-parse', '--verify', 'HEAD').decode().strip()
    if observed != head:
        raise ValueError(f'Sanitized HEAD differs: {repo}, {observed} != {head}')
    git(target.parent, 'cat-file', '-e', head + '^{commit}')
    git(target.parent, 'cat-file', '-e', head + '^{tree}')


def stable_tar_file(output, source, arcname, timestamp, mode):
    info = tarfile.TarInfo(arcname)
    info.mtime = timestamp
    info.uid = info.gid = 0
    info.uname = info.gname = ''
    info.pax_headers = {}
    if source.is_symlink():
        info.type = tarfile.SYMTYPE
        info.linkname = os.readlink(source)
        info.mode = 0o777
        output.addfile(info)
    elif source.is_dir():
        info.type = tarfile.DIRTYPE
        info.mode = 0o755
        output.addfile(info)
    else:
        info.mode = 0o755 if mode & 0o111 else 0o644
        info.size = source.stat().st_size
        with source.open('rb') as stream:
            output.addfile(info, stream)


def stable_tar_tree(output, directory, prefix, timestamp):
    stable_tar_file(output, directory, prefix, timestamp, directory.lstat().st_mode)
    for source in sorted(directory.rglob('*'), key=os.fsencode):
        name = (Path(prefix) / source.relative_to(directory)).as_posix()
        stable_tar_file(output, source, name, timestamp, source.lstat().st_mode)


def source_archive(root, directory):
    records = list(tracked_repositories(root))
    identity = [record for record, _, _ in records]
    manifest = directory / 'source.json'
    archive = directory / 'source.tar'
    if archive.is_file() and manifest.is_file():
        saved = json.loads(manifest.read_text())
        if (saved.get('version') == SOURCE_ARCHIVE_VERSION and saved['repositories'] == identity
                and saved['sha256'] == sha256_file(archive)):
            print('CMAKE_SOURCE_REUSED=' + saved['sha256'], flush=True)
            return
    with tempfile.TemporaryDirectory(prefix='git-sanitized-', dir=directory) as temporary:
        sanitized = Path(temporary)
        partial = directory / 'source.tar.part'
        with tarfile.open(partial, 'w', format=tarfile.PAX_FORMAT) as output:
            for record, repo, files in records:
                relative = Path(record['path'])
                for name, mode in sorted(files, key=lambda item: os.fsencode(item[0])):
                    source = repo / name
                    if source.is_file() or source.is_symlink():
                        stable_tar_file(output, source, (relative / name).as_posix(), record['timestamp'], mode)
                safe_repo = sanitized / relative
                safe_git_copy(repo, safe_repo / '.git', record)
                for source in sorted((safe_repo / '.git').rglob('*'), key=os.fsencode):
                    name = (relative / source.relative_to(safe_repo)).as_posix()
                    stable_tar_file(output, source, name, record['timestamp'], 0o644)
        digest = sha256_file(partial)
        partial.replace(archive)
        manifest.write_text(json.dumps({
            'version': SOURCE_ARCHIVE_VERSION, 'repositories': identity, 'sha256': digest,
        }, indent=2) + '\n')
        print('CMAKE_SOURCE_SHA256=' + digest, flush=True)


def release_hashes(release):
    hashes = {}
    active = False
    for line in release.splitlines():
        if line == 'SHA256:':
            active = True
        elif line and not line.startswith(' '):
            active = False
        elif active:
            digest, size, name = line.split()
            hashes[name] = (digest, int(size))
    if not hashes:
        raise ValueError('Ubuntu jammy Release contains no SHA256 index records')
    return hashes


def package_records(index):
    record = {}
    with lzma.open(index, 'rt', encoding='utf-8') as stream:
        for line in stream:
            if line == '\n':
                if record:
                    yield record
                record = {}
            elif not line.startswith((' ', '\t')):
                key, _, value = line.rstrip('\n').partition(': ')
                record[key] = value
        if record:
            yield record


def elf_arm64(path):
    with path.open('rb') as stream:
        header = stream.read(20)
    if len(header) != 20 or header[:6] != b'\x7fELF\x02\x01' or int.from_bytes(header[18:20], 'little') != 183:
        raise ValueError(f'Expected a little-endian ARM64 ELF executable: {path}')


def tools_archive(directory):
    archive = directory / 'tools.tar'
    manifest = directory / 'tools.json'
    if archive.is_file() and manifest.is_file():
        saved = json.loads(manifest.read_text())
        if (saved.get('version') == TOOLS_ARCHIVE_VERSION
                and saved['ninja_sha256'] == NINJA_SHA256
                and tuple(item['package'] for item in saved['packages']) == ARM_PACKAGES + HOST_PACKAGES
                and saved['sha256'] == sha256_file(archive)):
            print('CMAKE_ARM_TOOLS_REUSED=' + saved['sha256'], flush=True)
            return
    downloads = directory / 'downloads'
    downloads.mkdir(exist_ok=True)
    release = downloads / 'jammy.Release'
    release_sha = download(PORTS + 'dists/jammy/Release', release)
    hashes = release_hashes(release.read_text())
    keyring = Path('/usr/share/keyrings/ubuntu-archive-keyring.gpg')
    signature_verified = False
    if keyring.is_file() and shutil.which('gpgv'):
        signature = downloads / 'jammy.Release.gpg'
        download(PORTS + 'dists/jammy/Release.gpg', signature)
        subprocess.run(['gpgv', '--keyring', str(keyring), str(signature), str(release)], check=True)
        signature_verified = True
    packages = {}
    index_hashes = {}
    for component in ('main', 'universe'):
        name = f'{component}/binary-arm64/Packages.xz'
        expected, length = hashes[name]
        index = downloads / (component + '.Packages.xz')
        index_hashes[name] = download(PORTS + 'dists/jammy/' + name, index, expected)
        if index.stat().st_size != length:
            raise ValueError(f'Ubuntu package index length differs: {name}')
        for record in package_records(index):
            if record.get('Package') in ARM_PACKAGES + HOST_PACKAGES and record.get('Architecture') in ('arm64', 'all'):
                packages[record['Package']] = record
    absent = set(ARM_PACKAGES + HOST_PACKAGES) - packages.keys()
    if absent:
        raise ValueError('Packages absent from jammy ports: ' + ', '.join(sorted(absent)))
    with tempfile.TemporaryDirectory(prefix='arm-tools-', dir=directory) as temporary:
        tools = Path(temporary)
        selected = []
        for name in ARM_PACKAGES + HOST_PACKAGES:
            record = packages[name]
            deb = downloads / Path(record['Filename']).name
            digest = download(PORTS + record['Filename'], deb, record['SHA256'])
            if deb.stat().st_size != int(record['Size']):
                raise ValueError(f'Ubuntu deb length differs: {name}')
            destination = tools / 'host' if name in HOST_PACKAGES else tools
            subprocess.run(['dpkg-deb', '-x', str(deb), str(destination)], check=True)
            selected.append({'package': name, 'version': record['Version'],
                             'filename': record['Filename'], 'sha256': digest})
        ninja_zip = downloads / 'ninja-linux-aarch64-v1.12.1.zip'
        download(NINJA_URL, ninja_zip, NINJA_SHA256)
        (tools / 'bin').mkdir(exist_ok=True)
        with zipfile.ZipFile(ninja_zip) as source:
            ninja = tools / 'bin/ninja'
            ninja.write_bytes(source.read('ninja'))
            ninja.chmod(0o755)
        for path in ('usr/bin/git', 'usr/bin/xxd', 'usr/bin/make', 'usr/bin/patch', 'bin/ninja'):
            elf_arm64(tools / path)
        partial = directory / 'tools.tar.part'
        with tarfile.open(partial, 'w', format=tarfile.PAX_FORMAT) as output:
            for name in ('bin', 'usr', 'lib', 'host'):
                if (tools / name).exists():
                    stable_tar_tree(output, tools / name, name, 0)
        digest = sha256_file(partial)
        partial.replace(archive)
        manifest.write_text(json.dumps({
            'version': TOOLS_ARCHIVE_VERSION, 'sha256': digest, 'ninja_sha256': NINJA_SHA256,
            'release_sha256': release_sha, 'release_gpg_verified': signature_verified,
            'index_sha256': index_hashes, 'packages': selected,
        }, indent=2) + '\n')
        print('CMAKE_ARM_TOOLS_SHA256=' + digest, flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument('--skip-submodule-update', action='store_true',
                        help='Use only with an already initialized checkout.')
    parser.add_argument('--sources-only', action='store_true', help='Prepare Git sources without ARM tools.')
    args = parser.parse_args()
    root = args.root.resolve()
    if not args.skip_submodule_update:
        subprocess.run(['git', 'submodule', 'update', '--init', '--recursive', '--depth', '1', '--jobs', '12'],
                       cwd=root, check=True)
    directory = root / 'bazel/native_reference_inputs'
    directory.mkdir(parents=True, exist_ok=True)
    source_archive(root, directory)
    if not args.sources_only:
        tools_archive(directory)
    (directory / 'BUILD.bazel').write_text(
        'package(default_visibility = ["//visibility:public"])\n'
        'exports_files(["source.tar", "tools.tar"])\n')


if __name__ == '__main__':
    main()
