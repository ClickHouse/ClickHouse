#!/usr/bin/env bash
# Run CMake on the x86-64 Linux host or a native BuildBuddy ARM64 executor.
set -euo pipefail

configure_only="${CMAKE_REFERENCE_CONFIGURE_ONLY:-0}"
case "$configure_only" in
    0|1) ;;
    *) echo "CMAKE_REFERENCE_CONFIGURE_ONLY must be 0 or 1" >&2; exit 2 ;;
esac
case "${CMAKE_REFERENCE_ARCH:-x86_64}" in
    x86_64) ;;
    aarch64)
        native_runner="$(dirname "$0")/run_native_reference.sh"
        if [[ "$configure_only" == 1 ]]; then
            exec bash "$native_runner" --configure-only
        fi
        exec bash "$native_runner"
        ;;
    *) echo "CMAKE_REFERENCE_ARCH must be x86_64 or aarch64" >&2; exit 2 ;;
esac
archive_binary="${CMAKE_REFERENCE_ARCHIVE_BINARY:-0}"
case "$archive_binary" in
    0|1) ;;
    *) echo "CMAKE_REFERENCE_ARCHIVE_BINARY must be 0 or 1" >&2; exit 2 ;;
esac
root="$(git rev-parse --show-toplevel)"
cd "$root"
build="$root/build_bazel_reference_x86_64"
tools_dir="$root/build_bazel_reference_tools"
mkdir -p "$build" "$tools_dir"
export SOURCE_DATE_EPOCH=1
export CARGO_HOME="$tools_dir/cargo"
export RUSTUP_HOME="$tools_dir/rustup"
export PATH="$tools_dir/llvm/bin:$tools_dir/python/bin:$tools_dir/yasm/usr/bin:$tools_dir/cmake/bin:$tools_dir/ninja:$CARGO_HOME/bin:$PATH"

save_evidence() {
    status=$?
    trap - EXIT
    if [[ -n "${BUILDBUDDY_ARTIFACTS_DIRECTORY:-}" ]]; then
        dest="$BUILDBUDDY_ARTIFACTS_DIRECTORY"
        mkdir -p "$dest"
        for file in configure-command.json configure.log CMakeCache.txt metadata.sha256 \
                    compiler.sha256 compiler.txt toolchain.json build.log; do
            [[ ! -f "$build/$file" ]] || cp "$build/$file" "$dest/"
        done
        for file in trace.jsonl compile_commands.json model.cmake.json build.ninja ninja-commands.txt; do
            [[ ! -f "$build/$file" ]] || gzip -c "$build/$file" > "$dest/$file.gz"
        done
        if [[ -d "$build/.cmake/api/v1/reply" ]]; then
            tar -C "$build" -czf "$dest/cmake-file-api.tar.gz" .cmake/api/v1/reply
        fi
        if (( status == 0 && configure_only == 0 )); then
            cp "$build/cmake.sha256" "$dest/"
            if [[ "$archive_binary" == 1 ]]; then
                cp "$build/programs/clickhouse" "$dest/clickhouse.cmake"
            fi
        fi
        for file in src/Storages/System/StorageSystemBuildOptions.generated.cpp base/base/GitHash.generated.cpp; do
            if [[ -f "$build/$file" ]]; then
                mkdir -p "$dest/$(dirname "$file")"
                cp "$build/$file" "$dest/$file"
            fi
        done
    fi
    if (( status != 0 )); then
        echo "CMAKE_REFERENCE_FAILED=$status"
        [[ ! -f "$build/configure.log" ]] || tail -85 "$build/configure.log"
        [[ ! -f "$build/trace.jsonl" ]] || tail -36 "$build/trace.jsonl"
        [[ ! -f "$build/build.log" ]] || tail -92 "$build/build.log"
    fi
    exit "$status"
}
trap save_evidence EXIT

bazel --host_jvm_args=-Xmx24g build --config=remote --config=x86_64 \
    --remote_download_outputs=toplevel --noannounce_rc --color=no --curses=no \
    --show_progress_rate_limit=30 //bazel:cmake_toolchain_files

# Clang normally resolves its symlink before finding builtin headers. Hardlink the
# actual multicall executable into the installation so clang and clang++ both
# locate this exact toolchain's lib/clang/22.
python3 - "$root" "$tools_dir" "$build" <<'PY'
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

root, tools_dir, build = map(Path, sys.argv[1:])
manifest = json.loads((root / "bazel-bin/bazel/cmake_toolchain_files.json").read_text())
execroot = Path(os.path.realpath(root / "bazel-out/.."))
staged = (execroot / manifest["cc"]).parent.parent
out = tools_dir / "llvm"
(out / "bin").mkdir(parents=True, exist_ok=True)
(out / "lib/clang").mkdir(parents=True, exist_ok=True)
binary = (execroot / manifest["cxx"]).resolve(strict=True)
for name in ("clang", "clang++", "ld.lld", "llvm-ar", "llvm-ranlib", "llvm-nm",
             "llvm-objcopy", "llvm-strip", "llvm-readelf", "llvm-objdump"):
    target = out / "bin" / name
    target.unlink(missing_ok=True)
    try:
        os.link(binary, target)
    except OSError:
        shutil.copyfile(binary, target)
        shutil.copymode(binary, target)
for resource in (staged / "lib/clang").iterdir():
    installed = out / "lib/clang" / resource.name
    if installed.exists():
        for directory, _, _ in os.walk(installed):
            path = Path(directory)
            path.chmod(path.stat().st_mode | 0o700)
        shutil.rmtree(installed)
    shutil.copytree(resource, installed, symlinks=False, dirs_exist_ok=True)
    # copytree preserves Bazel's read-only directory permissions. This is our
    # own copy, so allow Git/BuildBuddy to clean the installed directories.
    for directory, _, _ in os.walk(installed):
        path = Path(directory)
        path.chmod(path.stat().st_mode | 0o700)
nasm = out / "bin/nasm"
nasm.unlink(missing_ok=True)
shutil.copy2((execroot / manifest["nasm"]).resolve(strict=True), nasm)
python_install = (execroot / manifest["python"]).parent.parent
python_dest = tools_dir / "python"
if python_dest.is_symlink():
    python_dest.unlink()
if not python_dest.exists():
    python_dest.symlink_to(python_install, target_is_directory=True)
source_hash = hashlib.file_digest(binary.open("rb"), "sha256").hexdigest() if sys.version_info >= (3, 11) else subprocess.check_output(["sha256sum", str(binary)], text=True).split()[0]
for name in ("clang", "clang++"):
    actual = subprocess.check_output(["sha256sum", str(out / "bin" / name)], text=True).split()[0]
    if actual != source_hash:
        raise SystemExit("Installed compiler differs from Bazel C++ toolchain")
(build / "compiler.sha256").write_text(source_hash + "  staged-llvm-clang\n")
(build / "toolchain.json").write_text(json.dumps(manifest, indent=2) + "\n")
with (build / "compiler.txt").open("w") as version:
    for name in ("clang", "clang++", "ld.lld", "llvm-ar", "llvm-ranlib", "llvm-nm", "llvm-objcopy", "nasm"):
        subprocess.run([str(out / "bin" / name), "--version"], stdout=version, stderr=subprocess.STDOUT, check=True)
resource_dir = subprocess.check_output([str(out / "bin/clang"), "-print-resource-dir"], text=True).strip()
if not (Path(resource_dir) / "include/stddef.h").is_file():
    raise SystemExit("Missing Clang builtin headers in " + resource_dir)
print("CMAKE_COMPILER_SHA256=" + source_hash, flush=True)
print("CMAKE_RESOURCE_DIR=" + resource_dir, flush=True)
PY

# CMake/Ninja compile on this remote host. Release Bazel's analysis memory first.
bazel --host_jvm_args=-Xmx24g shutdown
# The bootstrap script used the host Python before the repo's Python existed.
# Discard Bash's command cache so the reference and any2bazel run under 3.11.
hash -r
if [[ ! -x "$tools_dir/yasm/usr/bin/yasm" ]]; then
    curl --fail --location --retry 3 --silent --show-error \
        https://archive.ubuntu.com/ubuntu/pool/universe/y/yasm/yasm_1.3.0-2build1_amd64.deb \
        -o "$tools_dir/yasm.deb"
    mkdir -p "$tools_dir/yasm"
    dpkg-deb -x "$tools_dir/yasm.deb" "$tools_dir/yasm"
    rm "$tools_dir/yasm.deb"
fi
yasm --version
python3 --version
python3 -c 'import sys; assert sys.version_info >= (3, 11), sys.executable'
if [[ ! -x "$tools_dir/cmake/bin/cmake" ]]; then
    curl --fail --location --retry 3 --silent --show-error \
        https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.tar.gz \
        -o "$tools_dir/cmake.tar.gz"
    mkdir -p "$tools_dir/cmake"
    tar -xzf "$tools_dir/cmake.tar.gz" -C "$tools_dir/cmake" --strip-components=1
    rm "$tools_dir/cmake.tar.gz"
fi
if [[ ! -x "$tools_dir/ninja/ninja" ]]; then
    curl --fail --location --retry 3 --silent --show-error \
        https://github.com/ninja-build/ninja/releases/download/v1.12.1/ninja-linux.zip \
        -o "$tools_dir/ninja.zip"
    python3 - "$tools_dir" <<'PY'
from pathlib import Path
import sys
import zipfile
tools = Path(sys.argv[1])
with zipfile.ZipFile(tools / "ninja.zip") as archive:
    archive.extract("ninja", tools / "ninja")
(tools / "ninja/ninja").chmod(0o755)
(tools / "ninja.zip").unlink()
PY
fi
if [[ ! -x "$CARGO_HOME/bin/rustup" ]]; then
    curl --fail --location --retry 3 --silent --show-error \
        https://static.rust-lang.org/rustup/dist/x86_64-unknown-linux-gnu/rustup-init \
        -o "$tools_dir/rustup-init"
    chmod +x "$tools_dir/rustup-init"
    "$tools_dir/rustup-init" -y --no-modify-path --profile minimal --default-toolchain nightly-2026-03-22
fi
rustup toolchain install nightly-2026-03-22 --profile minimal
cmake --version
ninja --version
rustup run nightly-2026-03-22 rustc -vV

if [[ ! -f "$tools_dir/any2bazel/scripts/extract_cmake.py" ]]; then
    git clone --no-checkout https://github.com/EngFlow/any2bazel.git "$tools_dir/any2bazel"
    git -C "$tools_dir/any2bazel" checkout c1ca1b4e3583f5321934a25dae84af954c6a021c
fi
git submodule update --init --recursive --depth 1 --jobs 12
python3 bazel/initial_migration/configure_reference.py --arch x86_64 --llvm-root "$tools_dir/llvm" \
    --any2bazel "$tools_dir/any2bazel" --nasm "$tools_dir/llvm/bin/nasm" \
    "--cmake-arg=-DLINKER_NAME=$tools_dir/llvm/bin/ld.lld" \
    "--cmake-arg=-DLLVM_AR_PATH=$tools_dir/llvm/bin/llvm-ar" \
    "--cmake-arg=-DLLVM_RANLIB_PATH=$tools_dir/llvm/bin/llvm-ranlib" \
    "--cmake-arg=-DOBJCOPY_PATH=$tools_dir/llvm/bin/llvm-objcopy" \
    "--cmake-arg=-DNM_PATH=$tools_dir/llvm/bin/llvm-nm" \
    "--cmake-arg=-DSTRIP_PATH=$tools_dir/llvm/bin/llvm-strip"

ninja -C "$build" -t commands clickhouse > "$build/ninja-commands.txt"
sha256sum "$build/src/Storages/System/StorageSystemBuildOptions.generated.cpp" \
    "$build/base/base/GitHash.generated.cpp" > "$build/metadata.sha256"
echo 'CMAKE_REFERENCE_CONFIGURED'
if [[ "$configure_only" == 1 ]]; then
    echo 'CMAKE_REFERENCE_CONFIGURE_ONLY=x86_64'
    exit 0
fi
# This parallelism is for Ninja on the remote Linux host. It is independent of
# common:remote --jobs=500 for Bazel's remote workers.
cmake --build "$build" --target clickhouse --parallel "${CMAKE_REFERENCE_JOBS:-12}" 2>&1 | tee "$build/build.log"
sha256sum "$build/programs/clickhouse" | tee "$build/cmake.sha256"
echo 'CMAKE_REFERENCE_BUILT'
