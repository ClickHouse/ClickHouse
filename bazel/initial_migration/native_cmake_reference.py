#!/usr/bin/env python3
"""Build the original ClickHouse CMake reference on a native ARM64 RBE executor."""

import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile


ROOT = Path("/home/buildbuddy/workspace/repo-root")
BUILD_NAME = "build_bazel_reference_aarch64"
TOOL_NAMES = (
    "clang", "clang++", "ld.lld", "llvm-ar", "llvm-ranlib", "llvm-nm",
    "llvm-objcopy", "llvm-strip", "llvm-readelf", "llvm-objdump",
)
STRIP_FLAGS = ["--strip-all", "--remove-section=.note.gnu.build-id"]
METADATA_FILES = (
    "src/Storages/System/StorageSystemBuildOptions.generated.cpp",
    "base/base/GitHash.generated.cpp",
)
RESOURCES = {
    "ClickStack": ("contrib/clickstack-cmake", "clickstack_compressed", "ClickStackResources.generated.h"),
    "SQLConsole": ("contrib/sql-console-cmake", "sql_console_compressed", "SQLConsoleResources.generated.h"),
}


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, fromfile_prefix_chars="@")
    for name in (
        "source-archive", "tools-archive", "cc", "cxx", "rustc", "cargo",
        "rust-sysroot-anchor", "cmake", "python", "output-dir",
    ):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--jobs", type=int, default=12)
    parser.add_argument("--configure-only", action="store_true")
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")
    # Preserve Bazel's registered Rust sysroot spelling. Resolving rustc's
    # symlink would give rustc a different sysroot than rust.all_files.
    for key, value in vars(args).items():
        if isinstance(value, Path):
            setattr(args, key, value.absolute())
    return args


def sha256(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def elf_machine(path):
    with path.open("rb") as stream:
        header = stream.read(20)
    if len(header) != 20 or header[:5] != b"\x7fELF\x02" or header[5] not in (1, 2):
        raise ValueError(f"Expected ELF64 executable: {path}")
    return int.from_bytes(header[18:20], "little" if header[5] == 1 else "big")


def native_elf(path):
    machine = elf_machine(path)
    if machine != 183:
        raise ValueError(f"Expected AArch64 ELF machine 183, got {machine}: {path}")
    return {"sha256": sha256(path), "elf_machine": machine}


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2) + "\n")


def native_checkout():
    """Own ROOT only inside the same isolated BuildBuddy OCI mount Cargo uses."""
    if Path.cwd() != Path("/buildbuddy-execroot"):
        raise RuntimeError("The native CMake reference requires a BuildBuddy OCI executor")
    if ROOT.resolve() != ROOT:
        raise ValueError(f"Native CMake ROOT has a symlink ancestor: {ROOT}")
    root_fs = None
    execroot_mounted = False
    with Path("/proc/self/mountinfo").open() as mounts:
        for line in mounts:
            fields = line.split()
            separator = fields.index("-")
            mountpoint = Path(fields[4])
            if mountpoint == Path("/"):
                root_fs = fields[separator + 1]
            elif ROOT.is_relative_to(mountpoint):
                raise RuntimeError(f"Native CMake ROOT intersects an additional mount: {mountpoint}")
            if mountpoint == Path("/buildbuddy-execroot"):
                execroot_mounted = True
    if root_fs != "overlay" or not execroot_mounted:
        raise RuntimeError("Native CMake requires an OCI overlay rootfs with a separately mounted execroot")
    ROOT.mkdir(parents=True, exist_ok=False)


def run(command, env, log, *, cwd=ROOT, stderr=None):
    subprocess.run(
        [str(arg) for arg in command], cwd=cwd, env=env, check=True,
        stdout=log, stderr=stderr if stderr is not None else subprocess.STDOUT,
    )


def stage_host_development(tools, build):
    """Add missing Ubuntu files only inside native_checkout's verified OCI rootfs."""
    host = tools / "host"
    installed = []
    supplied = []
    for prefix in ("usr", "lib"):
        source = host / prefix
        if not source.is_dir():
            continue
        for path in sorted(source.rglob("*")):
            name = path.relative_to(host)
            destination = Path("/") / name
            if destination.exists() or destination.is_symlink():
                supplied.append(str(name))
            elif path.is_symlink():
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.symlink_to(os.readlink(path))
                installed.append(str(name))
            elif path.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
            else:
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
                installed.append(str(name))
    write_json(build / "host-development.json", {
        "uid": os.geteuid(), "installed": installed, "provided_by_image": supplied,
    })
    # The original Cargo invocation does not pass -fuse-ld for build scripts.
    # ClickHouse's target commands pass --gcc-toolchain, --sysroot and lld.
    native_elf(Path("/usr/bin/ld"))
    for name in (
        "/usr/lib/aarch64-linux-gnu/Scrt1.o",
        "/usr/lib/aarch64-linux-gnu/libutil.a",
        "/usr/lib/gcc/aarch64-linux-gnu/11/crtbeginS.o",
        "/usr/lib/gcc/aarch64-linux-gnu/11/libgcc_s.so",
    ):
        if not Path(name).is_file():
            raise ValueError(f"Missing Ubuntu host development file: {name}")


def stage_toolchain(args, build, tools, env):
    if platform.system() != "Linux" or platform.machine() not in ("aarch64", "arm64"):
        raise RuntimeError(f"Native ARM64 CMake needs Linux/aarch64, got {platform.system()}/{platform.machine()}")
    manifest = {
        "cc": str(args.cc), "cxx": str(args.cxx),
        "rustc": str(args.rustc), "cargo": str(args.cargo),
        "rust_sysroot_anchor": str(args.rust_sysroot_anchor),
        "python": str(args.python), "cmake": str(args.cmake),
    }
    write_json(build / "toolchain.json", manifest)
    compiler = native_elf(args.cxx)
    if native_elf(args.cc) != compiler:
        raise ValueError("The selected Bazel C and C++ compiler files differ")
    rust = {name: native_elf(path) for name, path in (("rustc", args.rustc), ("cargo", args.cargo))}
    native_elf(args.cmake)
    native_elf(tools / "bin/ninja")
    native_elf(args.python)
    staged = args.cc.parent.parent
    install = tools / "llvm"
    (install / "bin").mkdir(parents=True, exist_ok=True)
    (install / "lib/clang").mkdir(parents=True, exist_ok=True)
    binary = args.cxx.resolve(strict=True)
    for name in TOOL_NAMES:
        target = install / "bin" / name
        try:
            os.link(binary, target)
        except OSError:
            shutil.copy2(binary, target)
    for directory in (staged / "lib/clang").iterdir():
        shutil.copytree(directory, install / "lib/clang" / directory.name, symlinks=False)
    for name in ("clang", "clang++"):
        if sha256(install / "bin" / name) != compiler["sha256"]:
            raise ValueError("Installed CMake compiler differs from the resolved Bazel compiler")
    resource_dir = subprocess.check_output(
        [str(install / "bin/clang"), "-print-resource-dir"], cwd=ROOT, env=env, text=True,
    ).strip()
    if not (Path(resource_dir) / "include/stddef.h").is_file():
        raise ValueError(f"Missing staged Clang builtin headers: {resource_dir}")
    rust_libdir = subprocess.check_output(
        [str(args.rustc), "--target", "aarch64-unknown-linux-gnu", "--print", "target-libdir"],
        cwd=ROOT, env=env, text=True,
    ).strip()
    if not any(Path(rust_libdir).glob("libstd-*.rlib")):
        raise ValueError(f"Missing registered ARM Rust standard library: {rust_libdir}")
    (build / "compiler.sha256").write_text(compiler["sha256"] + "  staged-llvm-clang\n")
    with (build / "compiler.txt").open("w") as versions:
        for name in TOOL_NAMES:
            run([install / "bin" / name, "--version"], env, versions)
        for command in (
            [args.rustc, "-vV"], [args.cargo, "-vV"], [args.cmake, "--version"],
            [tools / "bin/ninja", "--version"], [tools / "usr/bin/git", "--version"],
            [args.python, "--version"],
        ):
            run(command, env, versions)
    return compiler, rust


def configure(args, build, tools, env):
    query = build / ".cmake/api/v1/query"
    query.mkdir(parents=True)
    (query / "codemodel-v2").touch()
    llvm = tools / "llvm/bin"
    command = [
        args.cmake, "-S", ROOT, "-B", build, "-G", "Ninja",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DCMAKE_TOOLCHAIN_FILE=cmake/linux/toolchain-aarch64.cmake",
        "-DCMAKE_MAKE_PROGRAM=" + str(tools / "bin/ninja"),
        "-DCMAKE_C_COMPILER=" + str(llvm / "clang"),
        "-DCMAKE_CXX_COMPILER=" + str(llvm / "clang++"),
        "-DCMAKE_ASM_COMPILER=" + str(llvm / "clang"),
        "-DCOMPILER_CACHE=disabled",
        "-DENABLE_LIBRARIES=ON", "-DENABLE_RUST=ON",
        "-DENABLE_TESTS=ON", "-DENABLE_CLICKHOUSE_ALL=ON",
        "-DCMAKE_EXPORT_COMPILE_COMMANDS=ON",
        "-UCMAKE_PROJECT_INCLUDE",
        "-DRust_RESOLVE_RUSTUP_TOOLCHAINS=OFF",
        "-DRust_COMPILER=" + str(args.rustc),
        "-DRust_CARGO=" + str(args.cargo),
        "-DLINKER_NAME=" + str(llvm / "ld.lld"),
        "-DLLVM_AR_PATH=" + str(llvm / "llvm-ar"),
        "-DLLVM_RANLIB_PATH=" + str(llvm / "llvm-ranlib"),
        "-DOBJCOPY_PATH=" + str(llvm / "llvm-objcopy"),
        "-DNM_PATH=" + str(llvm / "llvm-nm"),
        "-DSTRIP_PATH=" + str(llvm / "llvm-strip"),
        "--trace-expand", "--trace-format=json-v1",
    ]
    write_json(build / "configure-command.json", [str(arg) for arg in command])
    with (build / "configure.log").open("w") as output, (build / "trace.jsonl").open("w") as trace:
        run(command, env, output, stderr=trace)
    log = (build / "configure.log").read_text(errors="replace")
    if "LLVM: Native build, building tablegen inline" not in log:
        raise ValueError("ClickHouse did not configure native inline LLVM TableGen")
    if "LLVM: Cross-compiling detected" in log or (build / "contrib/llvm-project-cmake/native-tblgen").exists():
        raise ValueError("ClickHouse unexpectedly configured cross-compilation for LLVM")
    with (build / "ninja-commands.txt").open("w") as output:
        run([tools / "bin/ninja", "-C", build, "-t", "commands", "clickhouse"], env, output)
    (build / "metadata.sha256").write_text("".join(
        sha256(build / file) + "  " + str(build / file) + "\n"
        for file in METADATA_FILES
    ))


def record_reference(args, build, tools, env, compiler, rust):
    binary = build / "programs/clickhouse"
    binary_record = native_elf(binary)
    llvm = tools / "llvm/bin"
    record = {
        "arch": "aarch64",
        "host_system": platform.system(),
        "host_processor": platform.machine(),
        "bytes": binary.stat().st_size,
        "compiler_sha256": compiler["sha256"],
        "compiler_elf_machine": compiler["elf_machine"],
        "rustc_sha256": rust["rustc"]["sha256"],
        "cargo_sha256": rust["cargo"]["sha256"],
        "rustc_elf_machine": rust["rustc"]["elf_machine"],
        "cargo_elf_machine": rust["cargo"]["elf_machine"],
        "cmake_sha256": binary_record["sha256"],
        "elf_machine": binary_record["elf_machine"],
        "strip_flags": STRIP_FLAGS,
        "native_tablegen": {},
    }
    with (build / "cmake.readelf.txt").open("w") as output:
        run([llvm / "llvm-readelf", "-h", binary], env, output)
    with (build / "native-tablegen.readelf.txt").open("w") as output:
        for name in ("llvm-tblgen", "llvm-min-tblgen"):
            tool = build / "contrib/llvm-project-cmake" / name
            record["native_tablegen"][name] = native_elf(tool)
            run([llvm / "llvm-readelf", "-h", tool], env, output)
    stripped = build / "clickhouse.reference.stripped"
    with (build / "strip.log").open("w") as output:
        run([llvm / "llvm-strip", "--strip-all", "-o", stripped, binary], env, output)
        record["plain_stripped_sha256"] = sha256(stripped)
        run([llvm / "llvm-strip", *STRIP_FLAGS, "-o", stripped, binary], env, output)
        record["stripped_sha256"] = sha256(stripped)
        record["stripped_bytes"] = stripped.stat().st_size
    stripped.unlink()
    (build / "cmake.sha256").write_text(record["cmake_sha256"] + "  " + str(binary) + "\n")
    write_json(build / "cmake-reference-summary.json", record)


def copy_file(source, destination):
    if source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)


def gzip_file(source, destination):
    if source.is_file():
        destination.parent.mkdir(parents=True, exist_ok=True)
        with source.open("rb") as original, gzip.open(destination, "wb") as compressed:
            shutil.copyfileobj(original, compressed)


def archive_directory(build, relative, destination):
    directory = build / relative
    if directory.is_dir():
        with tarfile.open(destination, "w:gz") as archive:
            archive.add(directory, arcname=relative)


def save_evidence(build, dest):
    for name in (
        "configure-command.json", "configure.log", "CMakeCache.txt", "metadata.sha256",
        "cmake.sha256", "compiler.sha256", "compiler.txt", "toolchain.json", "host-development.json", "build.log",
        "cmake-reference-summary.json", "cmake.readelf.txt", "native-tablegen.readelf.txt", "strip.log",
    ):
        copy_file(build / name, dest / name)
    for name in ("trace.jsonl", "compile_commands.json", "build.ninja", "ninja-commands.txt"):
        gzip_file(build / name, dest / (name + ".gz"))
    archive_directory(build, ".cmake/api/v1/reply", dest / "cmake-file-api.tar.gz")
    archive_directory(
        build, "contrib/llvm-project/llvm/include/llvm/Config", dest / "llvm-config-headers.tar.gz",
    )
    for name in METADATA_FILES:
        copy_file(build / name, dest / name)
    licenses = "src/Storages/System/StorageSystemLicenses.generated.cpp"
    gzip_file(build / licenses, dest / (licenses + ".gz"))
    copy_file(build / "programs/clickhouse", dest / "clickhouse.cmake")
    records = {}
    for namespace, (directory, compressed, generated) in RESOURCES.items():
        parent = build / directory
        copy_file(parent / generated, dest / directory / generated)
        records[namespace] = []
        for path in sorted((parent / compressed).rglob("*.gz")):
            content = path.read_bytes()
            if len(content) < 18 or content[:4] != b"\x1f\x8b\x08\x08":
                raise ValueError(f"Unexpected GNU gzip resource header: {path}")
            header_end = content.index(b"\0", 10) + 1
            records[namespace].append({
                "file": path.relative_to(parent / compressed).as_posix(),
                "header": list(content[:header_end]),
                "mtime": int.from_bytes(content[4:8], "little"),
                "basename": content[10:header_end - 1].decode("utf-8"),
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            })
    write_json(dest / "resource-gzip-headers.json", records)


def print_log_tail(path):
    try:
        with path.open("rb") as log:
            log.seek(max(0, os.fstat(log.fileno()).st_size - 4096))
            tail = log.read(4096)
    except OSError:
        return
    if tail:
        print(f"{path.name} (last {len(tail)} bytes):", file=sys.stderr)
        print(tail.decode(errors="replace"), file=sys.stderr)


def build_reference(args, evidence):
    status = {"status": 1, "phase": "initializing", "configure_only": args.configure_only}
    build = ROOT / BUILD_NAME
    tools = ROOT / "build_bazel_reference_tools"
    try:
        status["phase"] = "extracting"
        native_checkout()
        tools.mkdir(parents=True)
        with (evidence / "prepare.log").open("w") as output:
            run(["tar", "-xf", args.source_archive, "-C", ROOT], os.environ, output)
            run(["tar", "-xf", args.tools_archive, "-C", tools], os.environ, output)
        build.mkdir(parents=True, exist_ok=False)
        env = dict(os.environ)
        env["PATH"] = os.pathsep.join([
            str(tools / "llvm/bin"), str(args.python.parent), str(args.cmake.parent),
            str(tools / "bin"), str(tools / "usr/bin"), str(args.rustc.parent),
            env.get("PATH", "/usr/bin:/bin"),
        ])
        env["LD_LIBRARY_PATH"] = os.pathsep.join([
            str(tools / "usr/lib/aarch64-linux-gnu"), str(tools / "lib/aarch64-linux-gnu"),
            *filter(None, [env.get("LD_LIBRARY_PATH")]),
        ])
        env["GIT_EXEC_PATH"] = str(tools / "usr/lib/git-core")
        env["SOURCE_DATE_EPOCH"] = "1"
        env["CARGO_HOME"] = str(tools / "cargo")
        env["CARGO_NET_OFFLINE"] = "true"
        env["CARGO_BUILD_JOBS"] = str(args.jobs)
        Path(env["CARGO_HOME"]).mkdir()
        status["phase"] = "staging-host-development"
        stage_host_development(tools, build)
        status["phase"] = "staging-toolchain"
        compiler, rust = stage_toolchain(args, build, tools, env)
        status["phase"] = "configuring"
        configure(args, build, tools, env)
        if not args.configure_only:
            status["phase"] = "building"
            with (build / "build.log").open("w") as output:
                run([args.cmake, "--build", build, "--target", "clickhouse", "--parallel", args.jobs], env, output)
            status["phase"] = "recording-hashes"
            record_reference(args, build, tools, env, compiler, rust)
        status["phase"] = "saving-evidence"
        save_evidence(build, evidence)
        write_json(evidence / "status.json", {
            "status": 0, "phase": "configured" if args.configure_only else "built",
            "configure_only": args.configure_only,
        })
        evidence.replace(args.output_dir)
    except Exception as error:
        rc = error.returncode if isinstance(error, subprocess.CalledProcessError) else 1
        status.update(status=128 - rc if rc < 0 else rc or 1, error=str(error)[:1024])
        print("CMAKE_REFERENCE_STATUS=" + json.dumps(status), file=sys.stderr)
        logs = {
            "extracting": [evidence / "prepare.log"],
            "staging-toolchain": [build / "compiler.txt"],
            "configuring": [build / "configure.log", build / "trace.jsonl"],
            "building": [build / "build.log"],
            "recording-hashes": [build / "strip.log"],
        }
        for log in logs.get(status["phase"], []):
            print_log_tail(log)
        return status["status"]
    return 0


def main():
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    # Bazel downloads every declared file from a failed action, even in minimal mode.
    with tempfile.TemporaryDirectory(dir=args.output_dir.parent, ignore_cleanup_errors=True) as scratch:
        evidence = Path(scratch) / "evidence"
        evidence.mkdir()
        return build_reference(args, evidence)


if __name__ == "__main__":
    sys.exit(main())
