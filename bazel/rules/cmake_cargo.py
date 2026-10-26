#!/usr/bin/env python3
"""Compile ClickHouse's Cargo static libraries using only declared inputs."""

import argparse
from collections import Counter
from contextlib import ExitStack
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import tempfile


def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, fromfile_prefix_chars="@")
    for name in [
        "output", "evidence", "cargo", "rustc", "rust-sysroot-anchor", "config", "manifest",
        "workspace-manifest", "cc", "cxx", "ar", "linux-headers", "glibc-headers",
    ]:
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--target", choices=["x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu"], required=True)
    parser.add_argument("--cargo-package", required=True)
    parser.add_argument("--cargo-feature", action="append", default=[])
    parser.add_argument("--path-crate", action="append", default=[])
    parser.add_argument("--mount", nargs=2, action="append", required=True, metavar=("INPUT_ROOT", "CLICKHOUSE_DIRECTORY"))
    parser.add_argument("--source", type=Path, action="append", required=True)
    parser.add_argument("--rust-flag", action="append", default=[])
    parser.add_argument("--native-source-root", type=Path)
    parser.add_argument("--native-compilation", action="store_true")
    parser.add_argument("--native-linker", type=Path)
    parser.add_argument("--native-unwind", type=Path)
    parser.add_argument("--native-gcc-s", type=Path)
    parser.add_argument("--native-glibc", type=Path)
    parser.add_argument("--native-c-flag", action="append", default=[])
    parser.add_argument("--native-cxx-flag", action="append", default=[])
    parser.add_argument("--openssl-headers", type=Path)
    parser.add_argument("--cmake", type=Path)
    parser.add_argument("--contrib-repositories", type=Path)
    return parser.parse_args()


def native_checkout(source_root):
    """Create an owned checkout only in the BuildBuddy OCI action rootfs."""
    # Bazel's "no-local" excludes dynamic local execution, but some standalone
    # local and sandbox strategies in Bazel 9.3 do not enforce "no-local".
    # BuildBuddy OCI binds this execution root from the action's own workspace.
    if Path.cwd() != Path("/buildbuddy-execroot"):
        raise RuntimeError("Cargo native_source_root requires a BuildBuddy OCI remote action")
    if not source_root.is_absolute() or source_root.resolve() != source_root:
        raise ValueError(f"native_source_root must be absolute, with no symlink or '..' ancestors: {source_root}")

    # The checkout must be in the OCI rootfs, not in a mounted host directory
    # or a persistent volume. Root and execroot use separate OCI mounts.
    root_fs = None
    execroot_mounted = False
    with Path("/proc/self/mountinfo").open() as mounts:
        for line in mounts:
            fields = line.split()
            separator = fields.index("-")
            mount_name = re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), fields[4])
            mountpoint = Path(mount_name)
            if mountpoint == Path("/"):
                root_fs = fields[separator + 1]
            elif source_root.is_relative_to(mountpoint):
                raise RuntimeError(f"Cargo native_source_root intersects an additional mount: {mountpoint}")
            if mountpoint == Path("/buildbuddy-execroot"):
                execroot_mounted = True
    if root_fs != "overlay" or not execroot_mounted:
        raise RuntimeError("Cargo native_source_root requires an OCI overlay rootfs with an isolated execroot mount")

    # Reject an existing checkout even if it is empty or a broken symlink.
    # Only the action that creates this directory may stage or remove it.
    source_root.mkdir(parents=True, exist_ok=False)
    return source_root


def source_mounts(args):
    mounts = []
    for root, directory in args.mount:
        destination = Path(directory)
        if destination.is_absolute() or ".." in destination.parts:
            raise ValueError(f"Invalid ClickHouse source directory {directory}")
        # Do not resolve symlinks. Bazel's declared file names, rather than the
        # host repository they may point into, specify the input directories.
        mounts.append((Path(root).absolute(), destination))
    return mounts


def mounted_source(path, mounts):
    path = path.absolute()
    matches = [(source, dest) for source, dest in mounts if path.is_relative_to(source)]
    if len(matches) != 1:
        raise ValueError(f"Expected one declared source directory for {path}, found {matches}")
    source_root, destination_root = matches[0]
    return destination_root / path.relative_to(source_root)


def stage_sources(args, checkout):
    mounts = source_mounts(args)
    counts = Counter()
    staged = set()
    for path in args.source:
        path = path.absolute()
        relative = mounted_source(path, mounts)
        if (relative.is_relative_to("contrib/rust_vendor")
                and relative.name.endswith(".clickhouse-cargo")):
            relative = relative.with_name(relative.name.removesuffix(".clickhouse-cargo"))
        if relative in staged:
            continue
        staged.add(relative)
        destination = checkout / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Cargo canonicalizes manifests when resolving path dependencies. They
        # must reside under this checkout, not resolve back into Bazel inputs.
        # Symlinking individual non-manifest files also avoids copying the
        # complete vendor tree into the remote action's temporary directory.
        if path.name in {"Cargo.toml", "Cargo.lock"}:
            shutil.copyfile(path, destination)
        else:
            destination.symlink_to(path)
        destination_root = next(dest for root, dest in mounts if path.is_relative_to(root))
        counts[str(destination_root)] += 1
    return dict(sorted(counts.items()))


def native_flags(args, checkout, gcc_root, libc_sysroot):
    # Restore directories owned by cc_library's toolchain. Each language's
    # flags are independently recorded from RUST_CFLAGS or RUST_CXXFLAGS.
    def restore_directories(flags):
        result = ["--gcc-toolchain=" + str(gcc_root)]
        for flag in flags:
            result.append(flag)
            if flag == "-fdebug-compilation-dir=.":
                result.append("-ffile-prefix-map=" + str(checkout) + "/=")
        return result

    cflags = restore_directories(args.native_c_flag) + ["--sysroot", str(libc_sysroot)]
    # Keep -isystem before -nostdinc++ like the native Cargo environment.
    include = str(checkout / "contrib/llvm-project/libcxx/include")
    cxxflags = []
    for flag in restore_directories(args.native_cxx_flag):
        if flag == "-nostdinc++":
            cxxflags.extend(["-isystem", include])
        cxxflags.append(flag)
    # Keep sysroot before the public libc++ definitions, like Corrosion.
    at = next((i for i, flag in enumerate(cxxflags) if flag.startswith("-D_LIBCPP_REMOVE_TRANSITIVE_INCLUDES")), len(cxxflags))
    cxxflags[at:at] = ["--sysroot", str(libc_sysroot)]
    return cflags, cxxflags


def cargo_linker(destination, clang, linker, gcc_root, libc_sysroot, unwind, gcc_s, glibc):
    """Supply Clang's host link dependencies from the same declared toolchain."""
    libraries = destination.parent / "cargo-native-libraries"
    libraries.mkdir()
    # Expose only libutil from LLVM's ABI-list based glibc link interface.
    # All other glibc libraries continue to come from ClickHouse's sysroot.
    for name, source in {
        "libgcc_s.a": gcc_s,
        "libutil.so": glibc / "libutil.so",
        "libutil.so.1": glibc / "libutil.so.1",
    }.items():
        if not source.is_file():
            raise FileNotFoundError("Missing declared LLVM runtime " + str(source))
        (libraries / name).symlink_to(source)
    command = [
        str(clang),
        "--ld-path=" + str(linker),
        "--gcc-toolchain=" + str(gcc_root),
        "--sysroot=" + str(libc_sysroot),
        "-L" + str(libraries),
    ]
    # LLVM's pinned libgcc_s archive accompanies libunwind, which provides
    # the actual _Unwind symbols required by Rust's host programs.
    destination.write_text("#!/bin/sh\nexec " + shlex.join(command) + ' "$@" ' + shlex.quote(str(unwind)) + "\n")
    destination.chmod(0o755)
    return destination


def cargo_config(template, vendor):
    content = template.read_text()
    substitutions = {
        "RUST_VENDOR_DIR": str(vendor),
        "RUST_RELEASE_PROFILE_EXTRA": "",
        "RUST_DEV_PROFILE": "[profile.dev]\ndebug = 1\nincremental = false\n",
    }
    for name, value in substitutions.items():
        key = "@" + name + "@"
        if key not in content:
            raise ValueError(f"Missing Cargo configuration substitution {key}")
        content = content.replace(key, value)
    unresolved = re.findall(r"@[A-Za-z_][A-Za-z_0-9]*@", content)
    if unresolved:
        raise ValueError(f"Unresolved Cargo configuration substitutions: {unresolved}")
    return content


def version(executable, *arguments):
    return subprocess.run([str(executable), *arguments], check=True, text=True, stdout=subprocess.PIPE).stdout.strip()


def native_rust_flags(args, gcc_root, libc_sysroot):
    # Cargo hashes the exact RUSTFLAGS vector for -Cextra-filename, including
    # argument order, split "-C" arguments, and the paths in link-arg.
    cpu_flags = []
    mangling = None
    for flag in args.rust_flag:
        if flag.startswith("-Csymbol-mangling-version="):
            if mangling is not None:
                raise ValueError("Duplicate Rust symbol-mangling-version in registered toolchain")
            mangling = flag.removeprefix("-C")
        elif flag.startswith(("-Ctarget-cpu=", "-Ctarget-feature=")):
            flag = flag.removeprefix("-C")
            if args.target.startswith("aarch64-") and flag.startswith("target-feature="):
                # The native aarch64 CMakeCache uses this accepted rustc spelling.
                flag = flag.replace("target-feature=", "target_feature=", 1)
            cpu_flags.extend(["-C", flag])
        else:
            raise ValueError(f"Rust flag is not in the native CMake toolchain: {flag}")
    if mangling != "symbol-mangling-version=v0" or not cpu_flags:
        raise ValueError("Registered Rust toolchain does not match native ClickHouse RUSTFLAGS")
    return [
        *cpu_flags,
        "-C", "link-arg=-fuse-ld=lld",
        "-C", "link-arg=--sysroot",
        "-C", "link-arg=" + str(libc_sysroot),
        "-C", "link-arg=--gcc-toolchain=" + str(gcc_root),
        "-C", "metadata=no-sanitizer",
        "-C", mangling,
        "-Adead_code", "-Amismatched_lifetime_syntaxes", "-Aunused_parens", "-Aunused_imports",
    ]


def selected_rust_sysroot(args):
    # Let rustc select its registered sysroot instead of inserting --sysroot
    # in RUSTFLAGS. Cargo includes RUSTFLAGS in archive member filenames.
    detected = Path(version(args.rustc, "--print", "sysroot"))
    registered = args.rust_sysroot_anchor.parent
    if detected.resolve() != registered.resolve():
        raise RuntimeError(f"Registered rustc selected {detected}, expected registered Rust sysroot {registered}")
    target_libdir = Path(version(args.rustc, "--target", args.target, "--print", "target-libdir"))
    if not target_libdir.is_dir() or not any(target_libdir.glob("libstd-*.rlib")):
        raise RuntimeError(f"Registered Rust sysroot is missing std for {args.target}: {target_libdir}")
    return detected, target_libdir


def main():
    args = parse_args()
    # Keep LLVM multicall names such as clang and llvm-ar: resolving these
    # symlinks changes which multicall program LLVM runs.
    for key in [
        "output", "evidence", "cargo", "rustc", "rust_sysroot_anchor", "config", "manifest",
        "workspace_manifest", "cc", "cxx", "ar", "linux_headers", "glibc_headers",
    ]:
        setattr(args, key, getattr(args, key).absolute())
    if args.openssl_headers:
        args.openssl_headers = args.openssl_headers.absolute()
    if args.cmake:
        args.cmake = args.cmake.absolute()
    if args.native_linker:
        for key in ("native_linker", "native_unwind", "native_gcc_s", "native_glibc"):
            setattr(args, key, getattr(args, key).absolute())
    arch = args.target.partition("-")[0]
    native_target = arch + "-linux-gnu"
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        temporary = Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="cargo-", dir=args.output.parent)))
        if args.native_source_root is not None:
            checkout = native_checkout(args.native_source_root)
            stack.callback(shutil.rmtree, checkout)
        else:
            checkout = temporary / "source"
        counts = stage_sources(args, checkout)
        # .. components must stay present in the Rust flags, because Cargo
        # includes their literal spelling in its filename hash.
        (checkout / "cmake/linux").mkdir(parents=True, exist_ok=True)
        gcc_root = checkout / "cmake/linux/../../contrib/sysroot" / ("linux-" + arch)
        libc_sysroot = gcc_root / native_target / "libc"
        if not libc_sysroot.is_dir():
            raise RuntimeError(f"Declared Cargo target sysroot was not staged: {libc_sysroot}")
        # Clang searches these two GNU system directories after its builtin headers.
        # Keep the CMake --sysroot operand: Cargo hashes it in archive member names.
        (libc_sysroot / "include").symlink_to(args.linux_headers, target_is_directory=True)
        (libc_sysroot / "usr/include").symlink_to(args.glibc_headers, target_is_directory=True)
        rust_sysroot, rust_target_libdir = selected_rust_sysroot(args)
        manifest = checkout / mounted_source(args.manifest, source_mounts(args))
        if not manifest.is_file():
            raise ValueError(f"Cargo manifest was not in the declared workspace source files: {manifest}")
        config = temporary / "config.toml"
        # CMake passes this spelling to Cargo. Rust embeds it in vendor
        # panic filenames even though it resolves to contrib/rust_vendor.
        (checkout / "contrib/corrosion-cmake").mkdir(exist_ok=True)
        config.write_text(cargo_config(args.config, checkout / "contrib/corrosion-cmake/../rust_vendor"))
        cargo_home = temporary / "cargo-home"
        cargo_home.mkdir()
        # Corrosion.cmake:740-746 computes this from workspace_manifest_path.
        # Rust's include!() embeds the literal OUT_DIR in panic locations; keep
        # CMake's "/./" spelling here instead of normalizing it through Path.
        # chdig and delta_kernel_ffi are imported from their own manifests;
        # args.workspace_manifest declares ClickHouse's shared source files.
        workspace_manifest = (
            manifest if args.cargo_package in {"chdig", "delta_kernel_ffi"}
            else checkout / mounted_source(args.workspace_manifest, source_mounts(args))
        )
        if not workspace_manifest.is_file():
            raise ValueError(f"Cargo workspace manifest was not declared: {workspace_manifest}")
        cmake_manifest = str(workspace_manifest)
        if args.cargo_package == "chdig":
            if workspace_manifest != checkout / "contrib/chdig/Cargo.toml":
                raise ValueError(f"Unexpected chdig manifest: {workspace_manifest}")
            # contrib/chdig-cmake imports ../chdig/Cargo.toml; Corrosion
            # hashes this literal spelling, retaining the parent directory.
            cmake_manifest = str(checkout) + "/contrib/chdig-cmake/../chdig/Cargo.toml"
        manifest_hash = hashlib.sha1(cmake_manifest.encode()).hexdigest()[:5]
        folder = workspace_manifest.parent.name + "_" + manifest_hash
        cargo_target = str(checkout) + "/build_bazel_reference_" + arch + "/./cargo/build/" + folder
        rust_flags = native_rust_flags(args, gcc_root, libc_sysroot)
        target_env = args.target.replace("-", "_")
        env = os.environ | {
            "CARGO_HOME": str(cargo_home),
            "CARGO_NET_OFFLINE": "true",
            "CARGO_BUILD_RUSTC": str(args.rustc),
            "RUSTC": str(args.rustc),
            "CARGO_ENCODED_RUSTFLAGS": "\x1f".join(rust_flags),
            "CARGO_ENCODED_RUSTDOCFLAGS": "\x1f".join(rust_flags),
            "CARGO_TARGET_" + target_env.upper() + "_LINKER": str(args.cc),
            "CC_" + target_env: str(args.cc),
            "CXX_" + target_env: str(args.cxx),
            "AR_" + target_env: str(args.ar),
            "CXXSTDLIB": "cxx",
            "SOURCE_DATE_EPOCH": "1",
        }
        if args.native_linker:
            # Cargo does not give target RUSTFLAGS to host build scripts,
            # even with a native --target. Ubuntu ARM runners lack host ld
            # and GCC development files. Supply those through the linker
            # driver, leaving Cargo's target RUSTFLAGS byte-for-byte intact.
            env["CARGO_TARGET_" + target_env.upper() + "_LINKER"] = str(cargo_linker(
                temporary / "clang-cargo-linker",
                args.cc, args.native_linker, gcc_root, libc_sysroot,
                args.native_unwind, args.native_gcc_s, args.native_glibc,
            ))
        if args.contrib_repositories is not None:
            if args.cargo_package != "_ch_rust_prql":
                raise ValueError("Only PRQL requests vendor git-describe metadata")
            repositories = json.loads(args.contrib_repositories.read_text())
            vendor = repositories["contrib/rust_vendor"]
            revision = vendor["commit"]
            if (vendor["name"] != "clickhouse_rust_vendor"
                    or not re.fullmatch("[0-9a-f]{40}", revision)):
                raise ValueError("Invalid pinned rust_vendor revision")
            # prqlc/build.rs uses vergen_gitcl's git describe in rust_vendor.
            # Native CMake's rust_vendor clone describes this pin with 9 hex
            # digits. The fetched archive has no .git; vergen_lib explicitly
            # accepts VERGEN_GIT_DESCRIBE as its no-Git override.
            env["VERGEN_GIT_DESCRIBE"] = revision[:9]
        if args.native_compilation:
            cflags, cxxflags = native_flags(args, checkout, gcc_root, libc_sysroot)
            env.pop("ARFLAGS", None)
            if args.openssl_headers:
                # Corrosion passes these twice; the final CFLAGS additionally
                # include OpenSSL's source headers. This tree has both the
                # original and the configured OpenSSL headers.
                cflags += ["-I", str(args.openssl_headers)]
                headers_out = temporary / "delta-ffi-headers"
                headers_out.mkdir()
                env.update({
                    "CARGO_TARGET_DIR": str(headers_out),
                    "OPENSSL_LIBS": "",
                    "OPENSSL_STATIC": "1",
                    "OPENSSL_LIB_DIR": str(args.openssl_headers),
                    "OPENSSL_INCLUDE_DIR": str(args.openssl_headers),
                    "LDFLAGS": "-nodefaultlibs -lc -lgcc",
                })
                # ffi/build.rs calls cbindgen twice; its nested cargo metadata
                # invocation must see the same offline vendor configuration.
                (cargo_home / "config.toml").write_text(config.read_text())
            env["CFLAGS"] = shlex.join(cflags)
            env["CXXFLAGS"] = shlex.join(cxxflags)
            build_tool_dirs = [str(args.cargo.parent), str(args.cc.parent)]
            if args.cmake:
                build_tool_dirs.insert(0, str(args.cmake.parent))
            env["PATH"] = os.pathsep.join(build_tool_dirs + [env.get("PATH", os.defpath)])
        # CMake's command supplies --target even for a native build; Cargo
        # then keeps host build-script flags separate from target Rust flags.
        command = [
            str(args.cargo), "rustc", "--lib", "--target=" + args.target,
            "--no-default-features",
            *(["--features=" + ",".join(args.cargo_feature)] if args.cargo_feature else []),
            "--package", args.cargo_package, "--crate-type=staticlib",
            "--manifest-path", str(manifest), "--target-dir", cargo_target,
            "--profile=release", "--config=" + str(config), "--offline", "--locked",
            "--", "-Cdefault-linker-libraries=no",
        ]
        subprocess.run(command, cwd=manifest.parent, env=env, check=True)
        built = Path(cargo_target) / args.target / "release" / ("lib" + args.cargo_package + ".a")
        if not built.is_file():
            raise FileNotFoundError(f"Cargo did not produce the expected raw archive: {built}")
        shutil.copyfile(built, args.output)
        members = version(args.ar, "t", str(built)).splitlines()
        compiled_path_crates = {
            name: [member for member in members if member.startswith(name + "-")]
            for name in [args.cargo_package, *args.path_crate]
        }
        missing_crates = [name for name, objects in compiled_path_crates.items() if not objects]
        if missing_crates:
            raise ValueError(f"Raw Cargo archive {args.cargo_package} is missing path crate members: {missing_crates}")
        evidence = {
            "package": args.cargo_package,
            "target": args.target,
            "native_source_root": str(args.native_source_root) if args.native_source_root is not None else None,
            "compiled_path_crates": compiled_path_crates,
            "cargo_command": command,
            "rustflags": rust_flags,
            "rustc_used": str(args.rustc),
            "rustc_default_sysroot": str(rust_sysroot),
            "rustc_target_libdir": str(rust_target_libdir),
            "extra_rustc_args": [],
            "vendor_git_describe": env.get("VERGEN_GIT_DESCRIBE"),
            "cargo_config": config.read_text(),
            "cargo_version": version(args.cargo, "-Vv"),
            "rustc_version": version(args.rustc, "-vV"),
            "rustc_sha256": digest(args.rustc),
            "cargo_sha256": digest(args.cargo),
            "declared_sources": counts,
            "native_environment": {
                name: env[name] for name in [
                    "CFLAGS", "CXXFLAGS", "CXXSTDLIB", "OPENSSL_LIBS",
                    "OPENSSL_STATIC", "OPENSSL_LIB_DIR", "OPENSSL_INCLUDE_DIR",
                    "LDFLAGS",
                ] if name in env
            } if args.native_compilation else None,
            "output_sha256": digest(args.output),
            "output_bytes": args.output.stat().st_size,
        }
        args.evidence.write_text(json.dumps(evidence, indent=2) + "\n")


if __name__ == "__main__":
    main()
