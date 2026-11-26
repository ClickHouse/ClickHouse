import argparse
import glob
import os
import platform
import shutil

from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

TEMP = "/tmp"

LLVM_VERSION = "23"

LLVM_SOURCE_DIR = f"{TEMP}/llvm-project"
NINJA_SOURCE_DIR = f"{TEMP}/ninja-src"
NINJA_BUILD_DIR = f"{TEMP}/ninja-build"
NINJA_LOG_SHARE_DIR = "share/clickhouse-build"
JEMALLOC_VERSION = "5.4.0"
JEMALLOC_URL = "https://github.com/jemalloc/jemalloc/releases/download/5.4.0/jemalloc-5.4.0.tar.bz2"
JEMALLOC_SHA256 = "200776fac271093e7c2f21edd6d62657ecd2be578d9328633f2a86bfa6ef4f1d"
JEMALLOC_SOURCE_DIR = f"{TEMP}/jemalloc-{JEMALLOC_VERSION}"
JEMALLOC_LIBRARY = f"{JEMALLOC_SOURCE_DIR}/lib/libjemalloc_pic.a"

# The toolchain executables are linked statically against musl, so that they run on any Linux
# regardless of its libc. Their dependencies (the C++ runtime, compiler-rt, zlib, zstd) are built
# against musl into a sysroot of their own. musl is ClickHouse's patched fork, at the tip of its
# release branch.
MUSL_REPO = "https://github.com/ClickHouse/musl.git"
MUSL_BRANCH = "ClickHouse/v1.2.6"
MUSL_SOURCE_DIR = f"{TEMP}/musl"
MUSL_BUILD_DIR = f"{TEMP}/musl-build"
ZLIB_VERSION = "1.3.2"
ZLIB_URL = f"https://github.com/madler/zlib/releases/download/v{ZLIB_VERSION}/zlib-{ZLIB_VERSION}.tar.gz"
ZLIB_SHA256 = "bb329a0a2cd0274d05519d61c667c062e06990d72e125ee2dfa8de64f0119d16"
ZSTD_VERSION = "1.6.0"
ZSTD_URL = f"https://github.com/facebook/zstd/releases/download/v{ZSTD_VERSION}/zstd-{ZSTD_VERSION}.tar.gz"
ZSTD_SHA256 = "2ba599ae2a257976278e7e71e998433a387bfd18aa7b78678229fd5d84f5e9b8"
MUSL_SYSROOT = f"{TEMP}/musl-sysroot"
# A copy of the distro clang's resource directory (its headers) where compiler-rt for the musl
# target is installed, passed with `-resource-dir` instead of writing into the distro package.
MUSL_RESOURCE_DIR = f"{TEMP}/musl-clang-resource"
MUSL_COMPILER_RT_BUILD_DIR = f"{TEMP}/musl-compiler-rt"
MUSL_CXX_RUNTIMES_BUILD_DIR = f"{TEMP}/musl-cxx-runtimes"
# musl's default thread stack is 128 KiB, too small for LLVM's recursive code on lld's and
# ThinLTO's worker threads. musl takes the default from `PT_GNU_STACK`, which this sets.
MUSL_THREAD_STACK_SIZE = 8 * 1024 * 1024
STAGE1_BUILD_DIR = f"{TEMP}/toolchain-stage1"
STAGE1_INSTALL_DIR = f"{TEMP}/toolchain-stage1-install"
STAGE2_BUILD_DIR = f"{TEMP}/toolchain-stage2"
STAGE2_INSTALL_DIR = f"{TEMP}/toolchain-stage2-install"
CH_PROFILE_BUILD_DIR = f"{TEMP}/toolchain-ch-profile"
CH_BOLT_BUILD_DIR = f"{TEMP}/toolchain-ch-bolt"
PROFDATA_PATH = f"{TEMP}/clang.profdata"
BOLT_PROFILES_DIR = f"{TEMP}/bolt-profiles"
BOLT_FDATA_PATH = f"{TEMP}/bolt.fdata"

REPO_PATH = "/ClickHouse"

OUTPUT_DIR = f"{Utils.cwd()}/ci/tmp"

# BOLT profile collection is time-limited to avoid filling disk with fdata files
# (~15 MB per clang process). It runs at full parallelism: the limit is on the
# number of profiles, so idle cores would only stretch the same work out in time.
# Four jobs for 20 minutes collected ~1600 profiles on amd64; 150 seconds at full
# parallelism collected 755 on amd64 and 1248 on aarch64, so this collects about as many.
BOLT_PROFILE_TIMEOUT = 300

# LLVM projects for the final toolchain (note: "all" doesn't work with cmake).
# `bolt` is needed by the BOLT stage of this job and by the ClickHouse BOLT builds.
STAGE2_LLVM_PROJECTS = "clang;lld;bolt"

# Backends of the final toolchain: the architectures ClickHouse is built for
# (Linux, FreeBSD and Darwin targets). Emscripten ships its own LLVM for wasm.
STAGE2_LLVM_TARGETS = "X86;AArch64;PowerPC;SystemZ;RISCV;LoongArch"

# What the toolchain archive contains, i.e. what gets built and installed in stage 2.
# Everything that CI invokes from the toolchain must be listed here: ClickHouse's CMake
# looks up `ld.lld`, `llvm-ar` and the other binutils in the compiler's directory first, and
# `/usr/local/bin` precedes the distro LLVM on `PATH`, so `llvm-profdata`, `llvm-cov` and
# `llvm-bolt` must match the compiler's profile formats. `clang-tidy` comes from the distro
# package: `cmake/clang_tidy.cmake` prefers the versioned name.
STAGE2_DISTRIBUTION_COMPONENTS = [
    "clang",
    "clang-resource-headers",
    "lld",
    "builtins",
    "runtimes",
    "bolt",
    "llvm-ar",
    "llvm-ranlib",
    "llvm-objcopy",
    "llvm-strip",
    "llvm-install-name-tool",
    "llvm-nm",
    "llvm-profdata",
    "llvm-cov",
    "llvm-symbolizer",
    "llvm-addr2line",
    "llvm-cxxfilt",
    "llvm-objdump",
    "llvm-readobj",
    "llvm-readelf",
    "llvm-dwarfdump",
    "llvm-dwp",
    "llvm-size",
    "llvm-strings",
]

# Cross-target triples for compiler-rt builtins. Builtins are freestanding C code
# (no sysroot needed), built via LLVM_BUILTIN_TARGETS so the toolchain is
# self-contained for all ClickHouse cross-compilation targets.
# Must match the architectures supported in contrib/compiler-rt-cmake/CMakeLists.txt.
CROSS_BUILTIN_TARGETS = [
    ("x86_64-unknown-linux-gnu", "Linux"),
    ("aarch64-unknown-linux-gnu", "Linux"),
    ("s390x-unknown-linux-gnu", "Linux"),
    ("powerpc64le-unknown-linux-gnu", "Linux"),
    ("riscv64-unknown-linux-gnu", "Linux"),
    ("loongarch64-unknown-linux-gnu", "Linux"),
    ("x86_64-pc-freebsd14", "FreeBSD"),
    ("aarch64-unknown-freebsd14", "FreeBSD"),
    ("powerpc64le-unknown-freebsd14", "FreeBSD"),
]


class JobStages(metaclass=MetaClasses.WithIter):
    CLONE_LLVM = "clone_llvm"
    BUILD_MUSL_SYSROOT = "build_musl_sysroot"
    BUILD_NINJA = "build_ninja"
    BUILD_JEMALLOC = "build_jemalloc"
    STAGE1_BUILD = "stage1_build"
    PROFILE_COLLECTION = "profile_collection"
    STAGE2_BUILD = "stage2_build"
    BOLT_OPTIMIZATION = "bolt_optimization"
    PACKAGE = "package"


def get_arch():
    machine = platform.machine()
    if machine == "x86_64":
        return "x86_64"
    elif machine == "aarch64":
        return "aarch64"
    else:
        raise RuntimeError(f"Unsupported architecture: {machine}")


def get_toolchain_file():
    arch = get_arch()
    return f"{REPO_PATH}/cmake/linux/toolchain-{arch}.cmake"


def get_musl_triple():
    return f"{get_arch()}-unknown-linux-musl"


def musl_c_flags():
    return (
        f"--target={get_musl_triple()} --sysroot={MUSL_SYSROOT}"
        f" -resource-dir={MUSL_RESOURCE_DIR}"
    )


def musl_cxx_flags():
    # `-nostdinc++`: the distro's libc++ headers next to clang are configured for glibc
    return (
        f"{musl_c_flags()} -stdlib=libc++ -nostdinc++"
        f" -isystem {MUSL_SYSROOT}/usr/include/c++/v1"
    )


def musl_exe_linker_flags():
    return (
        f"-static --rtlib=compiler-rt --unwindlib=libunwind"
        f" -Wl,-z,stack-size={MUSL_THREAD_STACK_SIZE}"
    )


def musl_llvm_cmake_args():
    """CMake arguments that build LLVM itself as static executables against the musl sysroot.

    The sysroot is given in the flags and `CMAKE_FIND_ROOT_PATH` rather than in `CMAKE_SYSROOT`,
    because LLVM passes `CMAKE_SYSROOT` on to the runtimes that stage 2 builds for the
    toolchain's own (glibc) default target.
    """
    return (
        f' -DCMAKE_C_FLAGS="{musl_c_flags()}"'
        f' -DCMAKE_CXX_FLAGS="{musl_cxx_flags()}"'
        f" -DCMAKE_FIND_ROOT_PATH={MUSL_SYSROOT}"
        f" -DCMAKE_FIND_ROOT_PATH_MODE_LIBRARY=ONLY"
        f" -DCMAKE_FIND_ROOT_PATH_MODE_INCLUDE=ONLY"
        f" -DCMAKE_FIND_ROOT_PATH_MODE_PACKAGE=ONLY"
        f" -DLLVM_USE_STATIC_ZSTD=ON"
        # A static executable cannot `dlopen`
        f" -DLLVM_ENABLE_PLUGINS=OFF"
    )


def shell_is_elf(path):
    """A shell condition that is true if :param path: is an ELF file (the image has no `file`)."""
    return f'[ "$(head -c 4 "{path}" | tail -c 3)" = ELF ]'


def shell_is_static(path):
    """A shell condition that is true if :param path: is a static (non-PIE) executable: one
    without a program interpreter or a dynamic section."""
    return (
        f'! llvm-readelf-{LLVM_VERSION} --program-headers "{path}"'
        f" | grep -qE '^ +(INTERP|DYNAMIC) '"
    )


def download_and_extract(name, url, sha256):
    tarball = f"{TEMP}/{os.path.basename(url)}"
    return Result.from_commands_run(
        name=f"Download {name}",
        command=[
            f"curl -fsSL -o {tarball} {url}",
            f"echo '{sha256}  {tarball}' | sha256sum -c",
            f"tar -xzf {tarball} -C {TEMP}",
        ],
        retries=3,
    )


def clean_dirs(*dirs):
    for d in dirs:
        if os.path.exists(d):
            print(f"Cleaning {d}")
            shutil.rmtree(d, ignore_errors=True)


def parse_args():
    parser = argparse.ArgumentParser(description="Build PGO+BOLT optimized clang")
    parser.add_argument(
        "--param",
        help="Optional user-defined job start stage (for local run)",
        default=None,
    )
    return parser.parse_args()


def main():
    args = parse_args()

    stages = list(JobStages)
    stage = args.param or JobStages.CLONE_LLVM
    if stage:
        assert stage in JobStages, f"--param must be one of [{list(JobStages)}]"
        print(f"Job will start from stage [{stage}]")
        while stage in stages:
            stages.pop(0)
        stages.insert(0, stage)

    arch = get_arch()
    toolchain_file = get_toolchain_file()
    print(f"Building toolchain for {arch}")
    print(f"Using ClickHouse toolchain file: {toolchain_file}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    res = True
    results = []

    if os.getuid() == 0:
        Shell.check(f"git config --global --add safe.directory {Utils.cwd()}")

    # Stage 0: Clone LLVM
    if res and JobStages.CLONE_LLVM in stages:
        clean_dirs(LLVM_SOURCE_DIR)
        results.append(
            Result.from_commands_run(
                name="Clone LLVM",
                command=(
                    f"git clone --depth 1 --branch release/{LLVM_VERSION}.x"
                    f" https://github.com/llvm/llvm-project.git {LLVM_SOURCE_DIR}"
                ),
                retries=3,
            )
        )
        res = results[-1].is_ok()

    CUSTOM_NINJA = f"{NINJA_BUILD_DIR}/ninja"
    # Stage 0.5: Build the musl sysroot that the toolchain executables are statically linked against
    if res and JobStages.BUILD_MUSL_SYSROOT in stages:
        musl_triple = get_musl_triple()
        zlib_source_dir = f"{TEMP}/zlib-{ZLIB_VERSION}"
        zstd_source_dir = f"{TEMP}/zstd-{ZSTD_VERSION}"
        clean_dirs(
            MUSL_SYSROOT,
            MUSL_RESOURCE_DIR,
            MUSL_SOURCE_DIR,
            MUSL_BUILD_DIR,
            MUSL_COMPILER_RT_BUILD_DIR,
            MUSL_CXX_RUNTIMES_BUILD_DIR,
            zlib_source_dir,
            zstd_source_dir,
        )
        cc = f"clang-{LLVM_VERSION} {musl_c_flags()}"

        results.append(
            Result.from_commands_run(
                name=f"Clone musl {MUSL_BRANCH}",
                command=[
                    f"git clone --depth 1 --branch {MUSL_BRANCH} {MUSL_REPO} {MUSL_SOURCE_DIR}",
                    f"git -C {MUSL_SOURCE_DIR} log -1 --format='musl commit %H'",
                ],
                retries=3,
            )
        )
        res = results[-1].is_ok()

        for name, url, sha256 in [
            (f"zlib {ZLIB_VERSION}", ZLIB_URL, ZLIB_SHA256),
            (f"zstd {ZSTD_VERSION}", ZSTD_URL, ZSTD_SHA256),
        ]:
            if res:
                results.append(download_and_extract(name, url, sha256))
                res = results[-1].is_ok()

        # The unwind tables let exceptions and backtraces pass through musl's C code. Not
        # `--enable-debug`: its `add-cfi` scripts would add CFI to the assembly that the fork
        # already annotates by hand (`__restore_rt`), and ClickHouse's own build does not run them.
        # The kernel (uapi) headers do not depend on the libc, so the distro's are reused.
        if res:
            results.append(
                Result.from_commands_run(
                    name="Build musl",
                    command=[
                        f"mkdir -p {MUSL_BUILD_DIR}",
                        f"cd {MUSL_BUILD_DIR} && {MUSL_SOURCE_DIR}/configure"
                        f' CC="clang-{LLVM_VERSION} --target={musl_triple}"'
                        f' CFLAGS="-funwind-tables -fasynchronous-unwind-tables"'
                        f" AR=llvm-ar-{LLVM_VERSION} RANLIB=llvm-ranlib-{LLVM_VERSION}"
                        f" --prefix={MUSL_SYSROOT}/usr --disable-shared --disable-wrapper",
                        f"make -C {MUSL_BUILD_DIR} -j$(nproc) install",
                        f"cp -r /usr/include/linux /usr/include/asm-generic {MUSL_SYSROOT}/usr/include/",
                        f"cp -r /usr/include/{arch}-linux-gnu/asm {MUSL_SYSROOT}/usr/include/asm",
                        # The licenses of the libraries linked into the toolchain executables ship with them
                        f"install -D -m 644 {MUSL_SOURCE_DIR}/COPYRIGHT {MUSL_SYSROOT}/share/doc/musl/COPYRIGHT",
                        f"install -D -m 644 {zlib_source_dir}/LICENSE {MUSL_SYSROOT}/share/doc/zlib/LICENSE",
                        f"install -D -m 644 {zstd_source_dir}/LICENSE {MUSL_SYSROOT}/share/doc/zstd/LICENSE",
                    ],
                )
            )
            res = results[-1].is_ok()

        # compiler-rt for the musl target: the builtins and crtbegin/crtend replace libgcc, and the
        # profile runtime is linked into the instrumented stage 1 clang
        if res:
            compiler_rt_cmake_cmd = (
                f"cmake -G Ninja"
                f" -DLLVM_ENABLE_RUNTIMES=compiler-rt"
                f" -DCMAKE_BUILD_TYPE=Release"
                f" -DCMAKE_C_COMPILER=clang-{LLVM_VERSION}"
                f" -DCMAKE_CXX_COMPILER=clang++-{LLVM_VERSION}"
                f" -DCMAKE_C_COMPILER_TARGET={musl_triple}"
                f" -DCMAKE_CXX_COMPILER_TARGET={musl_triple}"
                f" -DCMAKE_ASM_COMPILER_TARGET={musl_triple}"
                f' -DCMAKE_C_FLAGS="{musl_c_flags()}"'
                f' -DCMAKE_CXX_FLAGS="{musl_c_flags()}"'
                f' -DCMAKE_ASM_FLAGS="{musl_c_flags()}"'
                # Nothing can be linked before the builtins exist
                f" -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY"
                f" -DCOMPILER_RT_DEFAULT_TARGET_ONLY=ON"
                f" -DCOMPILER_RT_BUILD_BUILTINS=ON"
                f" -DCOMPILER_RT_BUILD_CRT=ON"
                f" -DCOMPILER_RT_BUILD_PROFILE=ON"
                f" -DCOMPILER_RT_BUILD_SANITIZERS=OFF"
                f" -DCOMPILER_RT_BUILD_XRAY=OFF"
                f" -DCOMPILER_RT_BUILD_LIBFUZZER=OFF"
                f" -DCOMPILER_RT_BUILD_MEMPROF=OFF"
                f" -DCOMPILER_RT_BUILD_ORC=OFF"
                f" -DCOMPILER_RT_BUILD_GWP_ASAN=OFF"
                f" -DCOMPILER_RT_BUILD_CTX_PROFILE=OFF"
                f" -DCOMPILER_RT_INCLUDE_TESTS=OFF"
                f" -DLLVM_INCLUDE_TESTS=OFF"
                f" -DLLVM_ENABLE_PER_TARGET_RUNTIME_DIR=ON"
                f" -DCMAKE_INSTALL_PREFIX={MUSL_RESOURCE_DIR}"
                f" -DCOMPILER_RT_INSTALL_PATH={MUSL_RESOURCE_DIR}"
                f" -S {LLVM_SOURCE_DIR}/runtimes"
                f" -B {MUSL_COMPILER_RT_BUILD_DIR}"
            )
            results.append(
                Result.from_commands_run(
                    name="Build compiler-rt for musl",
                    command=[
                        f"mkdir -p {MUSL_RESOURCE_DIR}",
                        f'cp -r "$(clang-{LLVM_VERSION} -print-resource-dir)/include" {MUSL_RESOURCE_DIR}/',
                        compiler_rt_cmake_cmd,
                        f"ninja -C {MUSL_COMPILER_RT_BUILD_DIR} install",
                        # Fails unless the driver finds the builtins where they were installed
                        f'test -f "$({cc} --rtlib=compiler-rt -print-libgcc-file-name)"',
                    ],
                )
            )
            res = results[-1].is_ok()

        # libc++ with libc++abi merged into `libc++.a`, so that `-stdlib=libc++` links both,
        # and libunwind, linked with `--unwindlib=libunwind`
        if res:
            cxx_runtimes_cmake_cmd = (
                f"cmake -G Ninja"
                f' -DLLVM_ENABLE_RUNTIMES="libunwind;libcxxabi;libcxx"'
                f" -DCMAKE_BUILD_TYPE=Release"
                f" -DCMAKE_C_COMPILER=clang-{LLVM_VERSION}"
                f" -DCMAKE_CXX_COMPILER=clang++-{LLVM_VERSION}"
                f" -DCMAKE_C_COMPILER_TARGET={musl_triple}"
                f" -DCMAKE_CXX_COMPILER_TARGET={musl_triple}"
                f" -DCMAKE_ASM_COMPILER_TARGET={musl_triple}"
                f' -DCMAKE_C_FLAGS="{musl_c_flags()}"'
                f' -DCMAKE_CXX_FLAGS="{musl_c_flags()}"'
                f' -DCMAKE_ASM_FLAGS="{musl_c_flags()}"'
                # Nothing can be linked before libunwind exists
                f" -DCMAKE_TRY_COMPILE_TARGET_TYPE=STATIC_LIBRARY"
                # Without linking, `check_library_exists` finds any symbol. This one changes the
                # code: musl has no `__cxa_thread_atexit_impl`, so libc++abi must use its own.
                f" -DLIBCXXABI_HAS_CXA_THREAD_ATEXIT_IMPL=OFF"
                f" -DLLVM_ENABLE_PER_TARGET_RUNTIME_DIR=OFF"
                f" -DLLVM_INCLUDE_TESTS=OFF"
                f" -DLIBUNWIND_INCLUDE_TESTS=OFF"
                f" -DLIBCXXABI_INCLUDE_TESTS=OFF"
                f" -DLIBCXX_INCLUDE_TESTS=OFF"
                f" -DLIBCXX_INCLUDE_BENCHMARKS=OFF"
                f" -DLIBUNWIND_ENABLE_SHARED=OFF"
                f" -DLIBCXXABI_ENABLE_SHARED=OFF"
                f" -DLIBCXX_ENABLE_SHARED=OFF"
                f" -DLIBUNWIND_USE_COMPILER_RT=ON"
                f" -DLIBCXXABI_USE_COMPILER_RT=ON"
                f" -DLIBCXX_USE_COMPILER_RT=ON"
                f" -DLIBCXXABI_USE_LLVM_UNWINDER=ON"
                f" -DLIBCXX_ENABLE_STATIC_ABI_LIBRARY=ON"
                f" -DLIBCXX_HAS_MUSL_LIBC=ON"
                f" -DCMAKE_INSTALL_PREFIX={MUSL_SYSROOT}/usr"
                f" -S {LLVM_SOURCE_DIR}/runtimes"
                f" -B {MUSL_CXX_RUNTIMES_BUILD_DIR}"
            )
            results.append(
                Result.from_commands_run(
                    name="Build libc++ for musl",
                    command=[
                        cxx_runtimes_cmake_cmd,
                        # libc++'s header scripts import their modules from the script's directory,
                        # which `PYTHONSAFEPATH` (set by praktika) keeps off `sys.path`
                        f"env -u PYTHONSAFEPATH ninja -C {MUSL_CXX_RUNTIMES_BUILD_DIR} install",
                    ],
                )
            )
            res = results[-1].is_ok()

        # Exceptions across threads exercise libc++, libc++abi, libunwind and the thread stack size
        if res:
            test_source = f"{TEMP}/musl-sysroot-test.cpp"
            test_binary = f"{TEMP}/musl-sysroot-test"
            with open(test_source, "w") as f:
                f.write(
                    "#include <cstdio>\n"
                    "#include <stdexcept>\n"
                    "#include <thread>\n"
                    "int main()\n"
                    "{\n"
                    "    int status = 1;\n"
                    "    std::thread thread([&]\n"
                    "    {\n"
                    "        try { throw std::runtime_error(\"test\"); }\n"
                    "        catch (const std::exception &) { status = 0; }\n"
                    "    });\n"
                    "    thread.join();\n"
                    "    std::puts(\"OK\");\n"
                    "    return status;\n"
                    "}\n"
                )
            results.append(
                Result.from_commands_run(
                    name="Test musl sysroot",
                    command=[
                        f"clang++-{LLVM_VERSION} {musl_cxx_flags()} {musl_exe_linker_flags()}"
                        f" -fuse-ld=lld -O2 {test_source} -o {test_binary}",
                        f"{test_binary}",
                        shell_is_static(test_binary),
                    ],
                )
            )
            res = results[-1].is_ok()

        # zlib and zstd are linked into LLVM with `LLVM_ENABLE_ZLIB` and `LLVM_ENABLE_ZSTD`
        if res:
            results.append(
                Result.from_commands_run(
                    name="Build zlib for musl",
                    command=[
                        f'cd {zlib_source_dir} && CC="{cc}" CFLAGS="-O2 -fPIC"'
                        f' LDFLAGS="{musl_exe_linker_flags()} -fuse-ld=lld"'
                        f" ./configure --static --prefix={MUSL_SYSROOT}/usr",
                        f"make -C {zlib_source_dir} -j$(nproc) install",
                    ],
                )
            )
            res = results[-1].is_ok()

        if res:
            zstd_make_args = f'CC="{cc}" CFLAGS="-O3 -fPIC" PREFIX={MUSL_SYSROOT}/usr'
            results.append(
                Result.from_commands_run(
                    name="Build zstd for musl",
                    command=[
                        f"make -C {zstd_source_dir}/lib -j$(nproc) {zstd_make_args} libzstd.a",
                        f"make -C {zstd_source_dir}/lib {zstd_make_args} install-static install-includes",
                    ],
                )
            )
            res = results[-1].is_ok()

        clean_dirs(
            MUSL_SOURCE_DIR,
            MUSL_BUILD_DIR,
            MUSL_COMPILER_RT_BUILD_DIR,
            MUSL_CXX_RUNTIMES_BUILD_DIR,
            zlib_source_dir,
            zstd_source_dir,
        )

    # Stage 0.6: Build custom Ninja with timing-based scheduling
    # Ninja 1.12.1 has prev_elapsed_time_millis on Edge (populated from .ninja_log).
    # We patch EdgeWeightHeuristic to use historical build times as edge weights,
    # giving 11-21% build time reduction via critical path scheduling.
    if res and JobStages.BUILD_NINJA in stages:
        clean_dirs(NINJA_SOURCE_DIR, NINJA_BUILD_DIR)
        results.append(
            Result.from_commands_run(
                name="Clone Ninja v1.12.1",
                command=(
                    f"git clone --depth 1 --branch v1.12.1"
                    f" https://github.com/ninja-build/ninja.git {NINJA_SOURCE_DIR}"
                ),
                retries=3,
            )
        )
        res = results[-1].is_ok()

        if res:
            # Patch EdgeWeightHeuristic to use .ninja_log timing data
            results.append(
                Result.from_commands_run(
                    name="Patch Ninja EdgeWeightHeuristic",
                    command=(
                        f"sed -i 's/return edge->is_phony() ? 0 : 1;/"
                        f"int64_t w = edge->prev_elapsed_time_millis < 0 ? 1 : edge->prev_elapsed_time_millis;\\n"
                        f"  return edge->is_phony() ? 0 : w;/' {NINJA_SOURCE_DIR}/src/build.cc"
                    ),
                )
            )
            res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Build Ninja",
                    command=[
                        # Statically against musl, as it ships in the toolchain archive
                        f"cmake -B {NINJA_BUILD_DIR} -S {NINJA_SOURCE_DIR} -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF"
                        f" -DCMAKE_C_COMPILER=clang-{LLVM_VERSION} -DCMAKE_CXX_COMPILER=clang++-{LLVM_VERSION}"
                        f' -DCMAKE_C_FLAGS="{musl_c_flags()}" -DCMAKE_CXX_FLAGS="{musl_cxx_flags()}"'
                        f' -DCMAKE_EXE_LINKER_FLAGS="{musl_exe_linker_flags()} -fuse-ld=lld"',
                        f"cmake --build {NINJA_BUILD_DIR}",
                    ],
                )
            )
            res = results[-1].is_ok()

        if res:
            print(f"Custom Ninja built at {CUSTOM_NINJA}")
            # Clean up source dir to save space
            clean_dirs(NINJA_SOURCE_DIR)

    # Stage 0.7: Build jemalloc, which the final toolchain executables link instead of musl's malloc
    if res and JobStages.BUILD_JEMALLOC in stages:
        clean_dirs(JEMALLOC_SOURCE_DIR)
        jemalloc_tarball = f"{TEMP}/jemalloc-{JEMALLOC_VERSION}.tar.bz2"
        results.append(
            Result.from_commands_run(
                name=f"Download jemalloc {JEMALLOC_VERSION}",
                command=f"curl -fsSL -o {jemalloc_tarball} {JEMALLOC_URL}",
                retries=3,
            )
        )
        res = results[-1].is_ok()

        if res:
            # jemalloc fails to start if the kernel page is larger than its own; aarch64 kernels use up to 64 KiB.
            # Huge pages (2 MiB with 4 KiB pages) back the heap and metadata unless MALLOC_CONF says otherwise.
            # `--disable-cxx`: libc++abi's `operator new` calls `malloc`, which is jemalloc's.
            lg_page = 16 if arch == "aarch64" else 12
            results.append(
                Result.from_commands_run(
                    name="Build jemalloc",
                    command=[
                        f"echo '{JEMALLOC_SHA256}  {jemalloc_tarball}' | sha256sum -c",
                        f"tar -xjf {jemalloc_tarball} -C {TEMP}",
                        f"cd {JEMALLOC_SOURCE_DIR} && ./configure"
                        f' CC="clang-{LLVM_VERSION} {musl_c_flags()}"'
                        f' LDFLAGS="{musl_exe_linker_flags()} -fuse-ld=lld"'
                        f" EXTRA_CFLAGS=-g0"
                        f" --disable-cxx"
                        f" --with-lg-page={lg_page} --with-lg-hugepage=21"
                        f" --with-malloc-conf=thp:always,metadata_thp:always",
                        f"make -C {JEMALLOC_SOURCE_DIR} -j$(nproc) build_lib_static",
                    ],
                )
            )
            res = results[-1].is_ok()

    # Stage 1: Build instrumented clang for PGO profile collection
    # Only needs clang + lld (for compilation/linking) and compiler-rt (profiling runtime)
    if res and JobStages.STAGE1_BUILD in stages:
        clean_dirs(STAGE1_BUILD_DIR, STAGE1_INSTALL_DIR)
        os.makedirs(STAGE1_BUILD_DIR, exist_ok=True)

        cmake_cmd = (
            f"cmake -G Ninja"
            f' -DLLVM_ENABLE_PROJECTS="clang;lld"'
            f' -DLLVM_ENABLE_RUNTIMES="compiler-rt"'
            f" -DLLVM_TARGETS_TO_BUILD=Native"
            f" -DCMAKE_BUILD_TYPE=Release"
            f" -DLLVM_BUILD_INSTRUMENTED=IR"
            f" -DCMAKE_C_COMPILER=clang-{LLVM_VERSION}"
            f" -DCMAKE_CXX_COMPILER=clang++-{LLVM_VERSION}"
            f" -DLLVM_ENABLE_LLD=ON"
            f"{musl_llvm_cmake_args()}"
            f' -DCMAKE_EXE_LINKER_FLAGS="{musl_exe_linker_flags()}"'
            f" -DLLVM_ENABLE_TERMINFO=OFF"
            f" -DLLVM_ENABLE_ZLIB=FORCE_ON"
            f" -DLLVM_ENABLE_ZSTD=FORCE_ON"
            f" -DCMAKE_INSTALL_PREFIX={STAGE1_INSTALL_DIR}"
            f" -S {LLVM_SOURCE_DIR}/llvm"
            f" -B {STAGE1_BUILD_DIR}"
        )
        results.append(
            Result.from_commands_run(
                name="Stage 1 CMake (instrumented clang)",
                command=cmake_cmd,
            )
        )
        res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 1 Build (instrumented clang)",
                    command=f"{CUSTOM_NINJA} -C {STAGE1_BUILD_DIR} clang lld",
                )
            )
            res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 1 Install",
                    command=(
                        f"{CUSTOM_NINJA} -C {STAGE1_BUILD_DIR}"
                        f" install-clang install-clang-resource-headers install-lld"
                    ),
                )
            )
            res = results[-1].is_ok()

        # Install compiler-rt headers (xray, sanitizer, etc.) into the clang resource
        # directory so that ClickHouse can find <xray/xray_interface.h> when compiled
        # with this toolchain.
        if res:
            resource_dirs = glob.glob(
                f"{STAGE1_INSTALL_DIR}/lib/clang/*/include"
            )
            if resource_dirs:
                resource_include = resource_dirs[0]
                results.append(
                    Result.from_commands_run(
                        name="Install compiler-rt headers",
                        command=(
                            f"cp -r {LLVM_SOURCE_DIR}/compiler-rt/include/xray"
                            f" {resource_include}/xray"
                        ),
                    )
                )
                res = results[-1].is_ok()
            else:
                print(
                    f"WARNING: No clang resource directory found in"
                    f" {STAGE1_INSTALL_DIR}/lib/clang/*/include"
                )

    # Stage 2: Profile collection - build ClickHouse with instrumented clang
    if res and JobStages.PROFILE_COLLECTION in stages:
        clean_dirs(CH_PROFILE_BUILD_DIR)

        # Checkout submodules first
        results.append(
            Result.from_commands_run(
                name="Checkout submodules for profile collection",
                command=[
                    f"git -C {REPO_PATH} submodule sync",
                    f"git -C {REPO_PATH} submodule init",
                    f"{REPO_PATH}/contrib/update-submodules.sh --max-procs 10",
                ],
                retries=3,
            )
        )
        res = results[-1].is_ok()

        if res:
            cmake_cmd = (
                f"cmake"
                f" -DCMAKE_BUILD_TYPE=None"
                f" -DENABLE_THINLTO=0"
                f" -DCMAKE_C_COMPILER={STAGE1_INSTALL_DIR}/bin/clang"
                f" -DCMAKE_CXX_COMPILER={STAGE1_INSTALL_DIR}/bin/clang++"
                # Link with the distro lld: the instrumented one never writes a profile
                # (lld exits with `_Exit`), and its multithreaded value profiling uses
                # non-atomic updates on aarch64, which can make a link spin forever.
                f" -DLINKER_NAME=ld.lld-{LLVM_VERSION}"
                f" -DCOMPILER_CACHE=disabled"
                f" -DENABLE_TESTS=0"
                f" -DENABLE_UTILS=0"
                f" -DCMAKE_TOOLCHAIN_FILE={toolchain_file}"
                f" {REPO_PATH}"
                f" -B {CH_PROFILE_BUILD_DIR}"
            )
            results.append(
                Result.from_commands_run(
                    name="Profile collection CMake",
                    command=cmake_cmd,
                )
            )
            res = results[-1].is_ok()

        if res:
            # Build may fail at link step but profraw files from compilation
            # steps are still useful for PGO
            build_result = Result.from_commands_run(
                name="Profile collection build (ClickHouse)",
                command=f"{CUSTOM_NINJA} -C {CH_PROFILE_BUILD_DIR} clickhouse",
            )
            if not build_result.is_ok():
                print(
                    "ClickHouse build finished with errors"
                    " (link failures with instrumented compiler are expected)."
                    " Profraw files from compilation steps should still be available."
                )
                build_result.status = Result.Status.OK
                build_result.info = "Build failed at link step (expected); profraw files collected"
            results.append(build_result)

        # Merge profraw files with the matching `llvm-profdata` (it supports the zlib-compressed
        # profile format the instrumented clang can emit)
        profraw_dir = f"{STAGE1_BUILD_DIR}/profiles/"
        if os.path.isdir(profraw_dir) and os.listdir(profraw_dir):
            results.append(
                Result.from_commands_run(
                    name="Merge PGO profiles",
                    command=(
                        f"llvm-profdata-{LLVM_VERSION} merge"
                        f" -output={PROFDATA_PATH}"
                        f" {profraw_dir}"
                    ),
                )
            )
            res = results[-1].is_ok()
        else:
            print(f"ERROR: No profraw files found in {profraw_dir}")
            res = False

        # Save .ninja_log for packaging (contains build timing data for scheduling)
        ninja_log_src = f"{CH_PROFILE_BUILD_DIR}/.ninja_log"
        ninja_log_saved = f"{TEMP}/clickhouse-ninja-log"
        if os.path.exists(ninja_log_src):
            shutil.copy2(ninja_log_src, ninja_log_saved)
            print(f"Saved .ninja_log ({os.path.getsize(ninja_log_saved)} bytes)")
        else:
            print("WARNING: .ninja_log not found after profile collection build")

        # Clean up to free disk space (~80 GB)
        print("Cleaning Stage 1 build and CH profile build to free disk space")
        clean_dirs(STAGE1_BUILD_DIR, CH_PROFILE_BUILD_DIR, STAGE1_INSTALL_DIR)

    # Stage 3: Build PGO-optimized clang with all projects/targets and BOLT-ready flags
    if res and JobStages.STAGE2_BUILD in stages:
        clean_dirs(STAGE2_BUILD_DIR, STAGE2_INSTALL_DIR)
        os.makedirs(STAGE2_BUILD_DIR, exist_ok=True)

        builtin_targets = ";".join(t for t, _ in CROSS_BUILTIN_TARGETS)

        # Create a minimal stub sysroot for cross-target builtins compilation.
        # Uses --sysroot to isolate from host headers, -ffreestanding for
        # compiler-provided headers (stdint.h, stddef.h, etc.), and
        # COMPILER_RT_BAREMETAL_BUILD=ON to exclude files needing full libc.
        # Remaining files need only a few OS headers which we stub out here.
        cross_sysroot = f"{TEMP}/cross-builtins-sysroot"
        cross_inc = f"{cross_sysroot}/include"
        for subdir in ["", "sys", "linux", "asm"]:
            os.makedirs(f"{cross_inc}/{subdir}", exist_ok=True)

        with open(f"{cross_inc}/assert.h", "w") as f:
            f.write(
                "#define assert(x) ((void)0)\n"
                "#define static_assert _Static_assert\n"
            )
        with open(f"{cross_inc}/sys/auxv.h", "w") as f:
            # Stubs for cpu_model/aarch64.c (FreeBSD: elf_aux_info, Linux: getauxval)
            f.write(
                "#pragma once\n"
                "#define AT_HWCAP 16\n"
                "#define AT_HWCAP2 26\n"
                "int elf_aux_info(int, void *, int);\n"
                "unsigned long getauxval(unsigned long);\n"
            )
        with open(f"{cross_inc}/linux/unistd.h", "w") as f:
            f.write("#include <asm/unistd.h>\n")
        with open(f"{cross_inc}/asm/unistd.h", "w") as f:
            # Stub for clear_cache.c on riscv64
            f.write("#define __NR_riscv_flush_icache 259\n")

        cross_flags = (
            f"-ffreestanding --sysroot={cross_sysroot} -isystem {cross_inc}"
        )
        builtin_cmake_args = " ".join(
            f"-DBUILTINS_{triple}_CMAKE_SYSTEM_NAME={system}"
            f" -DBUILTINS_{triple}_COMPILER_RT_BAREMETAL_BUILD=ON"
            f' -DBUILTINS_{triple}_CMAKE_C_FLAGS="{cross_flags}"'
            f' -DBUILTINS_{triple}_CMAKE_CXX_FLAGS="{cross_flags}"'
            for triple, system in CROSS_BUILTIN_TARGETS
        )

        cmake_cmd = (
            f"cmake -G Ninja"
            f' -DLLVM_ENABLE_PROJECTS="{STAGE2_LLVM_PROJECTS}"'
            f' -DLLVM_ENABLE_RUNTIMES="compiler-rt"'
            f' -DLLVM_TARGETS_TO_BUILD="{STAGE2_LLVM_TARGETS}"'
            f' -DLLVM_DISTRIBUTION_COMPONENTS="{";".join(STAGE2_DISTRIBUTION_COMPONENTS)}"'
            # Otherwise the runtimes depend on the test tools (`opt`, `lli`, `FileCheck`, ...)
            f" -DLLVM_INCLUDE_TESTS=OFF"
            f" -DCMAKE_BUILD_TYPE=Release"
            f" -DLLVM_PROFDATA_FILE={PROFDATA_PATH}"
            f" -DCMAKE_C_COMPILER=clang-{LLVM_VERSION}"
            f" -DCMAKE_CXX_COMPILER=clang++-{LLVM_VERSION}"
            f" -DLLVM_ENABLE_LLD=ON"
            f" -DLLVM_ENABLE_LTO=Thin"
            f"{musl_llvm_cmake_args()}"
            # jemalloc replaces musl's malloc: a member of `libc.a` is only pulled for symbols still undefined
            f' -DCMAKE_EXE_LINKER_FLAGS="{musl_exe_linker_flags()} -Wl,--emit-relocs,-znow -Wl,--whole-archive {JEMALLOC_LIBRARY} -Wl,--no-whole-archive"'
            f' -DCMAKE_SHARED_LINKER_FLAGS="-Wl,--emit-relocs,-znow"'
            f" -DLLVM_ENABLE_TERMINFO=OFF"
            f" -DLLVM_ENABLE_ZLIB=FORCE_ON"
            f" -DLLVM_ENABLE_ZSTD=FORCE_ON"
            f" -DLLVM_BINUTILS_INCDIR=/usr/include"
            f' -DLLVM_BUILTIN_TARGETS="{builtin_targets}"'
            f" {builtin_cmake_args}"
            f" -DCMAKE_INSTALL_PREFIX={STAGE2_INSTALL_DIR}"
            f" -S {LLVM_SOURCE_DIR}/llvm"
            f" -B {STAGE2_BUILD_DIR}"
        )
        results.append(
            Result.from_commands_run(
                name="Stage 2 CMake (PGO-optimized clang)",
                command=cmake_cmd,
            )
        )
        res = results[-1].is_ok()

        # The default target would build every LLVM tool, test utility and fuzzer, each
        # one a separate ThinLTO link, so build only the distribution
        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 2 Build",
                    command=f"{CUSTOM_NINJA} -C {STAGE2_BUILD_DIR} distribution",
                )
            )
            res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 2 Install",
                    command=f"{CUSTOM_NINJA} -C {STAGE2_BUILD_DIR} install-distribution",
                )
            )
            res = results[-1].is_ok()

        # Clean stage2 build dir, keep install
        print("Cleaning Stage 2 build directory to free disk space")
        clean_dirs(STAGE2_BUILD_DIR)

    # Stage 4: BOLT optimization (best-effort)
    # BOLT can fail due to architecture-specific issues (e.g., ADR relaxation
    # on aarch64 with AMDGPU code, before that backend was dropped) or disk space constraints (fdata files are
    # ~15 MB per compilation unit). If BOLT fails, we still have a PGO-optimized
    # toolchain which provides ~23% compilation speedup.
    if res and JobStages.BOLT_OPTIMIZATION in stages:
        bolt_ok = True
        bolt_results = []
        clang_binary = f"{STAGE2_INSTALL_DIR}/bin/clang-{LLVM_VERSION}"

        # Find the actual clang binary (it may be a different version than expected)
        if not os.path.exists(clang_binary):
            candidates = sorted(
                glob.glob(f"{STAGE2_INSTALL_DIR}/bin/clang-[0-9]*"),
                reverse=True,
            )
            if candidates:
                clang_binary = candidates[0]
            else:
                clang_binary = f"{STAGE2_INSTALL_DIR}/bin/clang"

        clang_basename = os.path.basename(clang_binary)
        print(f"BOLT target binary: {clang_binary}")
        llvm_bolt = f"{STAGE2_INSTALL_DIR}/bin/llvm-bolt"
        merge_fdata = f"{STAGE2_INSTALL_DIR}/bin/merge-fdata"
        clang_instrumented = f"{clang_binary}.inst"
        clang_bolted = f"{clang_binary}.bolt"
        clangpp_inst = None  # set in step 2 if BOLT instrumentation succeeds

        clean_dirs(BOLT_PROFILES_DIR, CH_BOLT_BUILD_DIR)
        os.makedirs(BOLT_PROFILES_DIR, exist_ok=True)

        # Step 1: Instrument clang with BOLT
        # A static executable has no finalization hook for BOLT, so the profile is written by a
        # watcher process forked at startup: with `-instrumentation-no-counters-clear` it writes
        # the profile once clang exits (a clang process outliving the sleep time also writes
        # cumulative intermediate profiles).
        result = Result.from_commands_run(
            name="BOLT instrument clang",
            command=(
                f"{llvm_bolt} {clang_binary}"
                f" -o {clang_instrumented}"
                f" -instrument"
                f" --instrumentation-file-append-pid"
                f" --instrumentation-file={BOLT_PROFILES_DIR}/prof"
                f" --instrumentation-sleep-time=3600"
                f" --instrumentation-no-counters-clear"
            ),
        )
        bolt_results.append(result)
        if not result.is_ok():
            bolt_ok = False
            print(
                "BOLT instrumentation failed"
                " (it used to fail on aarch64 with the AMDGPU backend, which is no longer built)."
                " Continuing with PGO-only toolchain."
            )

        # Step 2: Create symlinks for the instrumented binary so cmake can
        # use it as both C and C++ compiler (clang dispatches based on argv[0])
        if bolt_ok:
            inst_dir = os.path.dirname(clang_instrumented)
            clangpp_inst = os.path.join(inst_dir, "clang++.inst")
            try:
                if os.path.exists(clangpp_inst):
                    os.remove(clangpp_inst)
                os.symlink(os.path.basename(clang_instrumented), clangpp_inst)
                print(f"Created symlink: {clangpp_inst} -> {os.path.basename(clang_instrumented)}")
            except OSError as e:
                print(f"Failed to create clang++ symlink: {e}")
                bolt_ok = False

        # Step 3: Configure ClickHouse build with BOLT-instrumented clang
        if bolt_ok:
            cmake_cmd = (
                f"cmake"
                f" -DCMAKE_BUILD_TYPE=None"
                f" -DENABLE_THINLTO=0"
                f" -DCMAKE_C_COMPILER={clang_instrumented}"
                f" -DCMAKE_CXX_COMPILER={clangpp_inst}"
                f" -DCOMPILER_CACHE=disabled"
                f" -DENABLE_TESTS=0"
                f" -DENABLE_UTILS=0"
                f" -DCMAKE_TOOLCHAIN_FILE={toolchain_file}"
                f" {REPO_PATH}"
                f" -B {CH_BOLT_BUILD_DIR}"
            )
            result = Result.from_commands_run(
                name="BOLT profile collection CMake",
                command=cmake_cmd,
            )
            bolt_results.append(result)
            if not result.is_ok():
                bolt_ok = False
                print("BOLT profile collection cmake failed. Continuing with PGO-only.")

        # Step 4: Time-limited build to collect BOLT profiles
        # Each clang process writes a ~15 MB fdata file; see BOLT_PROFILE_TIMEOUT.
        if bolt_ok:
            result = Result.from_commands_run(
                name="BOLT profile collection build (time-limited)",
                command=(
                    f"bash -c 'timeout --signal=INT --kill-after=120"
                    f" {BOLT_PROFILE_TIMEOUT}"
                    f" {CUSTOM_NINJA} -k0"
                    f" -C {CH_BOLT_BUILD_DIR} clickhouse"
                    f" ; exit 0'"
                ),
            )
            bolt_results.append(result)

            # The watcher processes notice their clang has exited within a second and only then
            # write the profile. They are named like the instrumented `clang-<version>.inst` and
            # `clang++.inst`; matching the process name, not the command line, skips this shell.
            result = Result.from_commands_run(
                name="Wait for BOLT profile writers",
                command=(
                    "timeout 120 bash -c"
                    " 'while pgrep \"\\.inst$\" > /dev/null; do sleep 1; done'"
                ),
            )
            bolt_results.append(result)
            if not result.is_ok():
                bolt_ok = False
                print("BOLT profile writers did not finish. Continuing with PGO-only.")

            # Check if we collected any profiles
            fdata_files = glob.glob(f"{BOLT_PROFILES_DIR}/prof.*")
            if not fdata_files:
                bolt_ok = False
                print("No BOLT fdata profiles collected. Continuing with PGO-only.")
            else:
                print(f"Collected {len(fdata_files)} BOLT profile files")

        # Step 5: Merge BOLT profiles
        if bolt_ok:
            result = Result.from_commands_run(
                name="Merge BOLT profiles",
                command=(
                    f"{merge_fdata} -o {BOLT_FDATA_PATH}"
                    f" {BOLT_PROFILES_DIR}/prof.*"
                ),
            )
            bolt_results.append(result)
            if not result.is_ok():
                bolt_ok = False
                print("BOLT profile merge failed. Continuing with PGO-only.")

        # Step 6: Apply BOLT optimization
        # Flags match upstream perf-helper.py bolt_optimize
        if bolt_ok:
            result = Result.from_commands_run(
                name="BOLT optimize clang",
                command=(
                    f"{llvm_bolt} {clang_binary}"
                    f" -o {clang_bolted}"
                    f" -data={BOLT_FDATA_PATH}"
                    f" -reorder-blocks=ext-tsp"
                    f" -reorder-functions=cdsort"
                    f" -split-functions"
                    f" -split-all-cold"
                    f" -split-eh"
                    f" -dyno-stats"
                    f" -use-gnu-stack"
                ),
            )
            bolt_results.append(result)
            if not result.is_ok():
                bolt_ok = False
                print("BOLT optimization failed. Continuing with PGO-only.")

        # Step 7: Replace original binary with BOLTed version
        if bolt_ok:
            result = Result.from_commands_run(
                name="Install BOLTed clang",
                command=f"mv {clang_bolted} {clang_binary}",
            )
            bolt_results.append(result)
            if not result.is_ok():
                bolt_ok = False

        if bolt_ok:
            print("BOLT optimization applied successfully")
            results.extend(bolt_results)
        else:
            print("Packaging PGO-only toolchain (BOLT was skipped or failed)")
            # Mark all BOLT results as skipped so they don't fail the overall job
            for r in bolt_results:
                r.status = Result.Status.SKIPPED
            results.extend(bolt_results)

        # Clean up BOLT intermediates
        print("Cleaning BOLT intermediate files")
        clean_dirs(CH_BOLT_BUILD_DIR, BOLT_PROFILES_DIR)
        for f in [clang_instrumented, clangpp_inst, BOLT_FDATA_PATH, clang_bolted]:
            try:
                if f and os.path.exists(f):
                    os.remove(f)
            except OSError:
                pass

    # Stage 5: Package
    if res and JobStages.PACKAGE in stages:
        # Copy custom Ninja binary into the toolchain
        if os.path.exists(CUSTOM_NINJA):
            ninja_dest = f"{STAGE2_INSTALL_DIR}/bin/ninja"
            shutil.copy2(CUSTOM_NINJA, ninja_dest)
            os.chmod(ninja_dest, 0o755)
            print(f"Installed custom Ninja to {ninja_dest}")

        # Copy .ninja_log for pre-seeding CI builds
        ninja_log_saved = f"{TEMP}/clickhouse-ninja-log"
        if os.path.exists(ninja_log_saved):
            ninja_log_dir = f"{STAGE2_INSTALL_DIR}/{NINJA_LOG_SHARE_DIR}"
            os.makedirs(ninja_log_dir, exist_ok=True)
            shutil.copy2(ninja_log_saved, f"{ninja_log_dir}/ninja_log")
            print(f"Installed .ninja_log to {ninja_log_dir}/ninja_log")

        # LLVM installs the versioned `clang-<version>` plus unversioned
        # `clang++`->`clang`->`clang-<version>`, but not a versioned `clang++-<version>`. ClickHouse
        # selects compilers by versioned name, so without this the C++ compiler resolves to whatever
        # `clang++-<version>` is elsewhere on PATH (e.g. a distro one) while C/ASM use this
        # toolchain - a silent mismatch. Add the missing symlink.
        clangpp = f"{STAGE2_INSTALL_DIR}/bin/clang++-{LLVM_VERSION}"
        if not os.path.lexists(clangpp):
            os.symlink("clang", clangpp)
            print(f"Created symlink {clangpp} -> clang")

        results.append(
            Result.from_commands_run(
                name="Check that executables are static",
                command=(
                    f"for f in {STAGE2_INSTALL_DIR}/bin/*; do"
                    f' [ -L "$f" ] && continue;'
                    f" {shell_is_elf('$f')} || continue;"
                    f" {shell_is_static('$f')} || {{ echo \"$f is not statically linked\"; exit 1; }};"
                    f" done"
                ),
            )
        )
        res = results[-1].is_ok()

        # Strip ELF executables and shared libraries to reduce archive size
        # (relocations from --emit-relocs and LTO symbols are no longer needed).
        # Check the ELF magic to skip scripts (Python, Perl, shell) that strip cannot handle.
        if res:
            results.append(
                Result.from_commands_run(
                    name="Strip binaries",
                    command=(
                        f"find {STAGE2_INSTALL_DIR}/bin -type f -executable | while read -r f; do"
                        f" {shell_is_elf('$f')} || continue;"
                        f' strip --strip-unneeded "$f" || exit 1;'
                        f" done"
                        f" && find {STAGE2_INSTALL_DIR}/lib -name '*.so*' -type f"
                        f" -exec strip --strip-unneeded {{}} +"
                    ),
                )
            )
            res = results[-1].is_ok()

        # The executables link jemalloc, musl, zlib and zstd statically, and their licenses must ship with them
        if res:
            results.append(
                Result.from_commands_run(
                    name="Install licenses",
                    command=[
                        f"install -D -m 644 {JEMALLOC_SOURCE_DIR}/COPYING"
                        f" {STAGE2_INSTALL_DIR}/share/doc/jemalloc/COPYING",
                        f"cp -r {MUSL_SYSROOT}/share/doc/. {STAGE2_INSTALL_DIR}/share/doc/",
                    ],
                )
            )
            res = results[-1].is_ok()

        if res:
            output_path = f"{OUTPUT_DIR}/clang-pgo-bolt.tar.zst"
            results.append(
                Result.from_commands_run(
                    name="Package toolchain",
                    command=(
                        f"tar -C {STAGE2_INSTALL_DIR} -cf - ."
                        f" | zstd -T0 -19 -o {output_path}"
                    ),
                )
            )
            res = results[-1].is_ok()

        if res:
            file_size_mb = os.path.getsize(output_path) / (1024 * 1024)
            print(f"Toolchain archive size: {file_size_mb:.1f} MB")

    Result.create_from(results=results).complete_job()


if __name__ == "__main__":
    main()
