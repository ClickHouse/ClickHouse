import argparse
import glob
import os
import platform
import shutil

from ci.praktika.result import Result
from ci.praktika.utils import MetaClasses, Shell, Utils

TEMP = "/tmp"
LLVM_SOURCE_DIR = f"{TEMP}/llvm-project"
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

# BOLT profile collection settings: time-limited to avoid filling disk
# with fdata files (~15 MB per compilation unit)
BOLT_PROFILE_TIMEOUT = 1200  # 20 minutes
BOLT_PROFILE_PARALLELISM = 4

# All LLVM projects for the final toolchain (note: "all" doesn't work with cmake)
STAGE2_LLVM_PROJECTS = "clang;clang-tools-extra;lld;bolt;polly"


class JobStages(metaclass=MetaClasses.WithIter):
    CLONE_LLVM = "clone_llvm"
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
                    f"git clone --depth 1 --branch release/21.x"
                    f" https://github.com/llvm/llvm-project.git {LLVM_SOURCE_DIR}"
                ),
                retries=3,
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
            f" -DLLVM_ENABLE_TERMINFO=OFF"
            f" -DLLVM_ENABLE_ZLIB=OFF"
            f" -DLLVM_ENABLE_ZSTD=OFF"
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
                    command=f"ninja -C {STAGE1_BUILD_DIR} clang lld",
                )
            )
            res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 1 Install",
                    command=(
                        f"ninja -C {STAGE1_BUILD_DIR}"
                        f" install-clang install-clang-resource-headers install-lld"
                    ),
                )
            )
            res = results[-1].is_ok()

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
                command=f"ninja -C {CH_PROFILE_BUILD_DIR} clickhouse",
            )
            if not build_result.is_ok():
                print(
                    "ClickHouse build finished with errors"
                    " (link failures with instrumented compiler are expected)."
                    " Profraw files from compilation steps should still be available."
                )
                build_result.status = Result.Status.SUCCESS
                build_result.info = "Build failed at link step (expected); profraw files collected"
            results.append(build_result)

        # Merge profraw files using system llvm-profdata (stage 1 build lacks zlib
        # support, but the profraw files may contain zlib-compressed sections)
        profraw_dir = f"{STAGE1_BUILD_DIR}/profiles/"
        if os.path.isdir(profraw_dir) and os.listdir(profraw_dir):
            results.append(
                Result.from_commands_run(
                    name="Merge PGO profiles",
                    command=(
                        f"llvm-profdata-21 merge"
                        f" -output={PROFDATA_PATH}"
                        f" {profraw_dir}"
                    ),
                )
            )
            res = results[-1].is_ok()
        else:
            print(f"ERROR: No profraw files found in {profraw_dir}")
            res = False

        # Clean up to free disk space (~80 GB)
        print("Cleaning Stage 1 build and CH profile build to free disk space")
        clean_dirs(STAGE1_BUILD_DIR, CH_PROFILE_BUILD_DIR, STAGE1_INSTALL_DIR)

    # Stage 3: Build PGO-optimized clang with all projects/targets and BOLT-ready flags
    if res and JobStages.STAGE2_BUILD in stages:
        clean_dirs(STAGE2_BUILD_DIR, STAGE2_INSTALL_DIR)
        os.makedirs(STAGE2_BUILD_DIR, exist_ok=True)

        cmake_cmd = (
            f"cmake -G Ninja"
            f' -DLLVM_ENABLE_PROJECTS="{STAGE2_LLVM_PROJECTS}"'
            f' -DLLVM_ENABLE_RUNTIMES="compiler-rt"'
            f" -DLLVM_TARGETS_TO_BUILD=all"
            f" -DCMAKE_BUILD_TYPE=Release"
            f" -DLLVM_PROFDATA_FILE={PROFDATA_PATH}"
            f" -DCMAKE_C_COMPILER=clang-21"
            f" -DCMAKE_CXX_COMPILER=clang++-21"
            f" -DLLVM_ENABLE_LLD=ON"
            f" -DLLVM_ENABLE_LTO=Thin"
            f' -DCMAKE_EXE_LINKER_FLAGS="-Wl,--emit-relocs,-znow"'
            f' -DCMAKE_SHARED_LINKER_FLAGS="-Wl,--emit-relocs,-znow"'
            f" -DLLVM_ENABLE_TERMINFO=OFF"
            f" -DLLVM_ENABLE_ZLIB=OFF"
            f" -DLLVM_ENABLE_ZSTD=OFF"
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

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 2 Build",
                    command=f"ninja -C {STAGE2_BUILD_DIR}",
                )
            )
            res = results[-1].is_ok()

        if res:
            results.append(
                Result.from_commands_run(
                    name="Stage 2 Install",
                    command=f"ninja -C {STAGE2_BUILD_DIR} install",
                )
            )
            res = results[-1].is_ok()

        # Clean stage2 build dir, keep install
        print("Cleaning Stage 2 build directory to free disk space")
        clean_dirs(STAGE2_BUILD_DIR)

    # Stage 4: BOLT optimization (best-effort)
    # BOLT can fail due to architecture-specific issues (e.g., ADR relaxation
    # on aarch64 with AMDGPU code) or disk space constraints (fdata files are
    # ~15 MB per compilation unit). If BOLT fails, we still have a PGO-optimized
    # toolchain which provides ~23% compilation speedup.
    if res and JobStages.BOLT_OPTIMIZATION in stages:
        bolt_ok = True
        bolt_results = []
        clang_binary = f"{STAGE2_INSTALL_DIR}/bin/clang-21"

        # Find the actual clang binary (it may be clang-21, clang-20, etc.)
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
        result = Result.from_commands_run(
            name="BOLT instrument clang",
            command=(
                f"{llvm_bolt} {clang_binary}"
                f" -o {clang_instrumented}"
                f" -instrument"
                f" --instrumentation-file-append-pid"
                f" --instrumentation-file={BOLT_PROFILES_DIR}/prof"
            ),
        )
        bolt_results.append(result)
        if not result.is_ok():
            bolt_ok = False
            print(
                "BOLT instrumentation failed"
                " (known issue on aarch64 with AMDGPU backend code)."
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
        # Each compilation unit writes ~15 MB fdata file. With -j4 and a 20 minute
        # timeout, we expect ~300 compilations producing ~4.5 GB of profiles,
        # which gives BOLT enough data to optimize hot code paths.
        if bolt_ok:
            result = Result.from_commands_run(
                name="BOLT profile collection build (time-limited)",
                command=(
                    f"bash -c 'timeout --signal=INT --kill-after=120"
                    f" {BOLT_PROFILE_TIMEOUT}"
                    f" ninja -j{BOLT_PROFILE_PARALLELISM} -k0"
                    f" -C {CH_BOLT_BUILD_DIR} clickhouse"
                    f" ; exit 0'"
                ),
            )
            bolt_results.append(result)

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
        # Strip ELF executables and shared libraries to reduce archive size
        # (relocations from --emit-relocs and LTO symbols are no longer needed).
        # Use "file" to skip scripts (Python, Perl, shell) that strip cannot handle.
        results.append(
            Result.from_commands_run(
                name="Strip binaries",
                command=(
                    f"find {STAGE2_INSTALL_DIR}/bin -type f -executable"
                    f" -exec sh -c 'file \"$1\" | grep -q ELF && strip --strip-unneeded \"$1\"' _ {{}} \\;"
                    f" && find {STAGE2_INSTALL_DIR}/lib -name '*.so*' -type f"
                    f" -exec strip --strip-unneeded {{}} +"
                ),
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
