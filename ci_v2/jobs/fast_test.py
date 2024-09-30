import subprocess
import time

from praktika.utils import Shell


def clone_submodules():
    # List of submodules to update
    submodules_to_update = [
        "contrib/sysroot",
        "contrib/magic_enum",
        "contrib/abseil-cpp",
        "contrib/boost",
        "contrib/zlib-ng",
        "contrib/libxml2",
        "contrib/libunwind",
        "contrib/fmtlib",
        "contrib/aklomp-base64",
        "contrib/cctz",
        "contrib/libcpuid",
        "contrib/libdivide",
        "contrib/double-conversion",
        "contrib/llvm-project",
        "contrib/lz4",
        "contrib/zstd",
        "contrib/fastops",
        "contrib/rapidjson",
        "contrib/re2",
        "contrib/sparsehash-c11",
        "contrib/croaring",
        "contrib/miniselect",
        "contrib/xz",
        "contrib/dragonbox",
        "contrib/fast_float",
        "contrib/NuRaft",
        "contrib/jemalloc",
        "contrib/replxx",
        "contrib/wyhash",
        "contrib/c-ares",
        "contrib/morton-nd",
        "contrib/xxHash",
        "contrib/expected",
        "contrib/simdjson",
        "contrib/liburing",
        "contrib/libfiu",
        "contrib/incbin",
        "contrib/yaml-cpp",
    ]

    Shell.check("git submodule sync", verbose=True, strict=True)
    Shell.check("git submodule init", verbose=True, strict=True)

    for _ in range(5):
        try:
            subprocess.run(
                [
                    "xargs",
                    "--max-procs=100",
                    "--null",
                    "--no-run-if-empty",
                    "--max-args=1",
                    "git",
                    "submodule",
                    "update",
                    "--depth",
                    "1",
                    "--single-branch",
                ],
                input="\0".join(submodules_to_update) + "\0",
                text=True,
                check=True,
            )
            break
        except subprocess.CalledProcessError:
            print("Retrying submodule update due to network failure...")
            time.sleep(1)

    # Reset, checkout, and clean submodules
    subprocess.run(
        ["git", "submodule", "foreach", "git", "reset", "--hard"], check=True
    )
    subprocess.run(
        ["git", "submodule", "foreach", "git", "checkout", "@", "-f"], check=True
    )
    subprocess.run(["git", "submodule", "foreach", "git", "clean", "-xfd"], check=True)


if __name__ == "__main__":
    clone_submodules()
