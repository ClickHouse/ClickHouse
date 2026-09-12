#!/bin/sh
set -e

# Parse optional --max-procs argument (defaults to 64)
MAX_PROCS=64
while [ $# -gt 0 ]; do
    case "$1" in
        --max-procs)
            MAX_PROCS="$2"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 [--max-procs NUM]"
            exit 1
            ;;
    esac
done

SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
GIT_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd $GIT_DIR

git submodule init
git submodule sync

# NOTE: do not use --remote for `git submodule update`[1] command, since the submodule references to the specific commit SHA1 in the subproject.
#       It may cause unexpected behavior. Instead you need to commit a new SHA1 for a submodule.
#
#       [1] - https://git-scm.com/book/en/v2/Git-Tools-Submodules
git config --file .gitmodules --get-regexp '.*path' | sed 's/[^ ]* //' | xargs -I _ --max-procs $MAX_PROCS git submodule update --depth=1 --single-branch _

# We don't want to depend on any third-party CMake files.
# To check it, find and delete them.
# llvm-project: Used to build llvm. Could be replaced with our own cmake files
# corrosion: Used to build rust (Does not make sense to replace)
# rust_vendor: Used to build rust
# wasmtime: uses build.rs script that requires cmake files to be present
grep -o -P '"contrib/[^"]+"' .gitmodules |
  grep -v -P 'contrib/(llvm-project|corrosion|rust_vendor|wasmtime)' |
  xargs -I@ find @ \
    -'(' -name 'CMakeLists.txt' -or -name '*.cmake' -')' -and -not -name '*.h.cmake' \
    -delete
