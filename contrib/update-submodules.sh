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
# google-protobuf: Used to build protoc during cross-compilation
# grpc: Used to build grpc_cpp_plugin during cross-compilation
# abseil-cpp: Dependency for google-protobuf and grpc
# c-ares: Dependency for grpc
# corrosion: Used to build rust
# rust_vendor: Used to build rust
# aws-crt-cpp: TODO: Fix and stop using it
grep -o -P '"contrib/[^"]+"' .gitmodules |
  grep -v -P 'contrib/(llvm-project|google-protobuf|grpc|abseil-cpp|c-ares|corrosion|rust_vendor|aws-crt-cpp)' |
  xargs -I@ find @ \
    -'(' -name 'CMakeLists.txt' -or -name '*.cmake' -')' -and -not -name '*.h.cmake' \
    -delete
