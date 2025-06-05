#!/bin/sh
set -e

SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
GIT_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd $GIT_DIR

# Exclude from contribs some garbage subdirs that we don't need.
# It reduces the checked out files size about 3 times and therefore speeds up indexing in IDEs and searching.
# NOTE .git/ still contains everything that we don't check out (although, it's compressed)
# See also https://git-scm.com/docs/git-sparse-checkout
contrib/sparse-checkout/setup-sparse-checkout.sh

git submodule init
git submodule sync

# NOTE: do not use --remote for `git submodule update`[1] command, since the submodule references to the specific commit SHA1 in the subproject.
#       It may cause unexpected behavior. Instead you need to commit a new SHA1 for a submodule.
#
#       [1] - https://git-scm.com/book/en/v2/Git-Tools-Submodules
git config --file .gitmodules --get-regexp '.*path' | sed 's/[^ ]* //' | xargs -I _ --max-procs 64 git submodule update --depth=1 --single-branch _

# We don't want to depend on any third-party CMake files.
# To check it, find and delete them.
grep -o -P '"contrib/[^"]+"' .gitmodules |
  grep -v -P 'contrib/(llvm-project|google-protobuf|grpc|abseil-cpp|corrosion|aws-crt-cpp)' |
  xargs -I@ find @ \
    -'(' -name 'CMakeLists.txt' -or -name '*.cmake' -')' -and -not -name '*.h.cmake' \
    -delete
