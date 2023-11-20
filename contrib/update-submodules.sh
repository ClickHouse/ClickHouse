#!/bin/sh
set -e

SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "${SCRIPT_PATH}")
GIT_DIR=$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)
cd $GIT_DIR

contrib/sparse-checkout/setup-sparse-checkout.sh
git submodule init
git submodule sync
git config --file .gitmodules --get-regexp .*path | sed 's/[^ ]* //' | xargs -I _ --max-procs 64 git submodule update --depth=1 --single-branch _
