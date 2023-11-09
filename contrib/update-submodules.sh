#!/bin/sh
set -e

SCRIPT_PATH=$(realpath "$0")
GIT_DIR=$(dirname "${SCRIPT_PATH}") # <repo>/contrib/
GIT_DIR=$(dirname "${GIT_DIR}")     # <repo>/
cd $GIT_DIR

contrib/sparse-checkout/setup-sparse-checkout.sh
git submodule init
git submodule sync
git config --file .gitmodules --get-regexp .*path | sed 's/[^ ]* //' | xargs -I _ --max-procs 64 git submodule update --depth=1 --single-branch _
