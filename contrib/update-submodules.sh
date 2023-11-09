#!/bin/sh
set -e

GITDIR=$(git rev-parse --show-toplevel)

$GITDIR/contrib/sparse-checkout/setup-sparse-checkout.sh
git submodule init
git submodule sync
git config --file $GITDIR/.gitmodules --get-regexp .*path | sed 's/[^ ]* //' | xargs -I _ --max-procs 64 git submodule update --depth=1 --single-branch _
