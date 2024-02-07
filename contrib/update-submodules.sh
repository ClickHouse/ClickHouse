#!/bin/sh

set -e

WORKDIR=$(dirname "$0")
WORKDIR=$(readlink -f "${WORKDIR}")

"$WORKDIR/sparse-checkout/setup-sparse-checkout.sh"
git submodule init
git submodule sync
git submodule update --depth=1
