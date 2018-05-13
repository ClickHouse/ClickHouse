#!/usr/bin/env bash
set -e -x

# How to run:
# From "ci" directory:
#     jobs/quick-build/run.sh
# or:
#     ./run-with-docker.sh ubuntu:bionic jobs/quick-build/run.sh

CONFIG="$(dirname $0)"/config
cd "$(dirname $0)"/../..

. default-config

. get-sources.sh
. prepare-toolchain.sh
. install-libraries.sh
. build-normal.sh
