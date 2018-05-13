#!/usr/bin/env bash
set -e -x

source default-config

$SUDO apt-get install -y jq

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

mkdir -p "${WORKSPACE}/build"
pushd "${WORKSPACE}/build"

cmake -D CMAKE_BUILD_TYPE=Debug $CMAKE_FLAGS ../sources

make re2_st # Generated headers

jq --raw-output '.[] | .command' compile_commands.json | grep -v -P -- '-c .+/contrib/' | sed -r -e 's/-o\s+\S+/-fsyntax-only/' > syntax-commands
xargs --arg-file=syntax-commands --max-procs=$THREADS --replace /bin/sh -c "{}"

popd
