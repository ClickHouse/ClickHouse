#!/usr/bin/env bash
set -e

source default-config

if [[ "$SOURCES_METHOD" == "clone" ]]; then
    mkdir -p "${WORKSPACE}/sources"
    git clone --recursive --branch "$SOURCES_BRANCH" "$SOURCES_CLONE_URL" "${WORKSPACE}/sources"
elif [[ "$SOURCES_METHOD" == "local" ]]; then
    ln -s $(git rev-parse --show-toplevel) "${WORKSPACE}/sources"
else
    die "Unknown SOURCES_METHOD"
fi
