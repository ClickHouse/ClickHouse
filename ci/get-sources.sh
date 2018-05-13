#!/usr/bin/env bash
set -e -x

source default-config

if [[ "$SOURCES_METHOD" == "clone" ]]; then
    $SUDO apt-get install -y git
    SOURCES_DIR="${WORKSPACE}/sources"
    mkdir -p "${SOURCES_DIR}"
    git clone --recursive --branch "$SOURCES_BRANCH" "$SOURCES_CLONE_URL" "${SOURCES_DIR}"
    pushd "${SOURCES_DIR}"
    git checkout "$SOURCES_COMMIT"
    popd
elif [[ "$SOURCES_METHOD" == "local" ]]; then
    ln -f -s "${PROJECT_ROOT}" "${WORKSPACE}/sources"
else
    die "Unknown SOURCES_METHOD"
fi
