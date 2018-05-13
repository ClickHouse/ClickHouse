#!/usr/bin/env bash
set -e

source default-config

[[ -d "${WORKSPACE}/sources" ]] || die "Run get-sources.sh first"

./sources/release
