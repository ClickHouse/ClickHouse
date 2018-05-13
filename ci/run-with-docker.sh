#!/usr/bin/env bash
set -e -x

PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd -P)"
[[ -n "$CONFIG" ]] && DOCKER_ENV="--env=CONFIG"
docker run --network=host --mount=type=bind,source=${PROJECT_ROOT},destination=/ClickHouse --workdir=/ClickHouse/ci $DOCKER_ENV "$1" "$2"
