#!/usr/bin/env bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")/.."; pwd -P)"
docker run --network=host --mount=type=bind,source=PROJECT_ROOT,destination=/ClickHouse --workdir=/ClickHouse/ci --env=CONFIG "$1" "$2"
