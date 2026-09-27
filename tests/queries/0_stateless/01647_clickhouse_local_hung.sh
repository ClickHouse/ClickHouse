#!/usr/bin/env bash
# Tags: no-fasttest

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


for _ in {1..100}; do echo 'Hello, world!' | ${CLICKHOUSE_LOCAL} --query "SELECT * FROM table" --structure 's String' | wc -c; done | uniq -c
