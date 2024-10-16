#!/usr/bin/env bash
# Tags: long, zookeeper, no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# We should have correct env vars from shell_config.sh to run this test
python3 "$CURDIR"/02481_async_insert_dedup.python ReplicatedMergeTree
