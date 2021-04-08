#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

env CLICKHOUSE_CLIENT_OPT="--merge_tree_uniform_read_distribution=0" bash "$CURDIR"/00443_optimize_final_vertical_merge.sh
