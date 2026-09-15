#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-cas-storage
# no-fasttest: requires S3 / MinIO.
# no-shared-merge-tree: this test exercises EXPORT PARTITION on a plain (non-replicated) MergeTree.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./export_partition.lib
. "$CUR_DIR"/export_partition.lib

run_partition_export_dotted_destination_test plain
