#!/usr/bin/env bash
# Tags: no-fasttest, replica, no-parallel, no-replicated-database
# no-fasttest: requires S3 / MinIO.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# shellcheck source=./export_partition.lib
. "$CUR_DIR"/export_partition.lib

run_partition_export_dotted_destination_test replicated
