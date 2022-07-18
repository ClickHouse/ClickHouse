#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-s3-storage

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TMP_PATH=${CLICKHOUSE_TEST_UNIQUE_NAME}
QUERIES_FILE=02226_filesystem_cache_profile_events.queries
TEST_FILE=$CUR_DIR/filesystem_cache_queries/$QUERIES_FILE

for storagePolicy in 's3_cache' 'local_cache' 'azure_cache'; do
    echo "Using storage policy: $storagePolicy"
    cat $TEST_FILE | sed -e "s/_storagePolicy/${storagePolicy}/"  > $TMP_PATH
    ${CLICKHOUSE_CLIENT} --queries-file $TMP_PATH
    rm $TMP_PATH
    echo
done
