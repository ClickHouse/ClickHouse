#!/usr/bin/env bash
# Tags: long, no-fasttest, no-parallel, no-s3-storage

# set -x

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTS_DIR=$CUR_DIR/filesystem_cache
TMP_PATH=${CLICKHOUSE_TEST_UNIQUE_NAME}

for NAME in $(find "$TESTS_DIR"/*.sql -print0 | xargs -0 -n 1 basename | LC_ALL=C sort); do
    TEST_FILE=$TESTS_DIR/$NAME
    echo "Running tests from: $NAME"

    #for storagePolicy in 's3_lru_cache' 's3_arc_cache'; do
    for storagePolicy in 's3_lru_cache'; do
        echo "Using storage policy: $storagePolicy"
        cat $TEST_FILE | sed -e "s/storagePolicy/${storagePolicy}/" > $TMP_PATH
        ${CLICKHOUSE_CLIENT} --queries-file $TMP_PATH
        rm $TMP_PATH
    done
done
