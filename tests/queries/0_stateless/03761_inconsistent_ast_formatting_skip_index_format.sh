#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_FORMAT} --query "
CREATE TABLE t1
(
    c0 Int64,
    c1 Int64,
    INDEX i1 (c0 AS v3) TYPE minmax GRANULARITY 5
)
ENGINE = MergeTree
PARTITION BY tuple()
ORDER BY tuple();
"