#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
#  shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -q "desc file('$CUR_DIR/data_bson/c0_Int_10.bson')"

$CLICKHOUSE_LOCAL -q """
    DROP TABLE IF EXISTS t0;
    CREATE TABLE t0 (c0 Int) ENGINE = MergeTree() PRIMARY KEY (c0);
    INSERT INTO TABLE t0 (c0) SELECT * FROM file('$CUR_DIR/data_bson/c0_Int_10.bson');
"""

$CLICKHOUSE_LOCAL -q """SET min_chunk_bytes_for_parallel_parsing = 0;""" 2>&1 | grep -o 'BAD_ARGUMENTS'
