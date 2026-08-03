#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh


# In [1] PREALLOCATE attribute had been added for HASHED dictionaries,
# but it had been removed in [2].
#
#   [1]: https://github.com/ClickHouse/ClickHouse/pull/23979
#   [2]: https://github.com/ClickHouse/ClickHouse/pull/45388
#
# This is a backward compatiblity test that you can create dictionary with
# PREALLOCATE attribute (and also for the history/greppability, that it was
# such).

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE IF EXISTS data_01509;
    DROP DICTIONARY IF EXISTS dict_01509;
    CREATE TABLE data_01509
    (
      key   UInt64,
      value String
    )
    ENGINE = MergeTree()
    ORDER BY key;
    INSERT INTO data_01509 SELECT number key, toString(number) value FROM numbers(10e3);

    CREATE DICTIONARY dict_01509
    (
      key   UInt64,
      value String DEFAULT '-'
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'data_01509'))
    LAYOUT(SPARSE_HASHED(PREALLOCATE 1))
    LIFETIME(0);
    SYSTEM RELOAD DICTIONARY dict_01509;
" |& grep -o "HashedDictionary: .*"
