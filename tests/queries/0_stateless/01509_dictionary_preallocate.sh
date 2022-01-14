#!/usr/bin/env bash

# shellcheck disable=SC2031
# shellcheck disable=SC2030

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE IF EXISTS data_01509;
    DROP DICTIONARY IF EXISTS dict_01509;
    DROP DICTIONARY IF EXISTS dict_01509_preallocate;

    CREATE TABLE data_01509
    (
      key   UInt64,
      value String
    )
    ENGINE = MergeTree()
    ORDER BY key;
    INSERT INTO data_01509 SELECT number key, toString(number) value FROM numbers(10e3);
"

# regular
$CLICKHOUSE_CLIENT -nm -q "
    CREATE DICTIONARY dict_01509
    (
      key   UInt64,
      value String DEFAULT '-'
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'data_01509'))
    LAYOUT(SPARSE_HASHED(PREALLOCATE 0))
    LIFETIME(0);
    SHOW CREATE DICTIONARY dict_01509;
"
(
    # start new shell to avoid overriding variables for other client invocation
    CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace}
    $CLICKHOUSE_CLIENT -nm -q "SYSTEM RELOAD DICTIONARY dict_01509" |& grep -o "HashedDictionary.*"
)

# with preallocation
$CLICKHOUSE_CLIENT -nm -q "
    CREATE DICTIONARY dict_01509_preallocate
    (
      key   UInt64,
      value String DEFAULT '-'
    )
    PRIMARY KEY key
    SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'data_01509'))
    LAYOUT(SPARSE_HASHED(PREALLOCATE 1))
    LIFETIME(0);
    SHOW CREATE DICTIONARY dict_01509_preallocate;
    SYSTEM RELOAD DICTIONARY dict_01509_preallocate;
"
(
    # start new shell to avoid overriding variables for other client invocation
    CLICKHOUSE_CLIENT=${CLICKHOUSE_CLIENT/--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}/--send_logs_level=trace}
    $CLICKHOUSE_CLIENT -nm -q "SYSTEM RELOAD DICTIONARY dict_01509_preallocate" |& grep -o "HashedDictionary.*"
)

$CLICKHOUSE_CLIENT -nm -q "
    SELECT dictGet('dict_01509', 'value', toUInt64(1e12));
    SELECT dictGet('dict_01509', 'value', toUInt64(0));
    SELECT count() FROM dict_01509;

    SELECT dictGet('dict_01509_preallocate', 'value', toUInt64(1e12));
    SELECT dictGet('dict_01509_preallocate', 'value', toUInt64(0));
    SELECT count() FROM dict_01509_preallocate;
"

$CLICKHOUSE_CLIENT -nm -q "
    DROP TABLE data_01509;
    DROP DICTIONARY dict_01509;
    DROP DICTIONARY dict_01509_preallocate;
"
