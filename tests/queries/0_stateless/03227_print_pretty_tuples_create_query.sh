#!/usr/bin/env bash
# Tags: no-fasttest, no-asan, no-msan, no-tsan
# ^ requires S3

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo
echo "SHOW CREATE TABLE:"
${CLICKHOUSE_CLIENT} --output-format Raw --query "
    DROP TABLE IF EXISTS test;
    CREATE TABLE test (x Tuple(a String, b Array(Tuple(c Tuple(e String), d String))), y String) ORDER BY ();
    SET print_pretty_type_names = 1;
    SHOW CREATE TABLE test;
    SET print_pretty_type_names = 0;
    SHOW CREATE TABLE test;
    DROP TABLE test;
"

echo
echo "clickhouse-format:"
${CLICKHOUSE_FORMAT} --query "
    CREATE TABLE test (x Tuple(a String, b Array(Tuple(c Tuple(e String), d String))), y String) ORDER BY ()
"
${CLICKHOUSE_FORMAT} --oneline --query "
    CREATE TABLE test (x Tuple(a String, b Array(Tuple(c Tuple(e String), d String))), y String) ORDER BY ()
"

echo
echo "formatQuery:"
${CLICKHOUSE_CLIENT} --output-format Raw --query "
    SELECT formatQuery('CREATE TABLE test (x Tuple(a String, b Array(Tuple(c Tuple(e String), d String))), y String) ORDER BY ()') SETTINGS print_pretty_type_names = 1;
    SELECT formatQuery('CREATE TABLE test (x Tuple(a String, b Array(Tuple(c Tuple(e String), d String))), y String) ORDER BY ()') SETTINGS print_pretty_type_names = 0;
"
