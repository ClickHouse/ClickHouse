#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

DATA_FILE=$CLICKHOUSE_TEST_UNIQUE_NAME.data

for format in Parquet ORC Arrow ArrowStream Avro Native BSONEachRow JSONCompact Values TSKV JSONObjectEachRow JSONColumns JSONCompactColumns JSONCompact JSON TSV CSV
do
    echo $format
    $CLICKHOUSE_LOCAL -q "select * from generateRandom('a UInt64, b String, c Array(UInt64), d Tuple(a UInt64, b String)', 42) limit 10 format $format" > $DATA_FILE
    $CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE')"
done

rm $DATA_FILE

$CLICKHOUSE_LOCAL -q "select * from generateRandom('a UInt64, b String, c Array(UInt64), d Tuple(a UInt64, b String)', 42) limit 10 format JSONEachRow" > $DATA_FILE.jsonl
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE*')"


$CLICKHOUSE_LOCAL -q "select * from generateRandom('a UInt64, b String, c Array(UInt64), d Tuple(a UInt64, b String)', 42) limit 10 format JSONEachRow" > $DATA_FILE

$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE', auto, 'a UInt64, b String, c Array(UInt64), d Tuple(a UInt64, b String)')"

$CLICKHOUSE_LOCAL -mq "
desc file('$DATA_FILE');
desc file('$DATA_FILE');
"

$CLICKHOUSE_LOCAL -mq "
desc file('$DATA_FILE', JSONEachRow);
desc file('$DATA_FILE');
"

touch $DATA_FILE.1
$CLICKHOUSE_LOCAL -q "select * from generateRandom('a UInt64, b String, c Array(UInt64), d Tuple(a UInt64, b String)', 42) limit 10 format JSONEachRow" > $DATA_FILE.2
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE.{1,2}')"
$CLICKHOUSE_LOCAL -q "desc file('$DATA_FILE.{1,2}') settings schema_inference_mode='union'" 2>&1 | grep -c "CANNOT_DETECT_FORMAT"

$CLICKHOUSE_LOCAL -mq "
desc file('$DATA_FILE.2');
desc file('$DATA_FILE.{1,2}');
"

rm $DATA_FILE*
