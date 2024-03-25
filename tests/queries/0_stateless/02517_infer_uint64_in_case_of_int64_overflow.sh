#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

echo -ne "18446744073709551615" | $CLICKHOUSE_LOCAL --table=test --input-format=CSV -q "desc test";
echo -ne '"[18446744073709551615, 10, 11]"' | $CLICKHOUSE_LOCAL --table=test --input-format=CSV -q "desc test";
echo -ne "18446744073709551615\n10\n11" | $CLICKHOUSE_LOCAL --table=test  --input-format=CSV -q "desc test";
echo -ne "18446744073709551615" | $CLICKHOUSE_LOCAL --table=test --input-format=TSV -q "desc test";
echo -ne "[18446744073709551615, 10, 11]" | $CLICKHOUSE_LOCAL --table=test --input-format=TSV -q "desc test";
echo -ne "18446744073709551615\n10\n11" | $CLICKHOUSE_LOCAL --table=test --input-format=TSV -q "desc test";
echo -ne '{"number" : 18446744073709551615}' | $CLICKHOUSE_LOCAL --table=test --input-format=JSONEachRow -q "desc test";
echo -ne '{"number" : [18446744073709551615, 10, 11]}'| $CLICKHOUSE_LOCAL --table=test --input-format=JSONEachRow -q "desc test";
echo -ne '{"number" : [18446744073709551615, true, 11]}'| $CLICKHOUSE_LOCAL --table=test --input-format=JSONEachRow -q "desc test";
echo -ne '{"number" : 18446744073709551615}, {"number" : 10}, {"number" : 11}' | $CLICKHOUSE_LOCAL --table=test --input-format=JSONEachRow -q "desc test";
echo -ne '{"number" : 18446744073709551615}, {"number" : false}, {"number" : 11}' | $CLICKHOUSE_LOCAL --table=test --input-format=JSONEachRow -q "desc test";
echo -ne '{"number" : "18446744073709551615"}' | $CLICKHOUSE_LOCAL --input_format_json_try_infer_numbers_from_strings=1 --table=test --input-format=JSONEachRow -q "desc test";
