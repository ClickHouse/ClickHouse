#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS null_as_default";
$CLICKHOUSE_CLIENT --query="CREATE TABLE null_as_default (i Int8, s String DEFAULT 'Hello', n UInt64 DEFAULT 42, d Date DEFAULT '2019-06-19', a Array(UInt8) DEFAULT [1, 2, 3], t Tuple(String, Float64) DEFAULT ('default', i / 4)) ENGINE = Memory";

echo 'CSV'
echo '\N, 1, \N, "2019-07-22", "[10, 20, 30]", \N
1, world, 3, "2019-07-23", \N, tuple, 3.14
2, \N, 123, \N, "[]", test, 2.71828
3, \N, \N, \N, \N, \N' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT CSV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'TSV'
echo -e '\N\t1\t\N\t2019-07-22\t[10, 20, 30]\t\N
1\tworld\t3\t2019-07-23\t\N\t('\''tuple'\'', 3.14)
2\t\N\t123\t\N\t[]\t('\''test'\'', 2.71828)
3\t\N\t\N\t\N\t\N\t\N' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT TSV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'TSKV'
echo -e 'i=\N\ts=1\tn=\N\td=2019-07-22\ta=[10, 20, 30]\tt=\N
i=1\ts=world\tn=3\td=2019-07-23\ta=\N\tt=('\''tuple'\'', 3.14)
i=2\ts=\N\tn=123\td=\N\ta=[]\tt=('\''test'\'', 2.71828)
i=3\ts=\N\tn=\N\td=\N\ta=\N\tt=\N' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT TSKV";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'JSONEachRow'
echo '{"i": null, "s": "1", "n": null, "d": "2019-07-22", "a": [10, 20, 30], "t": null}
{"i": 1, "s": "world", "n": 3, "d": "2019-07-23", "a": null, "t": ["tuple", 3.14]}
{"i": 2, "s": null, "n": 123, "d": null, "a": [], "t": ["test", 2.71828]}
{"i": 3, "s": null, "n": null, "d": null, "a": null, "t": null}' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT JSONEachRow";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'JSONStringsEachRow'
echo '{"i": "null", "s": "1", "n": "ᴺᵁᴸᴸ", "d": "2019-07-22", "a": "[10, 20, 30]", "t": "NULL"}
{"i": "1", "s": "world", "n": "3", "d": "2019-07-23", "a": "null", "t": "('\''tuple'\'', 3.14)"}
{"i": "2", "s": "null", "n": "123", "d": "null", "a": "[]", "t": "('\''test'\'', 2.71828)"}
{"i": "3", "s": "null", "n": "null", "d": "null", "a": "null", "t": "null"}' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT JSONStringsEachRow";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'Template (Quoted)'
echo 'NULL, '\''1'\'', null, '\''2019-07-22'\'', [10, 20, 30], NuLl
1, '\''world'\'', 3, '\''2019-07-23'\'', NULL, ('\''tuple'\'', 3.14)
2, null, 123, null, [], ('\''test'\'', 2.71828)
3, null, null, null, null, null' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --format_custom_escaping_rule=Quoted --format_custom_field_delimiter=', ' --query="INSERT INTO null_as_default FORMAT CustomSeparated";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="TRUNCATE TABLE null_as_default";

echo 'Values'
echo '(NULL, '\''1'\'', (null), '\''2019-07-22'\'', ([10, 20, 30]), (NuLl)),
(1, '\''world'\'', (3), '\''2019-07-23'\'', (NULL), ('\''tuple'\'', 3.14)),
(2, null, (123), null, ([]), ('\''test'\'', 2.71828)),
(3, null, (null), null, (null), (null))' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default VALUES";
$CLICKHOUSE_CLIENT --query="SELECT * FROM null_as_default ORDER BY i";
$CLICKHOUSE_CLIENT --query="DROP TABLE null_as_default";
