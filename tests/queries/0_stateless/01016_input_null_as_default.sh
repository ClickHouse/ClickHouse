#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS null_as_default";
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS default_by_other_column";
$CLICKHOUSE_CLIENT --query="CREATE TABLE null_as_default (i Int8, s String DEFAULT 'Hello', n UInt64 DEFAULT 42, d Date DEFAULT '2019-06-19', a Array(UInt8) DEFAULT [1, 2, 3], t Tuple(String, Float64) DEFAULT ('default', i / 4)) ENGINE = Memory";
$CLICKHOUSE_CLIENT --query="CREATE TABLE default_by_other_column (a Float32 DEFAULT 100, b Float64 DEFAULT a, c Tuple(String, Float64) DEFAULT ('default', b / 4)) ENGINE = Memory";

echo 'CSV'
# `t` occupies one cell per element here, because input_format_csv_deserialize_separate_columns_into_tuple
# is on by default, so its nulls are element values and give the element defaults rather than the
# column DEFAULT. The other formats below read the tuple as a single field and still take the DEFAULT.
echo '\N, 1, \N, "2019-07-22", "[10, 20, 30]", \N, \N
1, world, 3, "2019-07-23", \N, tuple, 3.14
2, \N, 123, \N, "[]", test, 2.71828
3, \N, \N, \N, \N, \N, \N' | $CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO null_as_default FORMAT CSV";
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

echo 'default_by_other_column'
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO default_by_other_column(c) VALUES(null)";
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO default_by_other_column(b, c) VALUES(null, null)";
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO default_by_other_column(a, b, c) VALUES(null, null, null)";
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO default_by_other_column(a) VALUES(10)";
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query="INSERT INTO default_by_other_column(a, b, c) VALUES(1, 2, ('tuple', 3))";
$CLICKHOUSE_CLIENT --query="SELECT * FROM default_by_other_column ORDER BY a";
$CLICKHOUSE_CLIENT --query="DROP TABLE default_by_other_column";

echo 'JSONColumns, JSONCompactColumns, JSONColumnsWithMetadata'
# https://github.com/ClickHouse/ClickHouse/issues/121905
$CLICKHOUSE_CLIENT --query="DROP TABLE IF EXISTS null_as_default_json_columns";
$CLICKHOUSE_CLIENT --query="CREATE TABLE null_as_default_json_columns (a UInt32, b UInt32 DEFAULT a*2) ENGINE = Memory";
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query='INSERT INTO null_as_default_json_columns FORMAT JSONCompactColumns [[7],[null]]';
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query='INSERT INTO null_as_default_json_columns FORMAT JSONColumnsWithMetadata {"meta":[{"name":"a","type":"UInt32"},{"name":"b","type":"UInt32"}],"data":{"a":[8],"b":[null]}}';
# Omitting the "b" column entirely gives a*2 as well.
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query='INSERT INTO null_as_default_json_columns FORMAT JSONColumns {"a":[9]}';
# Only the null rows take the DEFAULT, 100 stays.
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --query='INSERT INTO null_as_default_json_columns FORMAT JSONColumns {"a":[10,11,12],"b":[null,100,null]}';
# With input_format_defaults_for_omitted_fields = 0 a null stays 0, as in JSONEachRow.
$CLICKHOUSE_CLIENT --input_format_null_as_default=1 --input_format_defaults_for_omitted_fields=0 --query='INSERT INTO null_as_default_json_columns FORMAT JSONColumns {"a":[13],"b":[null]}';
# An empty string with input_format_json_empty_as_default = 1 takes the DEFAULT too.
$CLICKHOUSE_CLIENT --input_format_json_empty_as_default=1 --query='INSERT INTO null_as_default_json_columns FORMAT JSONColumns {"a":[14],"b":[""]}';
$CLICKHOUSE_CLIENT --query="SELECT a, b FROM null_as_default_json_columns ORDER BY a";
$CLICKHOUSE_CLIENT --query="DROP TABLE null_as_default_json_columns";
