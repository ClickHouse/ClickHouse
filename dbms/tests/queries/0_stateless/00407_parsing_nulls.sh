#!/usr/bin/env bash

set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

echo -ne '\\tHello\t123\t\\N\n\\N\t\t2000-01-01 00:00:00\n' | ${CLICKHOUSE_LOCAL} --input-format=TabSeparated --output-format=TabSeparated --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne 'Hello,123,\\N\n\\N,0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=TabSeparated --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '"\\Hello",123,\\N\n"\\N",0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=TabSeparated --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '{"s" : null, "x" : 123}, {"s" : "\N", "t":"2000-01-01 00:00:00"}' | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --output-format=TabSeparated --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo "(NULL, 111, '2000-01-01 00:00:00'), ('\N', NULL, NULL), ('a\Nb', NULL, NULL)" | ${CLICKHOUSE_LOCAL} --input-format=Values --output-format=TabSeparated --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"

echo -ne '\\tHello\t123\t\\N\n\\N\t\t2000-01-01 00:00:00\n' | ${CLICKHOUSE_LOCAL} --input-format=TabSeparated --output-format=CSV --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne 'Hello,123,\\N\n\\N,0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=CSV --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '"\\Hello",123,\\N\n"\\N",0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=CSV --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '{"s" : null, "x" : 123}, {"s" : "\N", "t":"2000-01-01 00:00:00"}' | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --output-format=CSV --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo "(NULL, 111, '2000-01-01 00:00:00'), ('\N', NULL, NULL), ('a\Nb', NULL, NULL)" | ${CLICKHOUSE_LOCAL} --input-format=Values --output-format=CSV --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"

echo -ne '\\tHello\t123\t\\N\n\\N\t\t2000-01-01 00:00:00\n' | ${CLICKHOUSE_LOCAL} --input-format=TabSeparated --output-format=JSONEachRow --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne 'Hello,123,\\N\n\\N,0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=JSONEachRow --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '"\\Hello",123,\\N\n"\\N",0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=JSONEachRow --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '{"s" : null, "x" : 123}, {"s" : "\N", "t":"2000-01-01 00:00:00"}' | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --output-format=JSONEachRow --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo "(NULL, 111, '2000-01-01 00:00:00'), ('\N', NULL, NULL), ('a\Nb', NULL, NULL)" | ${CLICKHOUSE_LOCAL} --input-format=Values --output-format=JSONEachRow --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"

echo -ne '\\tHello\t123\t\\N\n\\N\t\t2000-01-01 00:00:00\n' | ${CLICKHOUSE_LOCAL} --input-format=TabSeparated --output-format=Values --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne 'Hello,123,\\N\n\\N,0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=Values --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '"\\Hello",123,\\N\n"\\N",0,"2000-01-01 00:00:00"' | ${CLICKHOUSE_LOCAL} --input-format=CSV --output-format=Values --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo -ne '{"s" : null, "x" : 123}, {"s" : "\N", "t":"2000-01-01 00:00:00"}' | ${CLICKHOUSE_LOCAL} --input-format=JSONEachRow --output-format=Values --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
echo "(NULL, 111, '2000-01-01 00:00:00'), ('\N', NULL, NULL), ('a\Nb', NULL, NULL)" | ${CLICKHOUSE_LOCAL} --input-format=Values --output-format=Values --structure='s Nullable(String), x Nullable(UInt64), t Nullable(DateTime)' --query="SELECT * FROM table"
