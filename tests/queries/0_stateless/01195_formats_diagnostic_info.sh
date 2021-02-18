#!/usr/bin/env bash
# shellcheck disable=SC2206

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "$CURDIR"/../shell_config.sh

PARSER=(${CLICKHOUSE_LOCAL} --query 'SELECT t, s, d FROM table' --structure 't DateTime, s String, d Decimal64(10)' --input-format CSV)
echo '2020-04-21 12:34:56, "Hello", 12345678' | "${PARSER[@]}"  2>&1| grep "ERROR" || echo "CSV"
echo '2020-04-21 12:34:56, "Hello", 123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo '2020-04-21 12:34:567, "Hello", 123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
#echo '2020-04-21, "Hello", 123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"    # DateTime parsing is unsafe, it produces unexpected result ("Hello" is parsed as time)
echo '2020-04-21 12:34:56, "Hello", 12345678,1' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo '2020-04-21 12:34:56,,123Hello' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56, "Hello", 12345678\n' | "${PARSER[@]}" 2>&1| grep "ERROR"

PARSER=(${CLICKHOUSE_LOCAL} --query 'SELECT t, s, d FROM table' --structure 't DateTime, s String, d Decimal64(10)' --input-format CustomSeparatedIgnoreSpaces --format_custom_escaping_rule CSV --format_custom_field_delimiter ',' --format_custom_row_after_delimiter "")
echo '2020-04-21 12:34:56, "Hello", 12345678' | "${PARSER[@]}"  2>&1| grep "ERROR" || echo -e  "\nCustomSeparatedIgnoreSpaces"
echo '2020-04-21 12:34:56, "Hello", 123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo '2020-04-21 12:34:567, "Hello", 123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo '2020-04-21 12:34:56, "Hello", 12345678,1' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo '2020-04-21 12:34:56,,123Hello' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56, "Hello", 12345678\n\n\n\n   ' | "${PARSER[@]}" 2>&1| grep "ERROR" || echo "OK"

PARSER=(${CLICKHOUSE_LOCAL} --query 'SELECT t, s, d FROM table' --structure 't DateTime, s String, d Decimal64(10)' --input-format TSV)
echo -e '2020-04-21 12:34:56\tHello\t12345678' | "${PARSER[@]}"  2>&1| grep "ERROR" || echo -e "\nTSV"
echo -e '2020-04-21 12:34:56\tHello\t123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:567\tHello\t123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\tHello\t12345678\t1' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\t\t123Hello' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\tHello\t12345678\n' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '\N\tHello\t12345678' | "${PARSER[@]}" 2>&1| grep -o "Unexpected NULL value"

PARSER=(${CLICKHOUSE_LOCAL} --query 'SELECT t, s, d FROM table' --structure 't DateTime, s String, d Decimal64(10)' --input-format CustomSeparated)
echo -e '2020-04-21 12:34:56\tHello\t12345678' | "${PARSER[@]}"  2>&1| grep "ERROR" || echo -e "\nCustomSeparated"
echo -e '2020-04-21 12:34:56\tHello\t123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:567\tHello\t123456789' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\tHello\t12345678\t1' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\t\t123Hello' | "${PARSER[@]}" 2>&1| grep "ERROR"
echo -e '2020-04-21 12:34:56\tHello\t12345678\n' | "${PARSER[@]}" 2>&1| grep "ERROR"
