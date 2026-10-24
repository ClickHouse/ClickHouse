#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# 1. Settings disabled by default: IPv4-looking string is stored as String in shared data
echo '1. Default: no IP inference'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ip": "192.168.1.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=0 --input_format_try_infer_ipv6=0 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT json.ip FROM t"
$CLICKHOUSE_CLIENT -q "SELECT JSONSharedDataPaths(json) FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 2. IPv4 inference enabled: string matching IPv4 pattern is inferred as IPv4
echo '2. IPv4 inference enabled'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ip": "192.168.1.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT json.ip FROM t"
$CLICKHOUSE_CLIENT -q "SELECT JSONSharedDataPaths(json) FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 3. IPv6 inference enabled: string matching IPv6 pattern is inferred as IPv6
echo '3. IPv6 inference enabled'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ip": "2001:db8::1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv6=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT json.ip FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 4. Non-IP strings are not affected even when inference is enabled
echo '4. Non-IP strings stay String'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
printf '{"json": {"ip": "1.2.3"}}
{"json": {"ip": "256.0.0.1"}}
{"json": {"ip": "not-an-ip"}}
' | \
    $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 5. Both settings enabled together: each value is inferred to its correct IP type
echo '5. Both IPv4 and IPv6 inference'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
printf '{"json": {"ip": "192.168.1.1"}}
{"json": {"ip": "2001:db8::1"}}
' | \
    $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 --input_format_try_infer_ipv6=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip), json.ip FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 6. Typed path declared in the schema takes precedence over inference settings
echo '6. Typed path takes precedence'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(ip IPv4)) ENGINE = Memory"
echo '{"json": {"ip": "10.0.0.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=0 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT toTypeName(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT json.ip FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 7. IP inference works independently of date/datetime inference settings
echo '7. IP inference with dates/datetimes disabled'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ip": "10.0.0.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_dates=0 --input_format_try_infer_datetimes=0 --input_format_try_infer_ipv4=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 8. Pure IPv4 literal is not inferred as IPv6 when only ipv6 inference is enabled
echo '8. IPv4 literal not inferred as IPv6'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ip": "192.168.1.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=0 --input_format_try_infer_ipv6=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip) FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 9. JSON dynamic path: first IP string on a new dynamic path should create IPv4 variant
echo '9. Dynamic path IPv4 inference'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=1)) ENGINE = Memory"
echo '{"json": {"ip": "192.168.1.1"}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ip), json.ip FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 10. Nested array of IP strings through shared-data fallback should become Array(IPv4)
echo '10. Dynamic path Array(IPv4) inference'
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS t"
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (json JSON(max_dynamic_paths=0)) ENGINE = Memory"
echo '{"json": {"ips": ["192.168.1.1", "10.0.0.1"]}}' | $CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "INSERT INTO t FORMAT JSONEachRow"
$CLICKHOUSE_CLIENT -q "SELECT dynamicType(json.ips), json.ips FROM t"
$CLICKHOUSE_CLIENT -q "DROP TABLE t"

# 11. CSV IPv4 inference (unquoted field)
echo '11. CSV IPv4 inference'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "desc format(CSV, '192.168.1.1
')"

# 12. CSV IPv6 inference (unquoted field)
echo '12. CSV IPv6 inference'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv6=1 -q "desc format(CSV, '2001:db8::1
')"

# 13. TSV IPv4 inference
echo '13. TSV IPv4 inference'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "desc format(TSV, '192.168.1.1
')"

# 14. Mixed IPv4 and String collapses to String
echo '14. Mixed IPv4 and String collapses to String'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "DESCRIBE format(JSONEachRow, '{\"ip\":\"192.168.1.1\"}
{\"ip\":\"not-an-ip\"}')"

# 15. Mixed IPv4 and IPv6 collapses to String
echo '15. Mixed IPv4 and IPv6 collapses to String'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 --input_format_try_infer_ipv6=1 -q "DESCRIBE format(JSONEachRow, '{\"ip\":\"192.168.1.1\"}
{\"ip\":\"2001:db8::1\"}')"

# 16. Values format quoted-string IPv4 inference
echo '16. Values format IPv4 inference'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "desc format(Values, '(\'192.168.1.1\')')"

# 17. Values format mixed IPv4 string and non-IP string collapses to String
echo '17. Values format mixed IPv4 and String collapses to String'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "desc format(Values, '(\'192.168.1.1\')\n(\'not-an-ip\')')"

# 18. An IPv4 address after an IPv6 one in a dynamic path keeps its family, as in shared data
echo '18. IPv4 after IPv6 in a dynamic path'
for settings in "--input_format_try_infer_ipv4=0 --input_format_try_infer_ipv6=1" "--input_format_try_infer_ipv4=1 --input_format_try_infer_ipv6=1"; do
    for max_dynamic_paths in 2 0; do
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $settings --input_format_parallel_parsing=0 -q "
            SELECT dynamicType(j.ip), toString(j.ip), dynamicType(j.a), toString(j.a)
            FROM format(JSONEachRow, 'j JSON(max_dynamic_paths=$max_dynamic_paths)', '{\"j\":{\"ip\":\"2001:db8::1\",\"a\":[\"2001:db8::2\"]}}
{\"j\":{\"ip\":\"192.168.1.1\",\"a\":[\"10.0.0.1\"]}}')"
    done
done

# 19. JSONExtract into Dynamic keeps every element of a mixed IP/text array
echo '19. JSONExtract mixed IP and text arrays'
for setting in "--input_format_try_infer_ipv4=1" "--input_format_try_infer_ipv6=1"; do
    for rows in "('[\"192.168.1.1\"]'), ('[\"10.0.0.1\",\"not-an-ip\"]')" "('[\"10.0.0.1\",\"not-an-ip\"]'), ('[\"192.168.1.1\"]')" \
                "('[\"2001:db8::1\"]'), ('[\"2001:db8::2\",\"not-an-ip\"]')" "('[\"2001:db8::2\",\"not-an-ip\"]'), ('[\"2001:db8::1\"]')"; do
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $setting -q "SELECT toString(d), dynamicType(d) FROM (SELECT JSONExtract(j, 'Dynamic') AS d FROM values('j String', $rows))"
    done
done

# 20. Escaped IP text is not inferred as an IP type, so the inferred schema reads it
echo '20. Escaped IP text'
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "DESCRIBE format(JSONEachRow, '{\"a\":\"192.168.1.\\\\u0031\"}')"
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv4=1 -q "SELECT a FROM format(JSONEachRow, '{\"a\":\"192.168.1.\\\\u0031\"}')"
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv6=1 -q "DESCRIBE format(JSONEachRow, '{\"a\":\"2001:db8:\\\\u003a1\"}')"
$CLICKHOUSE_CLIENT --input_format_try_infer_ipv6=1 -q "SELECT a FROM format(JSONEachRow, '{\"a\":\"2001:db8:\\\\u003a1\"}')"

# 21. TSKV decodes escapes before inference, so an escaped IP is not inferred as an IP type
echo '21. TSKV escaped IP text'
for setting in "--input_format_try_infer_ipv4=1" "--input_format_try_infer_ipv4=0"; do
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting -q "DESCRIBE format(TSKV, 'ip=192.168.1.\\\\x31\\n')"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting -q "SELECT ip FROM format(TSKV, 'ip=192.168.1.\\\\x31\\n')"
done
for setting in "--input_format_try_infer_ipv6=1" "--input_format_try_infer_ipv6=0"; do
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting -q "DESCRIBE format(TSKV, 'ip=2001:db8:\\\\x3a1\\n')"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting -q "SELECT ip FROM format(TSKV, 'ip=2001:db8:\\\\x3a1\\n')"
done

# 22. The same holds for escaped IPs inside TSKV arrays, tuples, maps and variants
echo '22. TSKV escaped IP text in compound values'
for value in "[\\'192.168.1.\\\\x31\\']" "(\\'192.168.1.\\\\x31\\', 1)" "{\\'192.168.1.\\\\x31\\':\\'k\\'}" "{\\'k\\':\\'192.168.1.\\\\x31\\'}" "[\\'2001:db8:\\\\x3a1\\']"; do
    for setting in "--input_format_try_infer_ipv4=1 --input_format_try_infer_ipv6=1" "--input_format_try_infer_ipv4=0 --input_format_try_infer_ipv6=0"; do
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $setting -q "DESCRIBE format(TSKV, 'ip=$value\\n')"
        # shellcheck disable=SC2086
        $CLICKHOUSE_CLIENT $setting -q "SELECT ip FROM format(TSKV, 'ip=$value\\n')"
    done
done
for setting in "--input_format_try_infer_ipv4=1" "--input_format_try_infer_ipv4=0"; do
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting --input_format_try_infer_variants=1 -q "DESCRIBE format(TSKV, 'ip=[\\'192.168.1.\\\\x31\\', [1]]\\n')"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $setting --input_format_try_infer_variants=1 -q "SELECT ip FROM format(TSKV, 'ip=[\\'192.168.1.\\\\x31\\', [1]]\\n')"
done
