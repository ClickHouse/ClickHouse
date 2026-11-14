#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Formats whose scalar reader keeps single quotes as a part of the value must infer String for a quoted address
# (a schema that says IPv4/IPv6 would fail to read it), while Quoted values and array elements still infer IP types.
SETTINGS="--input_format_try_infer_ipv4=1 --input_format_try_infer_ipv6=1"

check_string()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS -q "SELECT toTypeName(c1) IN ('String', 'Nullable(String)'), length(c1) FROM format($1, '$2')"
}

check_ip()
{
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS -q "SELECT toTypeName(c1) LIKE '%$1%', c1 FROM format($2, '$3')"
}

for ip in "192.168.1.1" "2001:db8::1"; do
    echo "Literal quotes stay String: $ip"
    check_string TSV "\'$ip\'\n"
    check_string TSVRaw "\'$ip\'\n"
    check_string CSV "\"\'$ip\'\"\n"
    # shellcheck disable=SC2086
    $CLICKHOUSE_CLIENT $SETTINGS -q "SELECT toTypeName(a) IN ('String', 'Nullable(String)'), length(a) FROM format(TSKV, 'a=\'$ip\'\n')"
done

echo 'Quoted and nested values infer IP types'
check_ip IPv4 Values "(\'192.168.1.1\')"
check_ip IPv6 Values "(\'2001:db8::1\')"
check_ip IPv4 TSV "192.168.1.1\n"
check_ip IPv6 TSV "2001:db8::1\n"
check_ip IPv4 TSV "[\'192.168.1.1\']\n"
check_ip IPv6 TSV "[\'2001:db8::1\']\n"
check_ip IPv4 CSV "\"192.168.1.1\"\n"
check_ip IPv6 CSV "\"2001:db8::1\"\n"
