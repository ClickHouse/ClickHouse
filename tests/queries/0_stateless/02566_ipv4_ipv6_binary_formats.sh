#!/usr/bin/env bash
# Tags: no-fasttest

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

filename="${CLICKHOUSE_TEST_UNIQUE_NAME}"_02566_ipv4_ipv6
echo "CapnProto"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format CapnProto settings format_schema='$CURDIR/format_schemas/02566_ipv4_ipv6:Message'" > $filename.capnp
${CLICKHOUSE_LOCAL} -q "select * from file($filename.capnp, auto, 'ipv6 IPv6, ipv4 IPv4') settings format_schema='$CURDIR/format_schemas/02566_ipv4_ipv6:Message'"
rm $filename.capnp

echo "Avro"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Avro"  > $filename.avro
${CLICKHOUSE_LOCAL} -q "select * from file($filename.avro, auto, 'ipv6 IPv6, ipv4 IPv4')"
rm $filename.avro

echo "Arrow"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Arrow"  > $filename.arrow
${CLICKHOUSE_LOCAL} -q "select * from file($filename.arrow, auto, 'ipv6 IPv6, ipv4 IPv4')"
rm $filename.arrow

echo "Parquet"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format Parquet"  > $filename.parquet
${CLICKHOUSE_LOCAL} -q "desc file($filename.parquet)"
${CLICKHOUSE_LOCAL} -q "select ipv6, toIPv4(ipv4) from file($filename.parquet, auto, 'ipv6 IPv6, ipv4 UInt32')"
rm $filename.parquet

echo "ORC"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format ORC"  > $filename.orc
${CLICKHOUSE_LOCAL} -q "desc file($filename.orc)"
${CLICKHOUSE_LOCAL} -q "select ipv6, toIPv4(ipv4) from file($filename.orc, auto, 'ipv6 IPv6, ipv4 UInt32')"
rm $filename.orc

echo "BSONEachRow"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format BSONEachRow"  > $filename.bson
${CLICKHOUSE_LOCAL} -q "select * from file($filename.bson, auto, 'ipv6 IPv6, ipv4 IPv4')"
rm $filename.bson

echo "MsgPack"
${CLICKHOUSE_LOCAL} -q "select '2001:db8:11a3:9d7:1f34:8a2e:7a0:765d'::IPv6 as ipv6, '127.0.0.1'::IPv4 as ipv4 format MsgPack"  > $filename.msgpack
${CLICKHOUSE_LOCAL} -q "select * from file($filename.msgpack, auto, 'ipv6 IPv6, ipv4 IPv4')"
rm $filename.msgpack


