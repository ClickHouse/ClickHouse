#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: `ipcrypt2` is not built in the fast test.

# --dump-schema cases from review: a sharding key after a qualified remote() target, the ipcrypt gate, a computed
# CAST target type, row data of values() that looks like a type, and a failed write to stdout.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

work="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "$work"
mkdir -p "$work"
trap 'rm -rf "$work"' EXIT

# Creates the objects in a fresh source, dumps them and replays the dump into a fresh target.
round_trip() {
    local name=$1 ddl=$2
    rm -rf "$work/src" "$work/dst"
    $CLICKHOUSE_LOCAL --path "$work/src" -n -q "$ddl"
    $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d < /dev/null > "$work/$name.sql" 2>/dev/null
    echo "$name: dump rc=$?"
    grep '^SET ' "$work/$name.sql"
    $CLICKHOUSE_LOCAL --path "$work/dst" -n --queries-file "$work/$name.sql" 2>&1 | grep -o 'Code: [0-9]*' | head -1
    echo "$name: replay rc=${PIPESTATUS[0]}"
}

round_trip remote_sharding_key "
    CREATE DATABASE d;
    CREATE TABLE d.src (x UInt8) ENGINE = MergeTree ORDER BY x;
    CREATE VIEW d.v AS SELECT * FROM remote('127.0.0.1', d.src, rand());"

round_trip ipcrypt "
    SET enable_ipcrypt = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (ip IPv6, encrypted IPv6 DEFAULT ipcryptEncrypt(ip, unhex('00112233445566778899aabbccddeeff')))
    ENGINE = MergeTree ORDER BY ip;"

round_trip computed_cast "
    SET allow_suspicious_low_cardinality_types = 1;
    CREATE DATABASE d;
    CREATE TABLE d.t (y UInt8, x UInt8 DEFAULT isNull(CAST(1, concat('LowCardinality(', 'UInt64)'))))
    ENGINE = MergeTree ORDER BY y;"

# A plain materialized view: a refreshable one starts a refresh that can still run when clickhouse-local exits.
round_trip values_rows "
    CREATE DATABASE d;
    CREATE TABLE d.src (s String) ENGINE = MergeTree ORDER BY s;
    CREATE TABLE d.dst (s String) ENGINE = MergeTree ORDER BY s;
    CREATE MATERIALIZED VIEW d.mv TO d.dst AS SELECT s FROM d.src WHERE s IN (SELECT s FROM values('s String', 'x LowCardinality(UInt64)'));"

# A dump that cannot be written must not report success.
if $CLICKHOUSE_LOCAL --path "$work/src" --dump-schema d < /dev/null >&- 2>/dev/null; then
    echo "closed stdout: success"
else
    echo "closed stdout: failed"
fi
