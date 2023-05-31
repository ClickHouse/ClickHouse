#!/usr/bin/env bash
# Tags: no-fasttest, use-maxminddb

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The file was downloaded from https://github.com/P3TERX/GeoLite.mmdb
MMDB_FILE=$CUR_DIR/data_mmdb/GeoLite2-Country.mmdb

${CLICKHOUSE_CLIENT} --query="drop table if exists test_mmdb"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_mmdb (key String, value String) ENGINE = MaxMindDB('$MMDB_FILE') PRIMARY KEY key"
${CLICKHOUSE_CLIENT} --query="select * from test_mmdb where key in ('1.2.2.3', '1.2.2.2')"

${CLICKHOUSE_CLIENT} --query="drop table if exists test_mmdb"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_mmdb (key IPv4, value String) ENGINE = MaxMindDB('$MMDB_FILE') PRIMARY KEY key"
${CLICKHOUSE_CLIENT} --query="select * from test_mmdb where key in ('1.2.2.3', '1.2.2.2')"

${CLICKHOUSE_CLIENT} --query="drop table if exists test_mmdb"
${CLICKHOUSE_CLIENT} --query="CREATE TABLE test_mmdb (key IPv6, value String) ENGINE = MaxMindDB('$MMDB_FILE') PRIMARY KEY key"
${CLICKHOUSE_CLIENT} --query="select * from test_mmdb where key in ('1.2.2.3', '1.2.2.2')"
