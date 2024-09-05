#!/usr/bin/env bash
# Tags: distributed
# https://github.com/ClickHouse/ClickHouse/issues/65520

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh


${CLICKHOUSE_CLIENT} -q "drop database if exists ${CLICKHOUSE_DATABASE}_db on cluster default"
${CLICKHOUSE_CLIENT} -q "create database ${CLICKHOUSE_DATABASE}_db on cluster default"

${CLICKHOUSE_CLIENT} -q "drop table if exists ${CLICKHOUSE_DATABASE}_db.segfault_table on cluster default"
${CLICKHOUSE_CLIENT} -q "drop table if exists ${CLICKHOUSE_DATABASE}_db.__segfault_table on cluster default"


${CLICKHOUSE_CLIENT} -q "
create table ${CLICKHOUSE_DATABASE}_db.__segfault_table on cluster default ( 
  c_mpcnr33 Int32 primary key,
  c_v8s String,
  c_l Int32,
  c_jismi1 String,
  c_p37t64z75 Bool not null,
  c_uz Bool,
  c_rp Int32 primary key,
  c_d56dwp13jp Bool,
  c_sf__xnd4 Float64 not null
)"

${CLICKHOUSE_CLIENT} -q "create table ${CLICKHOUSE_DATABASE}_db.segfault_table on cluster default as ${CLICKHOUSE_DATABASE}_db.__segfault_table ENGINE = Distributed(default, ${CLICKHOUSE_DATABASE}_db, __segfault_table, c_mpcnr33)"

${CLICKHOUSE_CLIENT} -q "
create table ${CLICKHOUSE_DATABASE}_db.__t_nh1w on cluster default ( 
  c_sfdzg Int32 ,
  c_xf Bool ,
  c_u3xs92nr4c String ,
  c_b_m Int32 primary key ,
  c_lgy Int32 ,
)"

${CLICKHOUSE_CLIENT} -q "create table ${CLICKHOUSE_DATABASE}_db.t_nh1w on cluster default as ${CLICKHOUSE_DATABASE}_db.__t_nh1w ENGINE = Distributed(default, ${CLICKHOUSE_DATABASE}_db, __t_nh1w, c_b_m)"


query="insert into ${CLICKHOUSE_DATABASE}_db.segfault_table (c_mpcnr33, c_v8s, c_l, c_jismi1, c_p37t64z75, c_uz, c_rp, c_d56dwp13jp, c_sf__xnd4) values (868701807, coalesce((select c_u3xs92nr4c from ${CLICKHOUSE_DATABASE}_db.t_nh1w order by c_u3xs92nr4c limit 1 offset 6), 'llwlzwb3'), 1824351772, coalesce(MACNumToString(lcm(-3, -6)), 'f'))"

curl -d@- -sS "${CLICKHOUSE_URL}" <<< "$query"


${CLICKHOUSE_CLIENT} -q "drop table if exists ${CLICKHOUSE_DATABASE}_db.segfault_table on cluster default"
${CLICKHOUSE_CLIENT} -q "drop table if exists ${CLICKHOUSE_DATABASE}_db.__segfault_table on cluster default"
