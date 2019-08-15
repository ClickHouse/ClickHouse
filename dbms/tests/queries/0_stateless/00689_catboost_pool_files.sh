#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

${CLICKHOUSE_CLIENT} --query="drop table if exists catboost_pool_desc;"
${CLICKHOUSE_CLIENT} --query="drop table if exists catboost_pool_vals;"
${CLICKHOUSE_CLIENT} --query="create table catboost_pool_desc (id String, type String) engine = File(TSV);"
${CLICKHOUSE_CLIENT} --query="insert into catboost_pool_desc select '0', 'Categ';"
${CLICKHOUSE_CLIENT} --query="create table catboost_pool_vals (str String) engine = File(TSV);"
${CLICKHOUSE_CLIENT} --query="insert into catboost_pool_vals select 'Hello';"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', 'data/${CLICKHOUSE_DATABASE}/catboost_pool_vals/data.TSV');"

${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('data/${CLICKHOUSE_DATABASE}/catboost_pool_desc/data.TSV', '../../../../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"

${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('${CURDIR}/00689_file.txt', '${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../${CURDIR}/00689_file.txt', '../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../../${CURDIR}/00689_file.txt', '../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../../../${CURDIR}/00689_file.txt', '../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../../../../${CURDIR}/00689_file.txt', '../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../../../../../${CURDIR}/00689_file.txt', '../../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"
${CLICKHOUSE_CLIENT} --query="select * from catBoostPool('../../../../../../${CURDIR}/00689_file.txt', '../../../../../../${CURDIR}/00689_file.txt');" 2>&1 | grep -o "Data"

${CLICKHOUSE_CLIENT} --query="drop table if exists catboost_pool_desc;"
${CLICKHOUSE_CLIENT} --query="drop table if exists catboost_pool_vals;"
