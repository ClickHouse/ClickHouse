#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree, no-distributed-cache, no-replicated-database
# Tag no-fasttest: requires Azure
# Tag no-shared-merge-tree: does not support replication
# Tag no-distributed-cache: Not supported auth type
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Azure caps container names at 64 chars, so the full ${CLICKHOUSE_TEST_UNIQUE_NAME} does not fit.
# Shorten it without losing uniqueness: a readable prefix, a hash of the *full* test name, and the
# database. Truncating the test name alone would not do - several 03008_azure_plain_rewritable_*
# tests share their first 24 characters. The test name must stay in the name at all because
# `clickhouse-test --database=X` runs every test against one shared database.
container="cont-$(echo "${CLICKHOUSE_TEST_NAME}" | tr _ - | cut -c1-16)-$(echo -n "${CLICKHOUSE_TEST_NAME}" | md5sum | cut -c1-6)-$(echo "${CLICKHOUSE_DATABASE}" | tr _ -)"

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 03008_alter_partition"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 03008_alter_partition_dst"

${CLICKHOUSE_CLIENT} -nm --query "
CREATE TABLE 03008_alter_partition (a Int32, b Int32) ENGINE = MergeTree() PARTITION BY intDiv(a, 20) order by a
SETTINGS disk = disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '${container}',
    path='/var/lib/clickhouse/disks/${container}/tables',
    container_name = '${container}',
    endpoint = 'http://localhost:10000/devstoreaccount1/${container}/plain-tables',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO 03008_alter_partition (*) SELECT number, number % 3 FROM numbers_mt(100);
"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DROP PARTITION 0"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DROP PART '1_2_2_0'"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DETACH PARTITION 2"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition ATTACH PARTITION 2"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition ATTACH PARTITION 2"

${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM 03008_alter_partition"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DETACH PARTITION 2"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DETACH PARTITION 2"
${CLICKHOUSE_CLIENT} --query "
ALTER TABLE 03008_alter_partition DROP DETACHED PARTITION 2 SETTINGS allow_drop_detached=1
"

${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM 03008_alter_partition"

${CLICKHOUSE_CLIENT} -nm --query "
CREATE TABLE 03008_alter_partition_dst (a Int32, b Int32) ENGINE = MergeTree() PARTITION BY intDiv(a, 20) order by a
SETTINGS disk = disk(
    type = object_storage,
    metadata_type = plain_rewritable,
    object_storage_type = azure_blob_storage,
    name = '${container}',
    path='/var/lib/clickhouse/disks/${container}/tables',
    container_name = '${container}',
    endpoint = 'http://localhost:10000/devstoreaccount1/${container}/plain-tables',
    account_name = 'devstoreaccount1',
    account_key = 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==');
"
${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition MOVE PARTITION 3 TO TABLE 03008_alter_partition_dst"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE 03008_alter_partition FINAL"

${CLICKHOUSE_CLIENT} -m --query "
SELECT count(*) FROM 03008_alter_partition;
SELECT count(*) FROM 03008_alter_partition_dst;
"

${CLICKHOUSE_CLIENT} --query "
INSERT INTO 03008_alter_partition_dst (*) SELECT number, number % 5 from numbers_mt(80, 20);
"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition_dst REPLACE PARTITION 4 FROM 03008_alter_partition"

${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE 03008_alter_partition_dst FINAL"

${CLICKHOUSE_CLIENT} -m --query "
SELECT count(*) FROM 03008_alter_partition;
SELECT DISTINCT b FROM 03008_alter_partition WHERE a >=80 ORDER BY b;
SELECT count(*) FROM 03008_alter_partition_dst;
SELECT DISTINCT b FROM 03008_alter_partition_dst WHERE a >=80 ORDER BY b
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 03008_alter_partition_dst SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 03008_alter_partition SYNC"
