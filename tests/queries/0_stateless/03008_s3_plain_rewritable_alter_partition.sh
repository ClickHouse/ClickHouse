#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# Tag no-shared-merge-tree: does not support replication

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 03008_alter_partition"
${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 03008_alter_partition_dst"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE 03008_alter_partition (a Int32, b Int32) ENGINE = MergeTree() PARTITION BY intDiv(a, 20) ORDER BY a
SETTINGS disk = disk(
    name = 03008_alter_partition,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_alter_partition/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"
${CLICKHOUSE_CLIENT} --query "
INSERT INTO 03008_alter_partition (*) SELECT number, number % 3 from numbers_mt(100);
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

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE 03008_alter_partition_dst (a Int32, b Int32) ENGINE = MergeTree() PARTITION BY intDiv(a, 20) ORDER BY a
SETTINGS disk = disk(
    name = 03008_alter_partition,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_alter_partition/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
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
SELECT DISTINCT b FROM 03008_alter_partition_dst WHERE a >=80 ORDER BY b;
"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 03008_alter_partition_dst SYNC"
${CLICKHOUSE_CLIENT} --query "DROP TABLE 03008_alter_partition SYNC"
