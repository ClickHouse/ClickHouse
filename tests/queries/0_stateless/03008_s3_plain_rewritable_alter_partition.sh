#!/usr/bin/env bash
# Tags: no-fasttest, no-shared-merge-tree
# Tag no-shared-merge-tree: does not support replication

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS 03008_alter_partition"

${CLICKHOUSE_CLIENT} -m --query "
CREATE TABLE 03008_alter_partition (a Int32, b Int64) ENGINE = MergeTree() PARTITION BY intDiv(a, 1000) ORDER BY tuple(a, b)
SETTINGS disk = disk(
    name = 03008_alter_partition,
    type = s3_plain_rewritable,
    endpoint = 'http://localhost:11111/test/03008_alter_partition/',
    access_key_id = clickhouse,
    secret_access_key = clickhouse);
"
${CLICKHOUSE_CLIENT} --query "
INSERT INTO 03008_alter_partition (*) SELECT number, number from numbers_mt(10000);
"
${CLICKHOUSE_CLIENT} --query "OPTIMIZE TABLE 03008_alter_partition FINAL"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DROP PARTITION 0"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition DROP PART '1_2_2_1'"

${CLICKHOUSE_CLIENT} --query "ALTER TABLE 03008_alter_partition ATTACH PARTITION 2" 2>&1 |
    grep -Fq "SUPPORT_IS_DISABLED"

${CLICKHOUSE_CLIENT} --query "SELECT count(*) FROM 03008_alter_partition"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 03008_alter_partition SYNC"
