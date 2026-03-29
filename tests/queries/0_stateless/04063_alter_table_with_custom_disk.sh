#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: Depends on S3
# Tag no-replicated-database: plain rewritable should not be shared between replicas

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --alter_sync 2 -nm -q "
    DROP TABLE IF EXISTS 04063_data SYNC;

    CREATE TABLE 04063_data
    (
        key Int32,
        val String
    )
    ENGINE = MergeTree()
    ORDER BY key
    SETTINGS disk = disk(
        name = '04063_disk_$CLICKHOUSE_DATABASE',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/04063_data_$CLICKHOUSE_DATABASE/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse
    );

    INSERT INTO 04063_data
    VALUES (1, 'foo'), (2, 'bar');

    ALTER TABLE 04063_data ADD COLUMN dt Date DEFAULT '2026-03-29';
    ALTER TABLE 04063_data MODIFY SETTING min_bytes_for_wide_part = 1000;
    ALTER TABLE 04063_data MODIFY COMMENT 'with comment';

    SELECT * FROM 04063_data ORDER BY key;

    SHOW CREATE TABLE 04063_data;
"

echo "# reject replacement of custom disk with its own name"
${CLICKHOUSE_CLIENT} -q "
    ALTER TABLE 04063_data MODIFY SETTING disk = '04063_disk_$CLICKHOUSE_DATABASE'; -- { serverError BAD_ARGUMENTS }
" && echo "OK"

echo "# reject replacement of custom disk parameters"
${CLICKHOUSE_CLIENT} -nm --query "
    ALTER TABLE 04063_data
    MODIFY SETTING disk = disk(
        name = '04063_disk_$CLICKHOUSE_DATABASE',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/04063_another_data_$CLICKHOUSE_DATABASE/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse
    ); -- { serverError BAD_ARGUMENTS }
" && echo "OK"

echo "# reject replacement of custom disk with another custom disk"
${CLICKHOUSE_CLIENT} -nm --query "
    ALTER TABLE 04063_data
    MODIFY SETTING disk = disk(
        name = '04063_another_disk_$CLICKHOUSE_DATABASE',
        type = s3_plain_rewritable,
        endpoint = 'http://localhost:11111/test/04063_data_$CLICKHOUSE_DATABASE/',
        access_key_id = clickhouse,
        secret_access_key = clickhouse
    ); -- { serverError BAD_ARGUMENTS }
" && echo "OK"

${CLICKHOUSE_CLIENT} --query "DROP TABLE 04063_data SYNC"
