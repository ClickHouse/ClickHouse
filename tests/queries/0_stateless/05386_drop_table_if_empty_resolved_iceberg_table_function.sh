#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: experimental Iceberg writes.
#
# Dropping a table function that has been resolved forwards the drop to the storage behind it,
# and an Iceberg storage deletes the table's files on drop when `iceberg_delete_data_on_drop` is
# enabled in the server settings. So `DROP TABLE ... IF EMPTY` of a resolved table function must
# be judged by the rows of that storage rather than go ahead unconditionally, while `DETACH`, which
# only removes the table from the catalog, still goes ahead.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

LAKE="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_resolved_tf"
rm -rf "${LAKE}"

# One row per INSERT, so neither randomized block sizes nor `async_insert` change the data files.
${CLICKHOUSE_CLIENT} --async_insert=0 --query "
    SET allow_experimental_insert_into_iceberg = 1;
    CREATE TABLE lake (x Int32) ENGINE = IcebergLocal('${LAKE}/');
    INSERT INTO lake VALUES (1);
    INSERT INTO lake VALUES (2);
    INSERT INTO lake VALUES (3);
"

FILES_BEFORE=$(find "${LAKE}" -type f | wc -l)

# ignore_drop_queries_probability = 0: the stress runner sets it, and a rewritten `DROP` becomes a
# `TRUNCATE`.
${CLICKHOUSE_CLIENT} --query "
    SET ignore_drop_queries_probability = 0;
    CREATE TABLE tf (x Int32) AS icebergLocal('${LAKE}/');
    SELECT count() FROM tf;
    DROP TABLE IF EMPTY tf SYNC; -- { serverError TABLE_NOT_EMPTY }
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tf';
    SELECT count() FROM tf;
"

FILES_AFTER=$(find "${LAKE}" -type f | wc -l)
[ "${FILES_BEFORE}" -eq "${FILES_AFTER}" ] && echo 'files preserved'

${CLICKHOUSE_CLIENT} --query "
    SET ignore_drop_queries_probability = 0;
    DETACH TABLE IF EMPTY tf;
    SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'tf';
"

${CLICKHOUSE_CLIENT} --query "
    ATTACH TABLE tf;
    DROP TABLE tf;
    DROP TABLE lake;
"
rm -rf "${LAKE}"
