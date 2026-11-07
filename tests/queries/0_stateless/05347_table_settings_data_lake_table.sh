#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the data lake engines are not built in fast test.
#
# A data lake table shows the settings `system.engine_settings` lists for its engine, without the deprecated catalog
# settings, which a table rejects. Listing it renders the default of `storage_catalog_type`, which needs a name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_PATH="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}/05347_table_settings_data_lake_table/"

function cleanup()
{
    rm -rf "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --distributed_ddl_output_mode none \
    --query "CREATE TABLE t (id Int64) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        (SELECT arraySort(groupArray(name)) FROM system.table_settings WHERE database = currentDatabase() AND table = 't')
        = (SELECT arraySort(groupArray(name)) FROM system.engine_settings WHERE engine_name = 'IcebergLocal'),
        (SELECT arraySort(groupArray(name)) FROM system.table_settings WHERE database = currentDatabase() AND table = 't' AND inherited)
        = (SELECT arraySort(groupArray(name)) FROM system.engine_settings WHERE engine_name = 'IcebergLocal' AND inherited),
        (SELECT count() FROM system.table_settings WHERE database = currentDatabase() AND table = 't'
            AND name IN ('storage_catalog_type', 'storage_catalog_url', 'storage_aws_access_key_id'))"
