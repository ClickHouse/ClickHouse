#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: reads Parquet files from Azurite

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Lazy materialization rereads the surviving files of a plain object storage, so it is only applied
# when the reread is pinned to the generation the main pass read. Azure with
# `azure_validate_etag_on_read` pins every `GET` with `If-Match` on the listed `ETag`, like S3 with
# `s3_validate_etag_on_read` (see 04625_lazy_materialization_object_storage_unpinned), so the
# optimization must be applied with the setting on and must not be applied with it off.
# In both cases the results must be identical.

AZURE_CONN="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
# Azure container names are limited to 63 characters and must be lowercase alphanumeric, so hash
# the unique name instead of embedding it.
AZURE_CONT="cont$(echo "${CLICKHOUSE_TEST_UNIQUE_NAME}" | md5sum | cut -c1-24)"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data_1.parquet', 'Parquet')
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(0, 1000)
    SETTINGS azure_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;

    INSERT INTO FUNCTION azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data_2.parquet', 'Parquet')
    SELECT number AS k, concat('val_', toString(number)) AS s
    FROM numbers(1000, 1000)
    SETTINGS azure_truncate_on_insert = 1, output_format_parquet_row_group_size = 100;
"

TABLE_FN="azureBlobStorage('${AZURE_CONN}', '${AZURE_CONT}', 'data_{1,2}.parquet', 'Parquet')"

# `enable_analyzer` is pinned because lazy materialization requires the analyzer, and
# `query_plan_max_limit_for_lazy_materialization = 0` because the CI settings randomizer may set it to 1.
for etag in 1 0; do
    echo "-- azure_validate_etag_on_read = $etag"
    echo "-- number of lazy read steps in the plan (1 only when the reread is ETag-pinned)"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --azure_validate_etag_on_read="$etag" \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --query "SELECT countIf(explain LIKE '%LazilyReadFromObjectStorage%') FROM (EXPLAIN SELECT s FROM ${TABLE_FN} ORDER BY k LIMIT 3)"
    echo "-- results are the same regardless of the plan"
    ${CLICKHOUSE_CLIENT} \
        --enable_analyzer=1 \
        --azure_validate_etag_on_read="$etag" \
        --query_plan_optimize_lazy_materialization=1 \
        --query_plan_max_limit_for_lazy_materialization=0 \
        --query_plan_optimize_lazy_materialization_for_object_storage=1 \
        --query "SELECT k, s FROM ${TABLE_FN} ORDER BY k LIMIT 3"
done
