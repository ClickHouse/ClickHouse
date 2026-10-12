#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Azurite endpoint at localhost:10000.

# The Azure leg of the write-side IO meter of https://github.com/ClickHouse/ClickHouse/issues/116565:
# `WriteBufferFromAzureBytes` must reach the client in the streamed ProfileEvents, which is what
# ClientBase::onProfileEvents sums into the live `<rate>/s IO` in the progress line.
# The S3 leg is covered by 05026_client_io_progress_line_write.expect.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CONNECTION_STRING="DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://localhost:10000/devstoreaccount1;"
CONTAINER="cont05026clientioazure"
BLOB="write_bytes_${CLICKHOUSE_DATABASE}.tsv"

# ~10 MB uploaded as 1 MB blocks, so the bytes are counted per staged block as the query runs
# (the write path a real Azure-backed insert takes), not once at finalize.
AZURE_BYTES=$($CLICKHOUSE_CLIENT --print-profile-events --profile-events-delay-ms=-1 --query "
INSERT INTO FUNCTION azureBlobStorage('${CONNECTION_STRING}', '${CONTAINER}', '${BLOB}', 'TSV', 'auto', 's String')
SELECT randomPrintableASCII(10000) FROM numbers(1000)
SETTINGS azure_truncate_on_insert = 1, azure_strict_upload_part_size = 1000000, azure_max_single_part_upload_size = 1000000
" 2>&1 | grep -o 'WriteBufferFromAzureBytes: [0-9]*' | tail -n 1 | cut -d' ' -f2)

# The whole ~10 MB body has to be visible to the client through the streamed ProfileEvents.
echo "WriteBufferFromAzureBytes streamed to the client: $(( ${AZURE_BYTES:-0} >= 10000000 ))"
