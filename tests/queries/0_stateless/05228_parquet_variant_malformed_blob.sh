#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: needs the Parquet format, which is not built in fasttest.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Every row has a value header promising more payload than the blob holds. Each must be reported as
# malformed data rather than read out of bounds.
DATA_FILE=$CUR_DIR/data_parquet/05228_variant_malformed.parquet

for n in 1 2 3 4 5; do
    ${CLICKHOUSE_LOCAL} --query "SELECT v FROM file('${DATA_FILE}', Parquet) WHERE n = $n" 2>&1 \
        | grep -o "Malformed Parquet variant: [0-9]* bytes are needed at offset [0-9]*, but the blob is [0-9]* bytes"
done
