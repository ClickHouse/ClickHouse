#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "2020-02-01" | $CLICKHOUSE_LOCAL -q "desc table table" --input-format "CSV" --file=-
echo "2020+02" | $CLICKHOUSE_LOCAL -q "desc table table" --input-format "CSV" --file=-
echo "1 + 1" | $CLICKHOUSE_LOCAL -q "desc table table" --input-format "CSV" --file=-
echo "200-10" | $CLICKHOUSE_LOCAL -q "desc table table" --input-format "CSV" --file=-



