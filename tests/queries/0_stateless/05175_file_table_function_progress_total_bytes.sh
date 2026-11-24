#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DATA_FILE="${CLICKHOUSE_DATABASE}_progress.tsv"
seq 1 3000000 > "${USER_FILES_PATH}/${DATA_FILE}"

# Reading from a file knows the total size upfront, so the progress bar must show the completion percentage.
${CLICKHOUSE_CLIENT} --progress=err --max_threads 1 --query \
    "SELECT sum(x) FROM file('${DATA_FILE}', 'TSV', 'x UInt32') FORMAT Null" 2>&1 \
    | tr '\r' '\n' | grep -c -o -m1 -E '[0-9]+%' | (grep -q -v '^0$' && echo "percentage shown" || echo "no percentage")

rm "${USER_FILES_PATH}/${DATA_FILE}"
