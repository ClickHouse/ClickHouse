#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A struct with no fields at all infers as `Tuple()`, and when the schema is taken from such a file
# while another file of the same glob carries a struct with fields, the reader has to realign the
# decoded struct to a requested tuple without a single element.

DIR="${CLICKHOUSE_TMP}/05055_arrow_ipc_empty_struct"
rm -rf "${DIR}"
mkdir -p "${DIR}"

${CLICKHOUSE_LOCAL} --query "SELECT tuple() AS t FROM numbers(2) INTO OUTFILE '${DIR}/a.arrow' FORMAT Arrow"
${CLICKHOUSE_LOCAL} --query "SELECT tuple(1 AS a) AS t FROM numbers(2) INTO OUTFILE '${DIR}/b.arrow' FORMAT Arrow"

${CLICKHOUSE_LOCAL} --query "SELECT * FROM file('${DIR}/*.arrow', Arrow) ORDER BY ALL"

rm -rf "${DIR}"
