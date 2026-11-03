#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The schema of an HTTP external table is bound by the first multipart part that names it. The `_structure` and
# `_types` fields are looked up by the part name each time, and plain form fields are added to the parameter set
# as they are read, so a later part with the same name can declare a different schema. Its data must be rejected,
# not written to the already-bound table, where the table header would label it with the first schema.

FIRST="${CLICKHOUSE_TMP}/05216_first.tsv"
SECOND="${CLICKHOUSE_TMP}/05216_second.tsv"
THIRD="${CLICKHOUSE_TMP}/05216_third.tsv"
printf '1\n' > "${FIRST}"
printf 'hello\n' > "${SECOND}"
printf '2\n' > "${THIRD}"

# The first part binds `_1 UInt64`; the second one declares `_1 String` for the same table.
${CLICKHOUSE_CURL} -sS -F 'ext_types=UInt64' -F "ext=@${FIRST}" -F 'ext_structure=_1 String' -F "ext=@${SECOND}" \
    "${CLICKHOUSE_URL}&query=SELECT+_1+FROM+ext" | grep -o -m1 'Code: 117'

# Two parts with the same name and the same schema are appended to one table.
${CLICKHOUSE_CURL} -sS -F 'ext_types=UInt64' -F "ext=@${FIRST}" -F "ext=@${THIRD}" \
    "${CLICKHOUSE_URL}&query=SELECT+sum(_1)+FROM+ext"

rm -f "${FIRST}" "${SECOND}" "${THIRD}"
