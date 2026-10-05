#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `http_max_multipart_form_data_size` cannot be set through URL parameters, so it comes from a user.
user="u_${CLICKHOUSE_DATABASE}"
${CLICKHOUSE_CLIENT} -q "CREATE USER $user IDENTIFIED WITH plaintext_password BY 'pw' SETTINGS http_max_multipart_form_data_size = 100"
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TEMPORARY TABLE, SELECT ON *.* TO $user"

url="${CLICKHOUSE_URL}&user=${user}&password=pw"
part="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_part.tsv"
python3 -c 'import sys; sys.stdout.write("x\n" * 30)' > "$part"

# Two parts of 60 bytes: each one fits the 100 byte limit on its own, the form as a whole does not.
${CLICKHOUSE_CURL} -sS -F "a=@$part" -F "b=@$part" \
    "${url}&query=SELECT+count()+FROM+a&a_structure=s+String&b_structure=s+String&a_format=TSV&b_format=TSV" \
    | grep -o -m1 'the maximum size of multipart/form-data'

# One part of the same size stays under the limit.
${CLICKHOUSE_CURL} -sS -F "a=@$part" \
    "${url}&query=SELECT+count()+FROM+a&a_structure=s+String&a_format=TSV"

# A part that ends exactly at the limit is not over it.
exact="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_exact.tsv"
python3 -c 'import sys; sys.stdout.write("x\n" * 50)' > "$exact"
${CLICKHOUSE_CURL} -sS -F "a=@$exact" \
    "${url}&query=SELECT+count()+FROM+a&a_structure=s+String&a_format=TSV"

${CLICKHOUSE_CLIENT} -q "DROP USER $user"
rm -f "$part" "$exact"
