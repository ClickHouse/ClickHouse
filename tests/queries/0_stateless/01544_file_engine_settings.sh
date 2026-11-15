#!/usr/bin/env bash
set -eu

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

the_file="$CLICKHOUSE_TMP/01544-t.csv"
rm -f -- "$the_file"

# We are going to check that format settings work for File engine,
# by creating a table with a non-default delimiter, and reading from it.
${CLICKHOUSE_LOCAL} --query "
    create table t(a int, b int) engine File(CSV, '$the_file') settings format_csv_delimiter = '|';
    insert into t select 1 a, 1 b;
"

# See what's in the file
cat "$the_file"

${CLICKHOUSE_LOCAL} --query "
    create table t(a int, b int) engine File(CSV, '$the_file') settings format_csv_delimiter = '|';
    select * from t;
" 

# Also check that the File engine emplicitly created by clickhouse-local
# uses the modified settings.
${CLICKHOUSE_LOCAL} --structure="a int, b int" --input-format=CSV --format_csv_delimiter="|" --query="select * from table" <<<"1|2"
