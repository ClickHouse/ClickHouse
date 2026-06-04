#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `**/` matches zero or more directory components, so `data/**/file.txt` must also match
# `data/file.txt` (zero directory levels), not only files nested one or more levels deep.

rm -rf "${USER_FILES_PATH:?}/04317"
mkdir -p "$USER_FILES_PATH/04317/sub/subsub"

echo -e '0\t0' > "$USER_FILES_PATH/04317/top.tsv"
echo -e '1\t1' > "$USER_FILES_PATH/04317/sub/mid.tsv"
echo -e '2\t2' > "$USER_FILES_PATH/04317/sub/subsub/deep.tsv"

# A directory whose name contains brace characters must still be traversed by `**/`.
mkdir -p "$USER_FILES_PATH/04317/{braced}"
echo -e '3\t3' > "$USER_FILES_PATH/04317/{braced}/braced.tsv"

echo "all levels (zero, one, two), should include the top-level file:"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('04317/**/*.tsv', 'TSV', 'a UInt8, b UInt8') ORDER BY a"

echo "explicit suffix, zero level must match top.tsv:"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('04317/**/top.tsv', 'TSV', 'a UInt8, b UInt8') ORDER BY a"

echo "directory name with braces is matched by **/:"
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('04317/**/braced.tsv', 'TSV', 'a UInt8, b UInt8') ORDER BY a"

rm -rf "${USER_FILES_PATH:?}/04317"
