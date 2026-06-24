#!/usr/bin/env bash
# Tags: no-fasttest

# Exercises the experimental AST-based glob parser (use_glob_ast_parser = 1) and
# asserts it produces exactly the same set of matched files as the legacy
# regex-based parser (use_glob_ast_parser = 0) for a range of glob constructs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}"
rm -rf "$DIR"
mkdir -p "$DIR/sub"

for name in 1 2 3 10 a b; do
    echo "$name" > "$DIR/data_$name.csv"
done
echo "nested" > "$DIR/sub/data_9.csv"
# A file whose name literally contains brackets: legacy escapes '[' and ']', so
# the new parser must also treat them literally (not as a character class).
echo "literal" > "$DIR/data_[ab].csv"

# For every pattern, the new parser (=1) must match the same files as the
# legacy parser (=0). We print "<pattern>: OK" on agreement, or a diff on mismatch.
run() {
    local pattern="$1"
    local query="SELECT _path FROM file('${CLICKHOUSE_DATABASE}/${pattern}', 'CSV', 'x String') ORDER BY _path"
    local legacy new
    legacy=$(${CLICKHOUSE_CLIENT} --use_glob_ast_parser 0 -q "$query" 2>&1)
    new=$(${CLICKHOUSE_CLIENT} --use_glob_ast_parser 1 -q "$query" 2>&1)
    if [ "$legacy" == "$new" ]; then
        echo "$pattern: OK"
    else
        echo "$pattern: MISMATCH"
        diff <(echo "$legacy") <(echo "$new")
    fi
}

run 'data_*.csv'
run 'data_?.csv'
run 'data_{1,3}.csv'
run 'data_{1,10,a}.csv'
run 'data_{1..3}.csv'
run 'data_{01..10}.csv'
run 'data_[ab].csv'
run '**/*.csv'

rm -rf "$DIR"
