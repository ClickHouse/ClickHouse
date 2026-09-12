#!/usr/bin/env bash

# The `{a,b,c}` globs in a path are expanded into separate paths, and the expansion is a Cartesian
# product, so a short pattern can ask for astronomically many paths. It used to be unbounded: the
# expansion ate all the memory of the server and could not be interrupted.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Too many paths: 2^100 of them.
MANY_GROUPS=$(${CLICKHOUSE_CLIENT} -q "SELECT repeat('{a,b}', 100)")
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${MANY_GROUPS}')" 2>&1 | grep -q -F 'expand to more than' && echo 'OK' || echo 'FAIL'

# Too many globs: this expands to a single path, but only after looking at every one of them.
MANY_SINGLE_GROUPS=$(${CLICKHOUSE_CLIENT} -q "SELECT repeat('{ab}', 2000)")
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('${MANY_SINGLE_GROUPS}')" 2>&1 | grep -q -F "globs to expand" && echo 'OK' || echo 'FAIL'

# The way the AST fuzzer found it: the `file` function reads a file into a string, and the `file`
# table function takes that string as a path, so every `{...}` of a `JSONEachRow` file becomes a glob.
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_data.jsonl', JSONEachRow, 'x UInt32')
    SELECT number FROM numbers(2000) SETTINGS engine_file_truncate_on_insert = 1"
${CLICKHOUSE_CLIENT} -q "
    SELECT * FROM file(file('${CLICKHOUSE_DATABASE}_data.jsonl'), JSONEachRow, 'x UInt32')" 2>&1 | grep -q -F "globs to expand" && echo 'OK' || echo 'FAIL'

# A single group with an enormous number of alternatives - a million of them, which is the shape a
# whole file passed as a path takes. The limit has to fire while the group is being scanned: checked
# only after the group has been parsed, it would still let the parser keep one offset and one
# `string_view` per alternative, so its memory would grow with the size of the pattern even though
# nothing is ever expanded.
${CLICKHOUSE_CLIENT} -q "SELECT * FROM file('{' || repeat('a,', 1000000) || 'a}')" 2>&1 | grep -q -F 'expand to more than' && echo 'OK' || echo 'FAIL'

# The server is still fine after refusing these patterns.
${CLICKHOUSE_CLIENT} -q "SELECT 1"

# Patterns of a sane size are still expanded as before.
for i in 1 2 3
do
    ${CLICKHOUSE_CLIENT} -q "
        INSERT INTO FUNCTION file('${CLICKHOUSE_DATABASE}_${i}.jsonl', JSONEachRow, 'x UInt32')
        SELECT ${i} SETTINGS engine_file_truncate_on_insert = 1"
done
${CLICKHOUSE_CLIENT} -q "SELECT sum(x) FROM file('${CLICKHOUSE_DATABASE}_{1,2,3}.jsonl', JSONEachRow, 'x UInt32')"

rm "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}_data.jsonl" "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}_1.jsonl" "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}_2.jsonl" "${USER_FILES_PATH:?}/${CLICKHOUSE_DATABASE}_3.jsonl"
