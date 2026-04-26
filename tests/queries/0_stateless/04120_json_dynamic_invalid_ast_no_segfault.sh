#!/usr/bin/env bash
# Tags: no-fasttest
#
# The AST fuzzer can produce structurally invalid ASTs (e.g. by removing children
# from an `ASTExpressionList` inside an `equals` function used as a JSON / Dynamic
# type parameter). The data type creators must throw a structured exception
# instead of crashing on out-of-bounds vector access or null dereference.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR/../shell_config.sh"

OPTS=(
    --allow_experimental_json_type 1
    --enable_json_type 1
    --allow_experimental_dynamic_type 1
    --allow_experimental_nullable_tuple_type 1
)

# Sanity: well-formed JSON / Dynamic types still work.
$CLICKHOUSE_CLIENT "${OPTS[@]}" -m -q "
DROP TABLE IF EXISTS t_smoke;
CREATE TABLE t_smoke (
    a JSON,
    b JSON(max_dynamic_paths = 100),
    c JSON(max_dynamic_types = 50, max_dynamic_paths = 100),
    d JSON(SKIP foo, SKIP REGEXP 'bar.*'),
    e JSON(name String),
    f Dynamic,
    g Dynamic(max_types = 10),
    h Array(Tuple(JSON(max_dynamic_paths = 758)))
) ENGINE = Memory;
DROP TABLE t_smoke;
" || exit 1

# Server-side AST fuzzer: 200 mutations over a CREATE TABLE that contains JSON
# and Dynamic types. The fuzzer can drop the identifier or the literal from the
# `equals(name, value)` parameter expression, leaving an empty / single-element
# argument list. Without the size guards in `createObject` and the `Dynamic`
# creator, this segfaults inside `boost::container::vector::operator[]`.
FUZZER_OPTS=("${OPTS[@]}" --ast_fuzzer_runs 200 --ast_fuzzer_any_query 1 --send_logs_level=fatal)

$CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
    "EXPLAIN AST CREATE TABLE t_fuzz_a (c0 JSON(max_dynamic_paths = 758)) ENGINE = Memory" >/dev/null 2>&1

$CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
    "EXPLAIN AST CREATE TABLE t_fuzz_b (c0 JSON(max_dynamic_types = 100), c1 Dynamic(max_types = 10)) ENGINE = Memory" >/dev/null 2>&1

$CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
    "EXPLAIN AST CREATE TABLE t_fuzz_c (c0 Array(Tuple(JSON(max_dynamic_paths = 758), Nullable(Tuple(c1 JSON(max_dynamic_paths = 0), c2 Dynamic(max_types = 5)))))) ENGINE = Memory" >/dev/null 2>&1

# After fuzzing, the server must still be alive and respond to queries.
$CLICKHOUSE_CLIENT -q "SELECT 'ok'"
