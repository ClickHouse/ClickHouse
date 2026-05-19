#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test for an exception in `DataTypeObject::createObject` and
# `DataTypeDynamic::create` when the AST is structurally malformed. In
# release builds these accessed `object_type_argument->parameter` and
# `function->arguments->children[0]` / `[1]` without validating the AST
# shape. The server-side `ast_fuzzer_runs` mutator can produce malformed
# shapes by replacing or dropping a child of the `equals(name, value)`
# parameter.

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

# Prime the global `column_like` AST pool with a variety of literals,
# identifiers, functions and table column references. The fuzzer's
# `fuzzColumnLikeExpressionList` and `fuzzExpressionList` mutators only
# actually insert or wrap children when this pool is non-empty.
$CLICKHOUSE_CLIENT "${OPTS[@]}" -m -q "
SELECT 1 + 1, 'string_literal', toUInt64(42), array(1, 2, 3), tuple(1, 'b'), if(1 = 1, 'yes', 'no'), now(), today();
DROP TABLE IF EXISTS t_prime;
CREATE TABLE t_prime (id UInt32, name String, value Array(Int32), data Tuple(a String, b UInt8)) ENGINE = Memory;
INSERT INTO t_prime VALUES (1, 'a', [1, 2, 3], ('x', 1)), (2, 'b', [4, 5], ('y', 2));
SELECT * FROM t_prime WHERE id = 1 AND name LIKE 'a%' ORDER BY id LIMIT 10;
DROP TABLE t_prime;
" > /dev/null 2>&1

# Main fuzz pass: three `JSON`/`Dynamic`-bearing `CREATE TABLE` shapes
# (single argument, two arguments, deeply nested) sent together in one
# client invocation so the server-side `ast_fuzzer_runs=1000` budget is
# spent on each shape per outer iteration. The outer bash loop keeps
# growing the global server-side `column_like` pool between iterations,
# which is what makes the rare mutations that replace or drop children
# of the inner `equals(name, value)` parameter expression eventually
# fire on the unfixed binary. `send_logs_level=fatal` suppresses the
# noisy per-fuzzed-query warnings.
FUZZER_OPTS=("${OPTS[@]}" --ast_fuzzer_runs 1000 --ast_fuzzer_any_query 1 --send_logs_level=fatal)

for _ in $(seq 1 25); do
    $CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -m -q "
        EXPLAIN AST CREATE TABLE t_fuzz_a (c0 JSON(max_dynamic_paths = 758)) ENGINE = Memory;
        EXPLAIN AST CREATE TABLE t_fuzz_b (c0 JSON(max_dynamic_types = 100), c1 Dynamic(max_types = 10)) ENGINE = Memory;
        EXPLAIN AST CREATE TABLE t_fuzz_c (c0 Array(Tuple(JSON(max_dynamic_paths = 758), Nullable(Tuple(c1 JSON(max_dynamic_paths = 0), c2 Dynamic(max_types = 5)))))) ENGINE = Memory;
    " >/dev/null 2>&1
done

# After fuzzing, the server must still be alive and respond to queries.
$CLICKHOUSE_CLIENT -q "SELECT 'ok'"
