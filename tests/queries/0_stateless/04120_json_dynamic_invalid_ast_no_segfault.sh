#!/usr/bin/env bash
# Tags: no-fasttest
#
# Regression test for a server segfault in `DataTypeObject::createObject` and
# `DataTypeDynamic::create` when the AST is structurally malformed.
#
# The server-side AST fuzzer (`ast_fuzzer_runs`) and its in-place mutators
# `fuzzColumnLikeExpressionList` and `fuzzExpressionList` can produce an
# `ASTExpressionList` of `JSON`/`Dynamic` arguments where:
#   * the `ASTObjectTypeArgument` child has been replaced with some other AST
#     node (e.g. a subquery, an arithmetic function, an identifier or literal
#     drawn from the fuzzer's `column_like` pool), or
#   * the inner `equals(name, value)` parameter expression has been left with
#     an empty or single-element argument list.
# Without the defensive casts and size guards in `createObject` and the
# `Dynamic` creator, accessing `object_type_argument->parameter` and
# `function->arguments->children[0]` / `[1]` segfaults inside
# `boost::intrusive_ptr::operator bool` or `boost::container::vector::operator[]`.

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

# Prime the fuzzer's `column_like` pool with a variety of AST nodes
# (literals, identifiers, functions, table column references). The fuzzer's
# `fuzzColumnLikeExpressionList` and `fuzzExpressionList` mutations only
# actually insert or wrap children when this pool is non-empty, so without
# priming the rare malformed-AST mutation almost never fires on a freshly
# started server.
$CLICKHOUSE_CLIENT "${OPTS[@]}" -m -q "
SELECT 1 + 1, 'string_literal', toUInt64(42), array(1, 2, 3), tuple(1, 'b'), if(1 = 1, 'yes', 'no'), now(), today();
DROP TABLE IF EXISTS t_prime;
CREATE TABLE t_prime (id UInt32, name String, value Array(Int32), data Tuple(a String, b UInt8)) ENGINE = Memory;
INSERT INTO t_prime VALUES (1, 'a', [1, 2, 3], ('x', 1)), (2, 'b', [4, 5], ('y', 2));
SELECT * FROM t_prime WHERE id = 1 AND name LIKE 'a%' ORDER BY id LIMIT 10;
DROP TABLE t_prime;
" > /dev/null 2>&1

# Server-side AST fuzzer over three `CREATE TABLE` shapes that contain `JSON`
# and `Dynamic` types. `ast_fuzzer_runs=1000` per query is enough to hit the
# rare child-replacement / child-drop mutations on the `equals(...)` parameter
# expression inside `ASTObjectTypeArgument` once the `column_like` pool is
# populated.
#
# `ast_fuzzer_runs` alone is not enough to make this consistently fail on the
# unfixed master in a single client invocation, because the bug requires the
# fuzzer to randomly pick one of several rare mutation branches. So the
# fuzzer queries are also run in an outer loop of 50 iterations. Empirically
# (10/10 trials, max 27 iterations to crash on unfixed master with priming +
# this fuzz budget) this is enough to consistently trip the bug.
#
# `send_logs_level=fatal` suppresses the noisy per-fuzzed-query warning logs.
FUZZER_OPTS=("${OPTS[@]}" --ast_fuzzer_runs 1000 --ast_fuzzer_any_query 1 --send_logs_level=fatal)

for _ in $(seq 1 50); do
    $CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
        "EXPLAIN AST CREATE TABLE t_fuzz_a (c0 JSON(max_dynamic_paths = 758)) ENGINE = Memory" >/dev/null 2>&1

    $CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
        "EXPLAIN AST CREATE TABLE t_fuzz_b (c0 JSON(max_dynamic_types = 100), c1 Dynamic(max_types = 10)) ENGINE = Memory" >/dev/null 2>&1

    $CLICKHOUSE_CLIENT "${FUZZER_OPTS[@]}" -q \
        "EXPLAIN AST CREATE TABLE t_fuzz_c (c0 Array(Tuple(JSON(max_dynamic_paths = 758), Nullable(Tuple(c1 JSON(max_dynamic_paths = 0), c2 Dynamic(max_types = 5)))))) ENGINE = Memory" >/dev/null 2>&1
done

# After fuzzing, the server must still be alive and respond to queries.
$CLICKHOUSE_CLIENT -q "SELECT 'ok'"
