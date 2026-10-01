#!/usr/bin/env bash
# Tags: shard, no-fasttest, no-parallel, no-msan
# `ReadFromRemote` pushes a predicate over a column of a distributed subquery into the `HAVING` of the
# query sent to the shard (`allow_push_predicate_ast_for_distributed_subqueries`). When that column is
# computed by a non-deterministic function, the pushed copy refers to the function once more, so the
# shard may filter on a different value than the one it returns. `hasNonRewritableFunction` must refuse
# the rewrite, and `ExpressionInfoVisitor` must resolve user-defined functions through their own
# factories to see that they are non-deterministic: a WASM UDF that is not declared `DETERMINISTIC`, or
# an `EXECUTABLE` UDF without `<deterministic>true</deterministic>` (`test_function` comes from
# `tests/config/test_function.xml`). A UDF declared `DETERMINISTIC` is still pushed down.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'SQL'
DROP FUNCTION IF EXISTS wasm_dist_pd_det;
DROP FUNCTION IF EXISTS wasm_dist_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_dist_pd_test';
SQL

${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_dist_pd_test', code FROM input('code String') FORMAT RawBlob" \
    < "${CUR_DIR}/wasm/identity_int.wasm"

${CLICKHOUSE_CLIENT} << 'SQL'
CREATE OR REPLACE FUNCTION wasm_dist_pd_nondet
    LANGUAGE WASM FROM 'identity_dist_pd_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

CREATE OR REPLACE FUNCTION wasm_dist_pd_det
    LANGUAGE WASM FROM 'identity_dist_pd_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

DROP TABLE IF EXISTS t_dist_pd;
CREATE TABLE t_dist_pd (k Int32) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_dist_pd SELECT number FROM numbers(10);
SQL

# The predicate is pushed as an AST only when the remote query is sent as text.
${CLICKHOUSE_CLIENT} --serialize_query_plan=0 --allow_push_predicate_ast_for_distributed_subqueries=1 << 'SQL'
SELECT count() FROM (SELECT wasm_dist_pd_nondet(k) AS v FROM remote('127.0.0.2', currentDatabase(), t_dist_pd)) WHERE v % 2 = 0
SETTINGS log_comment = '05260_wasm_nondeterministic';
SELECT count() FROM (SELECT wasm_dist_pd_det(k) AS v FROM remote('127.0.0.2', currentDatabase(), t_dist_pd)) WHERE v % 2 = 0
SETTINGS log_comment = '05260_wasm_deterministic';
SELECT count() FROM (SELECT test_function(toUInt64(k), toUInt64(k)) AS v FROM remote('127.0.0.2', currentDatabase(), t_dist_pd)) WHERE v % 4 = 0
SETTINGS log_comment = '05260_executable_nondeterministic';

SYSTEM FLUSH LOGS query_log;

-- Whether the shard received the predicate.
SELECT log_comment, countIf(query LIKE '%HAVING%') > 0
FROM system.query_log
-- The secondary queries do not carry `current_database`, so match them by the database they read.
WHERE has(databases, currentDatabase())
    AND event_date >= yesterday() AND event_time > now() - 600 AND type = 'QueryFinish' AND is_initial_query = 0
    AND log_comment IN ('05260_wasm_nondeterministic', '05260_wasm_deterministic', '05260_executable_nondeterministic')
    AND query LIKE '%t_dist_pd%'
GROUP BY log_comment ORDER BY log_comment;
SQL

${CLICKHOUSE_CLIENT} << 'SQL'
DROP TABLE t_dist_pd;
DROP FUNCTION wasm_dist_pd_det;
DROP FUNCTION wasm_dist_pd_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_dist_pd_test';
SQL
