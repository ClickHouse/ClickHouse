#!/usr/bin/env bash

# `EXPLAIN PIPELINE INSERT ... SELECT` builds its pipeline through `InterpreterInsertQuery`, which is a
# different call site than the one every other `EXPLAIN` uses, so the join columns of
# `system.query_log` are checked for it separately. The pipeline is described and thrown away, and the
# join of the explained query is reported all the same: these columns describe the pipeline that was
# built for the query, like the other `used_` columns describe what it instantiated while it was
# analyzed. This test lives outside 04891_query_log_join_columns.sql because `EXPLAIN PIPELINE` over an
# INSERT prints the pipeline as a graph whose node names are not stable, and neither `FORMAT Null` nor
# wrapping it in `SELECT ... FROM (EXPLAIN ...)` suppresses it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t1;
    DROP TABLE IF EXISTS t2;
    DROP TABLE IF EXISTS ins;

    CREATE TABLE t1 (a UInt64) ENGINE = Memory;
    CREATE TABLE t2 (a UInt64) ENGINE = Memory;
    CREATE TABLE ins (a UInt64) ENGINE = Memory;

    INSERT INTO t1 SELECT number FROM numbers(10);
    INSERT INTO t2 SELECT number FROM numbers(10);
"

${CLICKHOUSE_CLIENT} --query "
    EXPLAIN PIPELINE INSERT INTO ins SELECT t1.a FROM t1 JOIN t2 ON t1.a = t2.a
    SETTINGS log_queries = 1, log_comment = '${CLICKHOUSE_DATABASE}_explain_pipeline_insert', join_algorithm = 'hash'
" > /dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT used_number_of_joins, used_join_algorithms, used_join_kinds, used_join_strictness, spilled_to_disk
    FROM system.query_log
    WHERE current_database = currentDatabase()
      AND type = 'QueryFinish'
      AND event_date >= yesterday()
      AND log_comment = '${CLICKHOUSE_DATABASE}_explain_pipeline_insert'
"

# The explained INSERT is never executed.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ins"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE t1;
    DROP TABLE t2;
    DROP TABLE ins;
"
