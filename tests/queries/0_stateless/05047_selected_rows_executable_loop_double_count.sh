#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `ShellCommandSource` and `LoopSource` read their rows through an inner pipeline and then report
# the same rows again from the outer source, so `SelectedRows` and `SelectedBytes` must not be
# accounted by the inner one. The eight cells below that take no input query read a known number of
# rows, so both counters have to equal `read_rows` and `read_bytes` of the same query. The two cells
# that do take an input query are asserted separately: input rows are read once but never reach
# `read_rows`, so that equality cannot describe them.
#
# `SelectedBytes` is asserted against `read_bytes` rather than against a computed constant: a
# wrapping `loop()` trims its last chunk to the LIMIT, and both figures then follow the trimmed
# chunk.
#
# The executable cells need `user_scripts_path`, which is a server config option, so they run in a
# single `clickhouse-local` process with its own config and its own `query_log`. `currentDatabase`
# scopes the server-global `query_log`, so concurrent copies of this test cannot read each other's
# rows.
#
# `ast_fuzzer_runs` is pinned because the stress profile enables the server-side AST fuzzer for any
# query, and a fuzzed re-execution inherits `log_comment` and would win the `argMax` below.

# `log_comment` is matched exactly rather than by prefix: the test runner injects a client-level
# comment that also starts with the test number. `expected_rows` pins each cell to the rows it had
# to read, so a cell cannot silently stop reading its source and still report `ok`.
oracle() {
    cat <<'SQL'
SELECT
    cell,
    if(read_rows = expected_rows AND selected_rows = read_rows AND selected_bytes = read_bytes,
        'ok',
        'fail: read_rows=' || toString(read_rows)
            || ' expected_rows=' || toString(expected_rows)
            || ' SelectedRows=' || toString(selected_rows)
            || ' read_bytes=' || toString(read_bytes)
            || ' SelectedBytes=' || toString(selected_bytes)) AS result
FROM
(
    SELECT log_comment AS cell,
        argMax(read_rows, event_time_microseconds) AS read_rows,
        argMax(read_bytes, event_time_microseconds) AS read_bytes,
        argMax(ProfileEvents['SelectedRows'], event_time_microseconds) AS selected_rows,
        argMax(ProfileEvents['SelectedBytes'], event_time_microseconds) AS selected_bytes,
        map('05047_loop_no_wrap', 1000, '05047_loop_wrap', 25, '05047_loop_tf', 10,
            '05047_loop_file', 25, '05047_mergetree', 1000, '05047_numbers', 1000,
            '05047_exec_tf', 1000, '05047_exec_pool', 1000)[cell] AS expected_rows
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND log_comment IN ('05047_loop_no_wrap', '05047_loop_wrap', '05047_loop_tf',
            '05047_loop_file', '05047_mergetree', '05047_numbers',
            '05047_exec_tf', '05047_exec_pool')
    GROUP BY cell
)
ORDER BY cell
SQL
}

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS t_loop;
DROP TABLE IF EXISTS t_small;

CREATE TABLE t_loop (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_loop SELECT number FROM numbers(1000);

CREATE TABLE t_small (x UInt64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_small SELECT number FROM numbers(10);

SELECT * FROM loop(currentDatabase(), t_loop) LIMIT 1000
SETTINGS log_queries = 1, log_comment = '05047_loop_no_wrap', ast_fuzzer_runs = 0
FORMAT Null;

-- 25 rows out of a 10-row table is arithmetic proof that the loop wrapped at least twice, so one
-- statement inside \`initLoop\` covers every iteration and not only the first.
SELECT * FROM loop(currentDatabase(), t_small) LIMIT 25
SETTINGS log_queries = 1, log_comment = '05047_loop_wrap', ast_fuzzer_runs = 0
FORMAT Null;

-- The inner-table-function branch of \`initLoop\`, which plans from an AST instead of a resolved
-- storage.
SELECT * FROM loop(numbers(3)) LIMIT 10
SETTINGS log_queries = 1, log_comment = '05047_loop_tf', ast_fuzzer_runs = 0
FORMAT Null;

-- \`StorageFileSource\` reports a source-side figure of its own, counted in bytes consumed off the
-- storage layer rather than in the size of the rows, and reads its format through a second inner
-- pipeline. Wrapping it makes both layers report once per iteration, and neither may be added to
-- what \`LoopSource\` emits.
INSERT INTO FUNCTION file('05047_${CLICKHOUSE_DATABASE}.tsv', 'TSV', 'x UInt64')
    SELECT number FROM numbers(10) SETTINGS engine_file_truncate_on_insert = 1;

SELECT * FROM loop(file('05047_${CLICKHOUSE_DATABASE}.tsv', 'TSV', 'x UInt64')) LIMIT 25
SETTINGS log_queries = 1, log_comment = '05047_loop_file', ast_fuzzer_runs = 0
FORMAT Null;

-- Controls: neither has an inner pipeline on its read path, so a fix that deflated every read path
-- would still have to leave them at 1x.
SELECT * FROM t_loop
SETTINGS log_queries = 1, log_comment = '05047_mergetree', ast_fuzzer_runs = 0
FORMAT Null;

SELECT number FROM numbers(1000)
SETTINGS log_queries = 1, log_comment = '05047_numbers', ast_fuzzer_runs = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;
"

$CLICKHOUSE_CLIENT -q "$(oracle)"

WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05047_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT
mkdir -p "${WORK_DIR}/user_scripts" "${WORK_DIR}/data"

cat > "${WORK_DIR}/user_scripts/gen1000.sh" <<'SCRIPT'
#!/usr/bin/env bash
seq 0 999
SCRIPT

# The pool branch reads the row count the child declares on its stdout before parsing any rows.
cat > "${WORK_DIR}/user_scripts/gen1000_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
echo 1000
seq 0 999
SCRIPT

# Echoes every input row back, one per line, for the cell that has no chunk header.
cat > "${WORK_DIR}/user_scripts/echo_rows.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r x; do
    printf '%s\n' "${x}"
done
SCRIPT

# Echoes the chunk size back and then the rows of that chunk, so the row count the pool reads from
# the child equals the number of rows the input query produced.
cat > "${WORK_DIR}/user_scripts/echo_pool.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r n; do
    printf '%s\n' "${n}"
    for ((i = 0; i < n; i++)); do
        IFS=$'\t' read -r x
        printf '%s\n' "${x}"
    done
done
SCRIPT

chmod +x "${WORK_DIR}/user_scripts/gen1000.sh" "${WORK_DIR}/user_scripts/gen1000_pool.sh" \
    "${WORK_DIR}/user_scripts/echo_rows.sh" "${WORK_DIR}/user_scripts/echo_pool.sh"

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
    <query_log><database>system</database><table>query_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds></query_log>
</clickhouse>
EOF

# The two cells this block contributes to the shared equality take no input query, so nothing but
# the read path itself can move their counters.
#
# The two input-query cells run the same query over the same table and must report the same pair,
# whichever engine reads it: the scan is billed once and the child's output once. \`Memory\` has no
# index, so the filter cannot be pushed into the source and the scan is exactly 100 rows; MergeTree
# would make it depend on randomized granule settings. The non-pool cell must not set
# \`send_chunk_header\`: with a header the child would echo the count back as a data row.
$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" --query "
SELECT * FROM executable('gen1000.sh', 'TSV', 'x UInt64')
SETTINGS log_queries = 1, log_comment = '05047_exec_tf', ast_fuzzer_runs = 0
FORMAT Null;

CREATE TABLE t_pool (x UInt64) ENGINE = ExecutablePool('gen1000_pool.sh', 'TSV') SETTINGS pool_size = 1;
SELECT * FROM t_pool
SETTINGS log_queries = 1, log_comment = '05047_exec_pool', ast_fuzzer_runs = 0
FORMAT Null;

CREATE TABLE t_pool_src (x UInt64) ENGINE = Memory;
INSERT INTO t_pool_src SELECT number FROM numbers(100);

CREATE TABLE t_exec_in (x UInt64)
ENGINE = Executable('echo_rows.sh', 'TSV', (SELECT x FROM t_pool_src WHERE x < 10));

SELECT * FROM t_exec_in
SETTINGS log_queries = 1, log_comment = '05047_exec_input', ast_fuzzer_runs = 0
FORMAT Null;

CREATE TABLE t_pool_in (x UInt64)
ENGINE = ExecutablePool('echo_pool.sh', 'TSV', (SELECT x FROM t_pool_src WHERE x < 10))
SETTINGS send_chunk_header = 1, pool_size = 1;

SELECT * FROM t_pool_in
SETTINGS log_queries = 1, log_comment = '05047_exec_pool_input', ast_fuzzer_runs = 0
FORMAT Null;

SYSTEM FLUSH LOGS query_log;
$(oracle);

-- Rows only: the input side follows randomized block sizes, so no byte constant is asserted.
-- \`read_rows\` is pinned first because it is also the arming term - the child echoes the input
-- values, so an input query that never reached it emits nothing and \`read_rows\` is 0. With the
-- output pinned at 10, \`SelectedRows\` can only reach 110 as 100 scanned plus 10 emitted.
SELECT cell,
    if(read_rows = 10 AND selected_rows = 110, 'ok',
        'fail: read_rows=' || toString(read_rows) || ' SelectedRows=' || toString(selected_rows)) AS result
FROM
(
    SELECT log_comment AS cell,
        argMax(read_rows, event_time_microseconds) AS read_rows,
        argMax(ProfileEvents['SelectedRows'], event_time_microseconds) AS selected_rows
    FROM system.query_log
    WHERE type = 'QueryFinish' AND event_date >= yesterday() AND event_time >= now() - 600
        AND current_database = currentDatabase()
        AND log_comment IN ('05047_exec_input', '05047_exec_pool_input')
    GROUP BY cell
)
ORDER BY cell
"
