#!/usr/bin/env bash

# Inside a materialized view query, the source table read through `merge`, `loop` or `executable` is the
# table, like through `remote` or `Distributed`, also in the IN and scalar subqueries of the `executable`
# input query and of the row policies it reads, directly or through `merge`; only the view query itself,
# with its own subqueries, reads the inserted block. The same holds when a refreshable view appends to the
# source table, while a refreshable view reading it sees the table.
# The values do not depend on whether the inserted rows are already in the table. The database name is spelled out:
# `currentDatabase()` in the view query does not resolve to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_merge_not_in;
DROP TABLE IF EXISTS mv_merge_not_in_table;
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS mv_buffer_not_in;
DROP TABLE IF EXISTS mv_buffer_not_in_table;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_target;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src_dup;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);
CREATE TABLE src_dup (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src_dup SELECT number + 100 FROM numbers(10);

CREATE TABLE dist_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist AS dist_target ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dist_target);
CREATE TABLE buf AS dist_target ENGINE = Buffer(${CLICKHOUSE_DATABASE}, dist, 1, 100000, 100000, 100000, 100000, 100000000, 100000000);
INSERT INTO buf SELECT number + 100 FROM numbers(10);

CREATE TABLE dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

-- 'dist' raises the merge read above FetchColumns, so 'src' is planned by a nested interpreter.
CREATE MATERIALIZED VIEW mv_merge TO dst AS
    SELECT 'merge' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x >= 100) AS value
    FROM src;

-- The NOT IN subquery is part of the view query, so it reads the inserted block even inside the 'merge' child,
-- also when 'src' is not its leftmost table. It maps the inserted rows onto three rows of the table: the block
-- keeps 7 rows, the table 0, and no predicate 10.
CREATE MATERIALIZED VIEW mv_merge_not_in TO dst AS
    SELECT 'merge_not_in' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$')
         WHERE x >= 100 AND x NOT IN (SELECT n.number + 100 * (s.x < 100) FROM numbers(200) AS n JOIN src AS s ON n.number = s.x)) AS value
    FROM src;

-- So does a table on the right of NOT IN. 'src_dup' gets three rows it already has: the block keeps 7 rows, the
-- table 0, and no predicate 10 or 13.
CREATE MATERIALIZED VIEW mv_merge_not_in_table TO dst AS
    SELECT 'merge_not_in_table' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src_dup|dist)\$') WHERE x >= 100 AND x NOT IN src_dup) AS value
    FROM src_dup;

-- So does NOT IN over a 'Buffer', whose blocks are planned again for 'dist'.
CREATE MATERIALIZED VIEW mv_buffer_not_in TO dst AS
    SELECT 'buffer_not_in' AS name, count() AS block_rows,
        (SELECT count() FROM buf WHERE x NOT IN (SELECT x + 100 * (x < 100) FROM src)) AS value
    FROM src;

CREATE MATERIALIZED VIEW mv_buffer_not_in_table TO dst AS
    SELECT 'buffer_not_in_table' AS name, count() AS block_rows,
        (SELECT count() FROM buf WHERE x NOT IN src_dup) AS value
    FROM src_dup;

CREATE MATERIALIZED VIEW mv_loop TO dst AS
    SELECT 'loop' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25)) AS value
    FROM src;

INSERT INTO src VALUES (1), (2), (3);
INSERT INTO src_dup VALUES (101), (102), (103);

SELECT * FROM dst ORDER BY name;

DROP TABLE mv_merge;
DROP TABLE mv_merge_not_in;
DROP TABLE mv_merge_not_in_table;
DROP TABLE mv_loop;
DROP TABLE mv_buffer_not_in;
DROP TABLE mv_buffer_not_in_table;
DROP TABLE dst;
DROP TABLE buf;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
DROP TABLE src_dup;
"

# The input query of `executable` needs `user_scripts_path`, which is a server config option.
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05389_XXXXXX")
trap 'rm -rf "${WORK_DIR}"' EXIT
mkdir -p "${WORK_DIR}/user_scripts" "${WORK_DIR}/data"

cat > "${WORK_DIR}/user_scripts/echo_rows.sh" <<'SCRIPT'
#!/usr/bin/env bash
while IFS= read -r x; do
    printf '%s\n' "${x}"
done
SCRIPT
chmod +x "${WORK_DIR}/user_scripts/echo_rows.sh"

cat > "${WORK_DIR}/config.xml" <<EOF
<clickhouse>
    <user_scripts_path>${WORK_DIR}/user_scripts/</user_scripts_path>
    <path>${WORK_DIR}/data/</path>
</clickhouse>
EOF

$CLICKHOUSE_LOCAL --config-file="${WORK_DIR}/config.xml" -m --query "
CREATE DATABASE db_05389;
CREATE TABLE db_05389.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05389.src SELECT number + 100 FROM numbers(10);
CREATE TABLE db_05389.dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

CREATE MATERIALIZED VIEW db_05389.mv_executable TO db_05389.dst AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05389.src WHERE x >= 100))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_executable_in TO db_05389.dst AS
    SELECT 'executable_in' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64',
            (SELECT x FROM db_05389.src WHERE x >= 100 AND x IN (SELECT x FROM db_05389.src WHERE x < 105)))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_executable_scalar TO db_05389.dst AS
    SELECT 'executable_scalar' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64',
            (SELECT (SELECT sum(x) FROM db_05389.src WHERE x >= 100) AS x))) AS value
    FROM db_05389.src;

-- Through the table the policies read 4 and 1047, through the inserted block 1049 and 2.
CREATE TABLE db_05389.aux_in (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05389.aux_in SELECT number + 100 FROM numbers(10);
INSERT INTO db_05389.aux_in VALUES (4);
CREATE ROW POLICY p_in ON db_05389.aux_in USING x NOT IN (SELECT x FROM db_05389.src) TO ALL;
CREATE TABLE db_05389.aux_scalar (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05389.aux_scalar SELECT number + 100 FROM numbers(10);
INSERT INTO db_05389.aux_scalar VALUES (2), (200);
CREATE ROW POLICY p_scalar ON db_05389.aux_scalar USING x <= (SELECT max(x) FROM db_05389.src) TO ALL;

CREATE MATERIALIZED VIEW db_05389.mv_executable_merge_policy_in TO db_05389.dst AS
    SELECT 'executable_merge_policy_in' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM merge(db_05389, '^aux_in\$')))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_executable_merge_policy_scalar TO db_05389.dst AS
    SELECT 'executable_merge_policy_scalar' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM merge(db_05389, '^aux_scalar\$')))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_executable_policy_in TO db_05389.dst AS
    SELECT 'executable_policy_in' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05389.aux_in))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_loop_policy_in TO db_05389.dst AS
    SELECT 'loop_policy_in' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(db_05389, aux_in) LIMIT 3)) AS value
    FROM db_05389.src;

INSERT INTO db_05389.src VALUES (1), (2), (3);

SELECT * FROM db_05389.dst ORDER BY name;

-- A refreshable view appending to the source table pushes its block to these views like an INSERT.
TRUNCATE TABLE db_05389.dst;
CREATE MATERIALIZED VIEW db_05389.rmv_append REFRESH EVERY 1 YEAR APPEND TO db_05389.src EMPTY AS
    SELECT number + 1 AS x FROM numbers(3);
SYSTEM REFRESH VIEW db_05389.rmv_append;
SYSTEM WAIT VIEW db_05389.rmv_append;
SELECT 'append_refresh', * FROM db_05389.dst ORDER BY name;

-- A refreshable view has no inserted block, so it reads the table through every engine.
TRUNCATE TABLE db_05389.dst;
CREATE MATERIALIZED VIEW db_05389.rmv_read REFRESH EVERY 1 YEAR APPEND TO db_05389.dst EMPTY AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05389.src WHERE x >= 100))) AS value
    FROM db_05389.src
    UNION ALL
    SELECT 'executable_in', count(), (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64',
        (SELECT x FROM db_05389.src WHERE x >= 100 AND x IN (SELECT x FROM db_05389.src WHERE x < 105))))
    FROM db_05389.src
    UNION ALL
    SELECT 'executable_scalar', count(), (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64',
        (SELECT (SELECT sum(x) FROM db_05389.src WHERE x >= 100) AS x)))
    FROM db_05389.src
    UNION ALL
    SELECT 'loop', count(), (SELECT max(x) FROM (SELECT x FROM loop(db_05389, src) WHERE x > 0 LIMIT 25)) FROM db_05389.src
    UNION ALL
    SELECT 'merge', count(), (SELECT sum(x) FROM merge(db_05389, '^src\$') WHERE x >= 100) FROM db_05389.src;
SYSTEM REFRESH VIEW db_05389.rmv_read;
SYSTEM WAIT VIEW db_05389.rmv_read;
SELECT 'refreshable', * FROM db_05389.dst ORDER BY name;
"
