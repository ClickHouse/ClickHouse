#!/usr/bin/env bash

# Inside a materialized view query, the source table read through `merge`, `loop` or `executable` is the
# table, like through `remote` or `Distributed`, also in the IN and scalar subqueries of the `executable`
# input query; only the view query itself, with its own subqueries, reads the inserted block. The values do
# not depend on whether the inserted rows are already in the table. The database name is spelled out:
# `currentDatabase()` in the view query does not resolve to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_merge_in;
DROP TABLE IF EXISTS mv_merge_not_in_table;
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dist;
DROP TABLE IF EXISTS dist_target;
DROP TABLE IF EXISTS src;

CREATE TABLE src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO src SELECT number + 100 FROM numbers(10);

CREATE TABLE dist_target (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist AS dist_target ENGINE = Distributed(test_shard_localhost, ${CLICKHOUSE_DATABASE}, dist_target);

CREATE TABLE dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

-- 'dist' raises the merge read above FetchColumns, so 'src' is planned by a nested interpreter.
CREATE MATERIALIZED VIEW mv_merge TO dst AS
    SELECT 'merge' AS name, count() AS block_rows,
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x >= 100) AS value
    FROM src;

-- The IN subquery is part of the view query, so it reads the inserted block even inside the 'merge' child,
-- also when 'src' is not its leftmost table.
CREATE MATERIALIZED VIEW mv_merge_in TO dst AS
    SELECT 'merge_in' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$')
         WHERE x >= 100 AND x IN (SELECT n.number FROM numbers(200) AS n JOIN src AS s ON n.number = s.x)) AS value
    FROM src;

-- So does a table on the right of NOT IN.
CREATE MATERIALIZED VIEW mv_merge_not_in_table TO dst AS
    SELECT 'merge_not_in_table' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x >= 100 AND x NOT IN src) AS value
    FROM src;

CREATE MATERIALIZED VIEW mv_loop TO dst AS
    SELECT 'loop' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25)) AS value
    FROM src;

INSERT INTO src VALUES (1), (2), (3);

SELECT * FROM dst ORDER BY name;

DROP TABLE mv_merge;
DROP TABLE mv_merge_in;
DROP TABLE mv_merge_not_in_table;
DROP TABLE mv_loop;
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
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
            (SELECT x FROM db_05389.src WHERE x >= 100 AND x IN (SELECT x FROM db_05389.src)))) AS value
    FROM db_05389.src;

CREATE MATERIALIZED VIEW db_05389.mv_executable_scalar TO db_05389.dst AS
    SELECT 'executable_scalar' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64',
            (SELECT (SELECT sum(x) FROM db_05389.src WHERE x >= 100) AS x))) AS value
    FROM db_05389.src;

INSERT INTO db_05389.src VALUES (1), (2), (3);

SELECT * FROM db_05389.dst ORDER BY name;
"
