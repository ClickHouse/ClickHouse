#!/usr/bin/env bash

# Inside a materialized view query, the source table read by the nested interpreter of `merge`, `loop`
# or `executable` is the inserted block, like the rest of the view query. The database name is spelled
# out: `currentDatabase()` in the view query does not resolve to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every row is pushed to the views as its own block, so a scalar subquery result reused across blocks shows.
BLOCK_PER_ROW="max_block_size = 1, max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
    min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0"
UDF="udf_05388_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv_merge;
DROP TABLE IF EXISTS mv_loop;
DROP TABLE IF EXISTS mv_row_policy;
DROP TABLE IF EXISTS mv_policy_in, mv_policy_udf, mv_merge_policy, mv_loop_policy, mv_nested_policy, mv_atf, mv_nested_settings, mv_merge_atf, mv_merge_array_policy;
DROP ROW POLICY IF EXISTS policy_05388 ON policy_child, pin, pudf, pmerge, ploop, pn1, pn2, parr;
DROP TABLE IF EXISTS policy_child, pin, pudf, pmerge, ploop, pn1, pn2, pns, patf, src_atf, pmatf, parr;
DROP FUNCTION IF EXISTS ${UDF};
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
        (SELECT sum(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(src|dist)\$') WHERE x > 0) AS value
    FROM src;

CREATE MATERIALIZED VIEW mv_loop TO dst AS
    SELECT 'loop' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) WHERE x > 0 LIMIT 25)) AS value
    FROM src;

-- The row policy of a 'Merge' child is analyzed as a standalone expression.
CREATE TABLE policy_child (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO policy_child VALUES (1), (2), (3);
CREATE ROW POLICY policy_05388 ON policy_child
    USING x <= (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, src) LIMIT 25)) TO ALL;
CREATE MATERIALIZED VIEW mv_row_policy TO dst AS
    SELECT 'row policy' AS name, count() AS block_rows, any(c.value) AS value
    FROM src CROSS JOIN (SELECT count() AS value FROM merge(${CLICKHOUSE_DATABASE}, '^policy_child\$')) AS c;

-- A row policy or 'additional_table_filters' of a table read by a scalar subquery is analyzed in the same context.
CREATE TABLE pin ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pudf ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pmerge ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE ploop ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pn1 ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pn2 ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pns ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE parr ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE FUNCTION ${UDF} AS (v) -> v IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src);
CREATE ROW POLICY policy_05388 ON pin, pmerge, ploop, pn2 USING x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src) TO ALL;
CREATE ROW POLICY policy_05388 ON pudf USING ${UDF}(x) TO ALL;
CREATE ROW POLICY policy_05388 ON pn1 USING x <= (SELECT max(x) FROM ${CLICKHOUSE_DATABASE}.pn2) TO ALL;
CREATE ROW POLICY policy_05388 ON parr USING has((SELECT groupArray(x) FROM ${CLICKHOUSE_DATABASE}.src), x) TO ALL;

CREATE MATERIALIZED VIEW mv_policy_in TO dst AS
    SELECT 'policy in' AS name, count() AS block_rows, (SELECT max(x) FROM pin) AS value FROM src;
CREATE MATERIALIZED VIEW mv_policy_udf TO dst AS
    SELECT 'policy udf' AS name, count() AS block_rows, (SELECT max(x) FROM pudf) AS value FROM src;
CREATE MATERIALIZED VIEW mv_merge_policy TO dst AS
    SELECT 'merge policy' AS name, count() AS block_rows,
        (SELECT max(x) FROM merge(${CLICKHOUSE_DATABASE}, '^pmerge\$')) AS value
    FROM src;
-- An 'Array' scalar in the standalone-analyzed policy of a 'Merge' child.
CREATE MATERIALIZED VIEW mv_merge_array_policy TO dst AS
    SELECT 'merge array policy' AS name, count() AS block_rows,
        (SELECT max(x) FROM merge(${CLICKHOUSE_DATABASE}, '^parr\$')) AS value
    FROM src;
CREATE MATERIALIZED VIEW mv_loop_policy TO dst AS
    SELECT 'loop policy' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, ploop) LIMIT 3)) AS value
    FROM src;
-- The row policy of the 'Merge' child 'pn1' is analyzed as a standalone expression.
CREATE MATERIALIZED VIEW mv_nested_policy TO dst AS
    SELECT 'nested policy' AS name, count() AS block_rows,
        (SELECT count() FROM merge(${CLICKHOUSE_DATABASE}, '^pn1\$')) AS value
    FROM src;
-- The filter is in the settings of a query nested in the scalar subquery.
CREATE MATERIALIZED VIEW mv_nested_settings TO dst AS
    SELECT 'nested settings' AS name, count() AS block_rows,
        (SELECT max(x) FROM (SELECT x FROM pns
            SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.pns': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src)'})) AS value
    FROM src;

CREATE TABLE src_atf (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE patf ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pmatf ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE MATERIALIZED VIEW mv_atf TO dst AS
    SELECT 'additional filters' AS name, count() AS block_rows, (SELECT max(x) FROM patf) AS value FROM src_atf;
-- 'dist' makes the 'Merge' read 'pmatf' through a nested interpreter, which applies its filter.
CREATE MATERIALIZED VIEW mv_merge_atf TO dst AS
    SELECT 'merge filters' AS name, count() AS block_rows,
        (SELECT max(x) FROM merge(${CLICKHOUSE_DATABASE}, '^(pmatf|dist)\$')) AS value
    FROM src_atf;

INSERT INTO src SELECT number + 1 FROM numbers(3) SETTINGS ${BLOCK_PER_ROW};
INSERT INTO src_atf SELECT number + 1 FROM numbers(3)
    SETTINGS ${BLOCK_PER_ROW},
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.patf': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_atf)',
            '${CLICKHOUSE_DATABASE}.pmatf': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_atf)'};

SELECT * FROM dst ORDER BY name, value;

DROP TABLE mv_merge;
DROP TABLE mv_loop;
DROP TABLE mv_row_policy;
DROP TABLE mv_policy_in, mv_policy_udf, mv_merge_policy, mv_loop_policy, mv_nested_policy, mv_atf, mv_nested_settings, mv_merge_atf, mv_merge_array_policy;
DROP ROW POLICY policy_05388 ON policy_child, pin, pudf, pmerge, ploop, pn1, pn2, parr;
DROP TABLE policy_child, pin, pudf, pmerge, ploop, pn1, pn2, pns, patf, src_atf, pmatf, parr;
DROP FUNCTION ${UDF};
DROP TABLE dst;
DROP TABLE dist;
DROP TABLE dist_target;
DROP TABLE src;
"

# The input query of `executable` needs `user_scripts_path`, which is a server config option.
WORK_DIR=$(mktemp -d "${CLICKHOUSE_TMP}/05388_XXXXXX")
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
CREATE DATABASE db_05388;
CREATE TABLE db_05388.src (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO db_05388.src SELECT number + 100 FROM numbers(10);
CREATE TABLE db_05388.dst (name String, block_rows UInt64, value UInt64) ENGINE = MergeTree ORDER BY name;

CREATE MATERIALIZED VIEW db_05388.mv_executable TO db_05388.dst AS
    SELECT 'executable' AS name, count() AS block_rows,
        (SELECT sum(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05388.src WHERE x > 0))) AS value
    FROM db_05388.src;

-- 'Alias', 'Buffer' and a materialized view forward the read to the 'Executable' table.
CREATE TABLE db_05388.e (x UInt64) ENGINE = Executable('echo_rows.sh', 'TSV', (SELECT x FROM db_05388.src WHERE x > 0));
CREATE TABLE db_05388.e_alias ENGINE = Alias(db_05388, e);
CREATE TABLE db_05388.e_buffer (x UInt64) ENGINE = Buffer(db_05388, e, 1, 1000, 1000, 1000000000, 1000000000, 1000000000, 1000000000);
CREATE TABLE db_05388.other (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW db_05388.e_mv TO db_05388.e AS SELECT x FROM db_05388.other;

CREATE MATERIALIZED VIEW db_05388.mv_alias TO db_05388.dst AS
    SELECT 'alias' AS name, count() AS block_rows, (SELECT sum(x) FROM db_05388.e_alias) AS value FROM db_05388.src;
CREATE MATERIALIZED VIEW db_05388.mv_buffer TO db_05388.dst AS
    SELECT 'buffer' AS name, count() AS block_rows, (SELECT sum(x) FROM db_05388.e_buffer) AS value FROM db_05388.src;
CREATE MATERIALIZED VIEW db_05388.mv_materialized_view TO db_05388.dst AS
    SELECT 'materialized view' AS name, count() AS block_rows, (SELECT sum(x) FROM db_05388.e_mv) AS value FROM db_05388.src;

INSERT INTO db_05388.src SELECT number + 1 FROM numbers(3) SETTINGS ${BLOCK_PER_ROW};

SELECT * FROM db_05388.dst ORDER BY name, value;
"
