#!/usr/bin/env bash

# A scalar subquery of a materialized view is evaluated per inserted block when a filter of a table it reads has a subquery,
# which reads the inserted block. The database name is spelled out: `currentDatabase()` in the view query does not resolve
# to the test database during the push.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

# Every row is pushed to the views as its own block, so a scalar subquery result reused across blocks shows.
BLOCK_PER_ROW="max_block_size = 1, max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
    min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0"
UDF="udf_05388_${CLICKHOUSE_DATABASE}"
# Each copy of the scalar is one more chance for concurrently analyzed blocks to bind each other's value.
ARRAY_SCALARS=$(printf '(SELECT groupArray(x) FROM ppar), %.0s' $(seq 31))'(SELECT groupArray(x) FROM ppar)'

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS mv_policy_in, mv_policy_udf, mv_merge_policy, mv_nested_policy, mv_atf, mv_nested_settings, mv_merge_atf, mv_merge_array_policy, mv_merge_variant_policy, mv_parallel_array;
DROP TABLE IF EXISTS mv_alias_merge, mv_buffer_merge, mv_mv_merge, mv_tf_merge, mv_loop_merge_policy, mpol_mv;
DROP ROW POLICY IF EXISTS policy_05388 ON pin, pudf, pmerge, pml, pn1, pn2, parr, ppar, pvt, pvar;
DROP TABLE IF EXISTS mpol_alias, mpol_buffer, mpol_tf, mpol, mpl;
DROP TABLE IF EXISTS pin, pudf, pmerge, pn1, pn2, pns, patf, src_atf, pmatf, parr, pvt, pvar, src_par, ppar, dst_par;
DROP TABLE IF EXISTS mpol_other, pml, dst_eq;
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

-- A row policy or 'additional_table_filters' of a table read by a scalar subquery is analyzed in the same context.
CREATE TABLE pin ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pudf ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pmerge ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pn1 ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pn2 ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pns ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE parr ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE TABLE pml ENGINE = MergeTree ORDER BY x AS SELECT arrayJoin([1, 2, 3, 105]) AS x;
CREATE TABLE pvt (x UInt64, v Variant(String, UInt64), d Dynamic) ENGINE = MergeTree ORDER BY x
    AS SELECT number + 1, number + 1, number + 1 FROM numbers(3);
CREATE TABLE pvar ENGINE = MergeTree ORDER BY x AS SELECT number + 1 AS x FROM numbers(3);
CREATE FUNCTION ${UDF} AS (v) -> v IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src);
CREATE ROW POLICY policy_05388 ON pin, pmerge, pml, pn2, pvt USING x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src) TO ALL;
CREATE ROW POLICY policy_05388 ON pudf USING ${UDF}(x) TO ALL;
CREATE ROW POLICY policy_05388 ON pn1 USING x <= (SELECT max(x) FROM ${CLICKHOUSE_DATABASE}.pn2) TO ALL;
CREATE ROW POLICY policy_05388 ON parr USING has((SELECT groupArray(x) FROM ${CLICKHOUSE_DATABASE}.src), x) TO ALL;
CREATE ROW POLICY policy_05388 ON pvar USING x = variantElement((SELECT groupArray(v) FROM ${CLICKHOUSE_DATABASE}.pvt)[1], 'UInt64')
    AND dynamicType((SELECT groupArray(d) FROM ${CLICKHOUSE_DATABASE}.pvt)[1]) = 'UInt64'
    AND variantType((SELECT any(v) FROM ${CLICKHOUSE_DATABASE}.pvt)) = 'UInt64' TO ALL;

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
-- 'Variant' and 'Dynamic' scalars there keep their member types.
CREATE MATERIALIZED VIEW mv_merge_variant_policy TO dst AS
    SELECT 'merge variant policy' AS name, count() AS block_rows,
        (SELECT max(x) FROM merge(${CLICKHOUSE_DATABASE}, '^pvar\$')) AS value
    FROM src;

-- 'Alias', 'Buffer', a materialized view and a table function table forward the read to a 'Merge' table.
CREATE TABLE mpol (x UInt64) ENGINE = Merge(${CLICKHOUSE_DATABASE}, '^pmerge\$');
CREATE TABLE mpol_alias ENGINE = Alias(${CLICKHOUSE_DATABASE}, mpol);
CREATE TABLE mpol_buffer (x UInt64) ENGINE = Buffer(${CLICKHOUSE_DATABASE}, mpol, 1, 1000, 1000, 1000000000, 1000000000, 1000000000, 1000000000);
CREATE TABLE mpol_other (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mpol_mv TO mpol AS SELECT x FROM mpol_other;
CREATE TABLE mpol_tf AS merge(${CLICKHOUSE_DATABASE}, '^pmerge\$');
CREATE MATERIALIZED VIEW mv_alias_merge TO dst AS
    SELECT 'alias merge policy' AS name, count() AS block_rows, (SELECT max(x) FROM mpol_alias) AS value FROM src;
CREATE MATERIALIZED VIEW mv_buffer_merge TO dst AS
    SELECT 'buffer merge policy' AS name, count() AS block_rows, (SELECT max(x) FROM mpol_buffer) AS value FROM src;
CREATE MATERIALIZED VIEW mv_mv_merge TO dst AS
    SELECT 'materialized view merge policy' AS name, count() AS block_rows, (SELECT max(x) FROM mpol_mv) AS value FROM src;
CREATE MATERIALIZED VIEW mv_tf_merge TO dst AS
    SELECT 'table function merge policy' AS name, count() AS block_rows, (SELECT max(x) FROM mpol_tf) AS value FROM src;

-- Inside 'loop' the filter may read the source table instead of the inserted block, so the scalar is compared with the same
-- read joined per block; 105 keeps the table's answer deterministic.
CREATE TABLE mpl (x UInt64) ENGINE = Merge(${CLICKHOUSE_DATABASE}, '^pml\$');
CREATE TABLE dst_eq (name String, scalar_value UInt64, join_value UInt64) ENGINE = MergeTree ORDER BY name;
CREATE MATERIALIZED VIEW mv_loop_merge_policy TO dst_eq AS
    SELECT 'loop merge policy' AS name, (SELECT max(x) FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, mpl) LIMIT 4)) AS scalar_value,
        j.value AS join_value
    FROM src CROSS JOIN (SELECT max(x) AS value FROM (SELECT x FROM loop(${CLICKHOUSE_DATABASE}, mpl) LIMIT 4)) AS j;

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

-- An 'Array' scalar with the blocks pushed to the view concurrently.
CREATE TABLE src_par (x UInt64) ENGINE = Null;
CREATE TABLE ppar ENGINE = Memory AS SELECT number AS x FROM numbers(25);
CREATE ROW POLICY policy_05388 ON ppar USING x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_par) TO ALL;
CREATE TABLE dst_par (x UInt64, arrs Array(Array(UInt64))) ENGINE = Memory;
CREATE MATERIALIZED VIEW mv_parallel_array TO dst_par AS SELECT x, [${ARRAY_SCALARS}] AS arrs FROM src_par;

INSERT INTO src SELECT number + 1 FROM numbers(3) SETTINGS ${BLOCK_PER_ROW};
INSERT INTO src_atf SELECT number + 1 FROM numbers(3)
    SETTINGS ${BLOCK_PER_ROW},
        additional_table_filters = {'${CLICKHOUSE_DATABASE}.patf': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_atf)',
            '${CLICKHOUSE_DATABASE}.pmatf': 'x IN (SELECT x FROM ${CLICKHOUSE_DATABASE}.src_atf)'};
INSERT INTO src_par SELECT number FROM numbers_mt(25)
    SETTINGS max_block_size = 1, max_threads = 8, max_insert_threads = 8, parallel_view_processing = 1,
        optimize_trivial_insert_select = 0, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
        min_insert_block_size_rows_for_materialized_views = 0, min_insert_block_size_bytes_for_materialized_views = 0;

SELECT * FROM dst ORDER BY name, value;
SELECT name, scalar_value = join_value AS same, count() FROM dst_eq GROUP BY name, same ORDER BY name, same;
SELECT 'parallel array policy', count(), countIf(NOT arrayAll(a -> a = [x], arrs)) FROM dst_par;
SELECT 'merge variant policy without a view', count() FROM merge(${CLICKHOUSE_DATABASE}, '^pvar\$');

DROP TABLE mv_policy_in, mv_policy_udf, mv_merge_policy, mv_nested_policy, mv_atf, mv_nested_settings, mv_merge_atf, mv_merge_array_policy, mv_merge_variant_policy, mv_parallel_array;
DROP TABLE mv_alias_merge, mv_buffer_merge, mv_mv_merge, mv_tf_merge, mv_loop_merge_policy, mpol_mv;
DROP ROW POLICY policy_05388 ON pin, pudf, pmerge, pml, pn1, pn2, parr, ppar, pvt, pvar;
DROP TABLE mpol_alias, mpol_buffer, mpol_tf, mpol, mpl;
DROP TABLE pin, pudf, pmerge, pn1, pn2, pns, patf, src_atf, pmatf, parr, pvt, pvar, src_par, ppar, dst_par;
DROP TABLE mpol_other, pml, dst_eq;
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

CREATE TABLE db_05388.pexe ENGINE = MergeTree ORDER BY x AS SELECT arrayJoin([1, 2, 3, 105]) AS x;
CREATE ROW POLICY policy_05388 ON db_05388.pexe USING x IN (SELECT x FROM db_05388.src) TO ALL;
CREATE TABLE db_05388.mexe (x UInt64) ENGINE = Merge(db_05388, '^pexe\$');
CREATE TABLE db_05388.dst_eq (name String, scalar_value UInt64, join_value UInt64) ENGINE = MergeTree ORDER BY name;
-- The input query of 'executable' may read the source table instead of the inserted block, like 'loop' above.
CREATE MATERIALIZED VIEW db_05388.mv_executable_merge_policy TO db_05388.dst_eq AS
    SELECT 'executable merge policy' AS name,
        (SELECT max(x) FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05388.mexe))) AS scalar_value,
        j.value AS join_value
    FROM db_05388.src
    CROSS JOIN (SELECT max(x) AS value FROM executable('echo_rows.sh', 'TSV', 'x UInt64', (SELECT x FROM db_05388.mexe))) AS j;

INSERT INTO db_05388.src SELECT number + 1 FROM numbers(3) SETTINGS ${BLOCK_PER_ROW};

SELECT name, scalar_value = join_value AS same, count() FROM db_05388.dst_eq GROUP BY name, same ORDER BY name, same;
"
