#!/usr/bin/env bash
# A column matcher (`*`, `t.*`, `COLUMNS(...)`) in a row policy, `additional_table_filters` or
# `additional_result_filter` is rejected with a clear diagnostic: a filter is a predicate over the
# rows of one table, and a matcher there could only ever expand into the arguments of a function
# such as `ignore(*)`. The analyzer used to fail on it with an obscure `There are no table sources`.
# SQL UDFs are global, so the UDF name is made unique per test database.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT <<EOF
DROP TABLE IF EXISTS t_05227;
DROP TABLE IF EXISTS allowed_05227;
CREATE TABLE t_05227 (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_05227 SELECT number, number FROM numbers(100);
CREATE TABLE allowed_05227 (a UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO allowed_05227 SELECT number FROM numbers(10);

SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(*)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(COLUMNS(\'.*\'))'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(t_05227.*)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ignore(* EXCEPT b)'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'arrayMap(x -> ignore(*), [1])[1] = 0'}; -- { serverError BAD_ARGUMENTS }
SELECT a FROM t_05227 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'not ignore(*)'; -- { serverError BAD_ARGUMENTS }

-- A matcher inside a subquery of the filter resolves against the subquery's own table and is fine.
SELECT 'subquery in a filter', count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'a IN (SELECT * FROM allowed_05227)'};

-- A matcher hidden in a SQL UDF body is rejected the same way.
DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_udf_matcher;
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_udf_matcher AS () -> ignore(*);
SELECT count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'not ${CLICKHOUSE_DATABASE}_udf_matcher()'}; -- { serverError BAD_ARGUMENTS }

-- A row policy with a matcher is rejected when it is created or altered, not only on the next read.
CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING not ignore(*) TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING not ${CLICKHOUSE_DATABASE}_udf_matcher() TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING a < 50 TO ALL;
ALTER ROW POLICY p_05227 ON t_05227 USING not ignore(COLUMNS('.*')); -- { serverError BAD_ARGUMENTS }
SELECT 'row policy kept after a rejected ALTER', count() FROM t_05227;
DROP FUNCTION ${CLICKHOUSE_DATABASE}_udf_matcher;

CREATE ROW POLICY OR REPLACE p_05227 ON t_05227 USING a IN (SELECT * FROM allowed_05227) TO ALL;
SELECT 'subquery in a row policy', count() FROM t_05227;
DROP ROW POLICY p_05227 ON t_05227;

-- A qualified matcher over a Tuple column expands into its elements, not into table columns, and keeps working.
DROP TABLE IF EXISTS tup_05227;
CREATE TABLE tup_05227 (id UInt32, tup Tuple(x UInt8, y UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO tup_05227 VALUES (1, (1, 2)), (2, (0, 0)), (3, (0, 3));
SELECT 'tuple matcher in a filter', groupArray(id) FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'greatest(tup.*) > 0'};
SELECT count() FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'not ignore(tup_05227.*)'}; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING greatest(tup.*) > 0 TO ALL;
SELECT 'tuple matcher in a row policy', groupArray(id) FROM tup_05227;
-- The qualifier of a Tuple column can itself be qualified with the table.
SELECT 'table-qualified tuple matcher in a filter', groupArray(id) FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'greatest(tup_05227.tup.*) > 0'};
SELECT 'database-qualified tuple matcher in a filter', groupArray(id) FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'greatest(${CLICKHOUSE_DATABASE}.tup_05227.tup.*) > 0'};
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING greatest(tup_05227.tup.*) > 0 TO ALL;
SELECT 'table-qualified tuple matcher in a row policy', groupArray(id) FROM tup_05227;
-- A Tuple matcher over a parameter of a SQL UDF expands into the elements of the argument once the UDF is inlined.
DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_udf_tuple;
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_udf_tuple AS (x) -> greatest(x.*);
SELECT 'tuple matcher over a UDF parameter in a filter', groupArray(id) FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': '${CLICKHOUSE_DATABASE}_udf_tuple(tup) > 0'};
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING ${CLICKHOUSE_DATABASE}_udf_tuple(tup) > 0 TO ALL;
SELECT 'tuple matcher over a UDF parameter in a row policy', groupArray(id) FROM tup_05227;
-- The same UDF called with the table expands into the table columns, so it is rejected.
SELECT count() FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': '${CLICKHOUSE_DATABASE}_udf_tuple(tup_05227) > 0'}; -- { serverError BAD_ARGUMENTS }
SELECT count() FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': '${CLICKHOUSE_DATABASE}_udf_tuple(tup) > 0 AND ${CLICKHOUSE_DATABASE}_udf_tuple(tup_05227) > 0'}; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING ${CLICKHOUSE_DATABASE}_udf_tuple(tup_05227) > 0 TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING greatest(tup.*) > 0 TO ALL;
DROP FUNCTION ${CLICKHOUSE_DATABASE}_udf_tuple;
-- When the table exists, a qualified matcher is checked against its columns when the policy is created or altered.
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING not ignore(tup_05227.*) TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING not ignore(no_such_column.*) TO ALL; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING not ignore(id.*) TO ALL; -- { serverError BAD_ARGUMENTS }
ALTER ROW POLICY p_05227 ON tup_05227 USING not ignore(tup_05227.*); -- { serverError BAD_ARGUMENTS }
SELECT 'tuple row policy kept after a rejected ALTER', groupArray(id) FROM tup_05227;
DROP ROW POLICY p_05227 ON tup_05227;

-- A policy on a table that does not exist yet is checked on the read.
DROP TABLE IF EXISTS late_05227;
CREATE ROW POLICY OR REPLACE p_05227 ON late_05227 USING not ignore(late_05227.*) TO ALL;
CREATE TABLE late_05227 (id UInt32) ENGINE = MergeTree ORDER BY id;
SELECT count() FROM late_05227; -- { serverError BAD_ARGUMENTS }
-- Also when the policy is applied to a source table of Merge, or of a view.
CREATE VIEW late_view_05227 AS SELECT * FROM late_05227;
SELECT count() FROM merge(currentDatabase(), '^late_05227\$'); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM late_view_05227; -- { serverError BAD_ARGUMENTS }
DROP VIEW late_view_05227;
DROP ROW POLICY p_05227 ON late_05227;
DROP TABLE late_05227;

-- With analyzer_compatibility_prefer_alias_over_subcolumn, a qualifier that names both the table and a Tuple column
-- expands into the table columns, so it is rejected.
DROP TABLE IF EXISTS same_05227;
CREATE TABLE same_05227 (id UInt32, same_05227 Tuple(x UInt8, y UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO same_05227 VALUES (1, (1, 2)), (2, (0, 0)), (3, (0, 3));
SELECT 'tuple named as the table', groupArray(id) FROM same_05227 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0, additional_table_filters = {'same_05227': 'greatest(same_05227.*) > 0'};
SELECT count() FROM same_05227 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1, additional_table_filters = {'same_05227': 'greatest(same_05227.*) > 0'}; -- { serverError BAD_ARGUMENTS }
DROP TABLE same_05227;

-- With the setting, a multi-part qualifier whose first part is the table name is looked up without that part first,
-- like the analyzer does: here a is a plain column, so the matcher is rejected, without it nest_05227.a is a Tuple.
DROP TABLE IF EXISTS nest_05227;
CREATE TABLE nest_05227 (id UInt32, a UInt8, nest_05227 Tuple(a Tuple(x UInt8, y UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO nest_05227 VALUES (1, 0, ((1, 2))), (2, 0, ((0, 0))), (3, 0, ((0, 3)));
SELECT 'nested tuple named as the table', groupArray(id) FROM nest_05227 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0, additional_table_filters = {'nest_05227': 'greatest(nest_05227.a.*) > 0'};
SELECT count() FROM nest_05227 SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1, additional_table_filters = {'nest_05227': 'greatest(nest_05227.a.*) > 0'}; -- { serverError BAD_ARGUMENTS }
DROP TABLE nest_05227;

-- A quoted table name with dots is a single-part qualifier, it is looked up without that part first as well.
DROP TABLE IF EXISTS \`d.nest_05227\`;
CREATE TABLE \`d.nest_05227\` (id UInt32, a UInt8, \`d.nest_05227\` Tuple(a Tuple(x UInt8, y UInt8))) ENGINE = MergeTree ORDER BY id;
INSERT INTO \`d.nest_05227\` VALUES (1, 0, ((1, 2))), (2, 0, ((0, 0))), (3, 0, ((0, 3)));
SELECT 'nested tuple named as a dotted table', groupArray(id) FROM \`d.nest_05227\` SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 0, additional_table_filters = {'d.nest_05227': 'greatest(\`d.nest_05227\`.a.*) > 0'};
SELECT count() FROM \`d.nest_05227\` SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1, additional_table_filters = {'d.nest_05227': 'greatest(\`d.nest_05227\`.a.*) > 0'}; -- { serverError BAD_ARGUMENTS }
DROP TABLE \`d.nest_05227\`;

-- When the table has the name of its database, a qualifier is looked up without the table name first, and only then
-- without the database and the table, like the analyzer does: here db.tup is not a Tuple, so db.db.tup.* is rejected.
CREATE TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE} (id UInt32, tup Tuple(x UInt8, y UInt8), ${CLICKHOUSE_DATABASE} Tuple(tup UInt8)) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE} VALUES (1, (1, 2), (0)), (2, (0, 0), (0)), (3, (0, 3), (0));
SELECT count() FROM ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE} SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE}': 'greatest(${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE}.tup.*) > 0'}; -- { serverError BAD_ARGUMENTS }
CREATE ROW POLICY OR REPLACE p_05227 ON ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE} USING greatest(${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE}.tup.*) > 0 TO ALL; -- { serverError BAD_ARGUMENTS }
DROP TABLE ${CLICKHOUSE_DATABASE}.${CLICKHOUSE_DATABASE};

-- An unaliased parameterized view has a name of its own, a Tuple column can be qualified with it.
CREATE VIEW pv_05227 AS SELECT * FROM tup_05227 WHERE id > {min_id:UInt32};
SELECT 'view-qualified tuple matcher on a parameterized view', groupArray(id) FROM pv_05227(min_id = 0) SETTINGS additional_table_filters = {'pv_05227': 'greatest(pv_05227.tup.*) > 0'};
SELECT 'database-qualified tuple matcher on a parameterized view', groupArray(id) FROM pv_05227(min_id = 0) SETTINGS additional_table_filters = {'pv_05227': 'greatest(${CLICKHOUSE_DATABASE}.pv_05227.tup.*) > 0'};
SELECT count() FROM pv_05227(min_id = 0) SETTINGS additional_table_filters = {'pv_05227': 'not ignore(pv_05227.*)'}; -- { serverError BAD_ARGUMENTS }
DROP VIEW pv_05227;

-- The qualifier of a matcher inside a lambda can be a parameter of the lambda, which shadows the table.
SELECT 'tuple matcher on a lambda parameter', groupArray(id) FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'arrayExists(x -> greatest(x.*) > 0, [tup])'};
CREATE ROW POLICY OR REPLACE p_05227 ON tup_05227 USING arrayExists(x -> greatest(x.*) > 0, [tup]) TO ALL;
SELECT 'tuple matcher on a lambda parameter in a row policy', groupArray(id) FROM tup_05227;
DROP ROW POLICY p_05227 ON tup_05227;
SELECT count() FROM tup_05227 SETTINGS additional_table_filters = {'tup_05227': 'arrayExists(x -> x > 0, [id]) AND NOT ignore(x.*)'}; -- { serverError BAD_ARGUMENTS }
DROP TABLE tup_05227;

-- A matcher-free filter keeps working.
SELECT 'plain filter', count() FROM t_05227 WHERE 1 SETTINGS additional_table_filters = {'t_05227': 'b > 5'};
-- additional_result_filter is applied on top of the rows the LIMIT has already selected.
SELECT 'plain result filter';
SELECT a FROM t_05227 ORDER BY a LIMIT 3 SETTINGS additional_result_filter = 'a > 0';
-- A result column named dummy is not shadowed by the internal table of additional_result_filter.
SELECT 'tuple matcher in a result filter';
SELECT tuple(number, 0) AS dummy FROM numbers(3) SETTINGS additional_result_filter = 'greatest(dummy.*) > 0', analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT tuple(number, 0) AS _additional_result_filter_0 FROM numbers(3) SETTINGS additional_result_filter = 'greatest(_additional_result_filter_0.*) > 0', analyzer_compatibility_prefer_alias_over_subcolumn = 1;

DROP TABLE t_05227;
DROP TABLE allowed_05227;
EOF
