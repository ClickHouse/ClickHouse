#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="u_${CLICKHOUSE_DATABASE}"
user2="u2_${CLICKHOUSE_DATABASE}"
# The settings under which `count()` is answered from metadata (trivial count).
trivial="apply_patch_parts = 0, apply_mutations_on_fly = 0, optimize_trivial_count_query = 1, enable_parallel_replicas = 0, make_distributed_plan = 0, empty_result_for_aggregation_by_empty_set = 0"

${CLICKHOUSE_CLIENT} --multiline -q "
DROP USER IF EXISTS ${user};
DROP USER IF EXISTS ${user2};
CREATE TABLE t_visible (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_hidden (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_show (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_col (id UInt32, s String) ENGINE = MergeTree ORDER BY id SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_visible SELECT number, 'v' FROM numbers(3);
INSERT INTO t_hidden SELECT number, repeat('h', 100) FROM numbers(1000);
INSERT INTO t_show SELECT number, 's' FROM numbers(5);
INSERT INTO t_col SELECT number, 'c' FROM numbers(7);
CREATE TABLE m (id UInt32, s String) ENGINE = Merge(currentDatabase(), '^t_(visible|hidden)\$');
CREATE TABLE m_show (id UInt32, s String) ENGINE = Merge(currentDatabase(), '^t_(visible|show)\$');
CREATE TABLE m_col (id UInt32, s String) ENGINE = Merge(currentDatabase(), '^t_(visible|col)\$');
CREATE TABLE m_func AS merge(currentDatabase(), '^t_(visible|hidden)\$');
CREATE TABLE a_m ENGINE = Alias(currentDatabase(), m);
CREATE TABLE mm (id UInt32, s String) ENGINE = Merge(currentDatabase(), '^m\$');
CREATE TABLE a_hidden ENGINE = Alias(currentDatabase(), t_hidden);
CREATE TABLE m_alias (id UInt32, s String) ENGINE = Merge(currentDatabase(), '^(t_visible|a_hidden)\$');
CREATE USER ${user} IDENTIFIED WITH plaintext_password BY '${user}';
GRANT SELECT ON t_visible TO ${user};
GRANT SELECT ON m TO ${user};
GRANT SELECT ON m_show TO ${user};
GRANT SELECT ON m_col TO ${user};
GRANT SELECT ON m_func TO ${user};
GRANT SELECT ON a_m TO ${user};
GRANT SELECT ON mm TO ${user};
GRANT SHOW TABLES ON t_show TO ${user};
GRANT SELECT(id) ON t_col TO ${user};
GRANT SELECT ON a_hidden TO ${user};
GRANT SELECT ON m_alias TO ${user};
CREATE USER ${user2} IDENTIFIED WITH plaintext_password BY '${user2}';
GRANT SELECT ON ${CLICKHOUSE_DATABASE}.* TO ${user2};
REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.t_hidden FROM ${user2};
SELECT min(data_compressed_bytes) > 0 FROM system.columns WHERE database = currentDatabase() AND table = 't_visible';
"

# Rows and sizes of a matched table the user cannot see are not reported.
${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --multiline -q "
SELECT 'm', count() FROM m SETTINGS ${trivial};
SELECT 'merge()', count() FROM merge(currentDatabase(), '^t_(visible|hidden)\$') SETTINGS ${trivial};
SELECT 'm_func', count() FROM m_func;
SELECT 'a_m', count() FROM a_m SETTINGS ${trivial};
SELECT 'mm', count() FROM mm SETTINGS ${trivial};
"

for table in m m_func a_m mm
do
    ${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --multiline -q "
    SELECT '${table}', total_rows, total_bytes = (SELECT total_bytes FROM system.tables WHERE database = currentDatabase() AND name = 't_visible')
    FROM system.tables WHERE database = currentDatabase() AND name = '${table}';
    SELECT '${table}', c.name, c.data_compressed_bytes = v.data_compressed_bytes, c.data_uncompressed_bytes = v.data_uncompressed_bytes
    FROM system.columns AS c INNER JOIN (SELECT name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns WHERE database = currentDatabase() AND table = 't_visible') AS v USING (name)
    WHERE c.database = currentDatabase() AND c.table = '${table}' ORDER BY c.name;
    "
done

# The trivial count is still used for a partially granted user.
${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" -q "EXPLAIN SELECT count() FROM m SETTINGS ${trivial}" | grep -c 'Optimized trivial count'

# A visible table the user cannot read is not counted from metadata either.
${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --multiline -q "
SELECT count() FROM m_show SETTINGS ${trivial}; -- { serverError ACCESS_DENIED }
SELECT count() FROM m_show; -- { serverError ACCESS_DENIED }
SELECT 'm_show', total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'm_show';
SELECT 'm_col', count() FROM m_col SETTINGS ${trivial};
"

# A table-level revoke inside a database-wide grant hides that table.
${CLICKHOUSE_CLIENT} --user "${user2}" --password "${user2}" --multiline -q "
SELECT 'u2 t_hidden', count() FROM system.tables WHERE database = currentDatabase() AND name = 't_hidden';
SELECT 'u2 m', count() FROM m;
SELECT 'u2 m', count() FROM m SETTINGS ${trivial};
SELECT 'u2 m', total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'm';
"

# A visible Alias child does not report the sizes of its hidden target.
${CLICKHOUSE_CLIENT} --user "${user}" --password "${user}" --multiline -q "
SELECT 'm_alias', name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns
WHERE database = currentDatabase() AND table = 'm_alias' ORDER BY name;
"

${CLICKHOUSE_CLIENT} --multiline -q "
SELECT 'admin m', count() FROM m SETTINGS ${trivial};
SELECT 'admin m', total_rows FROM system.tables WHERE database = currentDatabase() AND name = 'm';
DROP USER IF EXISTS ${user};
DROP USER IF EXISTS ${user2};
"
