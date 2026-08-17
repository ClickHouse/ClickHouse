#!/usr/bin/env bash
# Tags: no-old-analyzer
# no-old-analyzer: The plan assertions describe applyTrivialCountIfPossible; the old analyzer decides trivial count in TreeRewriter

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_04743;
CREATE TABLE t_04743 (A Array(UInt32), B Array(UInt32), n UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04743 VALUES ([1,2,3],[1,2],1), ([4,5],[],2), ([6],[7,8,9],3);

-- arrayJoin in the aggregate argument multiplies rows, so the stored row count (3) is not the answer
SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(unnest(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(A) + 1) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(B)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(arrayJoin(arrayJoin([A, B]))) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;

-- the same values with the optimization off
SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(unnest(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(A) + 1) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(B)) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;
SELECT count(arrayJoin(arrayJoin([A, B]))) FROM t_04743 SETTINGS optimize_trivial_count_query = 0;

-- aggregates without arrayJoin keep the optimization
SELECT count() FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(*) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(1) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count(n) FROM t_04743 SETTINGS optimize_trivial_count_query = 1;
SELECT count() FROM t_04743 ARRAY JOIN A SETTINGS optimize_trivial_count_query = 1;

-- plans: the optimization is refused for the arrayJoin argument and kept otherwise
SELECT count() > 0 FROM (EXPLAIN SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';
SELECT count() > 0 FROM (EXPLAIN SELECT count(arrayJoin(A)) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%ReadFromMergeTree%';
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';
SELECT count() > 0 FROM (EXPLAIN SELECT count(n) FROM t_04743 SETTINGS optimize_trivial_count_query = 1)
WHERE explain ILIKE '%Optimized trivial count%';

DROP TABLE t_04743;
"

# file() counts inside read() when the flag is set, and it reaches that path even though
# totalRows() is unknown, so it distinguishes the guard's position from a later one.
unique_name=${CLICKHOUSE_TEST_UNIQUE_NAME}
tmp_dir=${USER_FILES_PATH}/${unique_name}
mkdir -p "${tmp_dir}"
rm -rf "${tmp_dir:?}"/*

cat > "${tmp_dir}/arr.csv" <<'EOF'
"[1,2,3]"
"[4,5]"
"[6]"
EOF

chmod 777 "${tmp_dir}"
chmod 777 "${tmp_dir}/arr.csv"

${CLICKHOUSE_CLIENT} -q "
SELECT count(arrayJoin(A)) FROM file('${unique_name}/arr.csv', 'CSV', 'A Array(UInt32)')
SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1;
SELECT count(arrayJoin(A)) FROM file('${unique_name}/arr.csv', 'CSV', 'A Array(UInt32)')
SETTINGS optimize_trivial_count_query = 0, optimize_count_from_files = 1;
SELECT count() FROM file('${unique_name}/arr.csv', 'CSV', 'A Array(UInt32)')
SETTINGS optimize_trivial_count_query = 1, optimize_count_from_files = 1;
"

rm -rf "${tmp_dir:?}"

# The old analyzer decides trivial count in TreeRewriter. A column argument is already refused there
# because it makes the column required; a constant argument requires no column, so it needs the guard.
# Per-query SETTINGS enable_analyzer = 0 so these arms run in every CI configuration.
${CLICKHOUSE_CLIENT} -q "
DROP TABLE IF EXISTS t_04743_old;
DROP TABLE IF EXISTS p_04743_old;
CREATE TABLE t_04743_old (A Array(UInt32), n UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_04743_old VALUES ([1,2,3],1), ([4,5],2), ([6],3);
CREATE TABLE p_04743_old (n UInt32) ENGINE = MergeTree PARTITION BY n ORDER BY tuple();
INSERT INTO p_04743_old VALUES (1), (2), (3);

SELECT count(arrayJoin([10, 20])) FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
SELECT count(arrayJoin([10, 20])) FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 0;
-- normalize_function_names is randomized in CI; the alias reaches TreeRewriter as arrayJoin only when it is on
SELECT count(unnest([10, 20])) FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1, normalize_function_names = 1;
SELECT count(arrayJoin(A)) FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;

-- the partition-predicate path decides trivial count separately, so it needs the guard too
SELECT count(arrayJoin([n, n])) FROM p_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
SELECT count(arrayJoin([n, n])) FROM p_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 0;

-- aggregates without arrayJoin keep the optimization on both paths
SELECT count() FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
SELECT count(n) FROM t_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
SELECT count() FROM p_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;
SELECT count(n) FROM p_04743_old SETTINGS enable_analyzer = 0, optimize_trivial_count_query = 1;

DROP TABLE t_04743_old;
DROP TABLE p_04743_old;
"
