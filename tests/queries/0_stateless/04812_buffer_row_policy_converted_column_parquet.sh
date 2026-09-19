#!/usr/bin/env bash
# Tags: no-replicated-database, no-fasttest

# A row policy on a Buffer column the destination declares differently used to abort the server
# (Logical error: 'Unexpected return type from materialize') once the implicit WHERE -> PREWHERE
# move fired, and to fail with TYPE_MISMATCH on a URL destination even without a filter.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user04812_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET send_logs_level = 'fatal';

DROP TABLE IF EXISTS ${db}.t04812_pq_dst;
DROP TABLE IF EXISTS ${db}.t04812_pq_buf;
DROP TABLE IF EXISTS ${db}.t04812_str_dst;
DROP TABLE IF EXISTS ${db}.t04812_str_buf;
DROP TABLE IF EXISTS ${db}.t04812_flt_dst;
DROP TABLE IF EXISTS ${db}.t04812_flt_buf;
DROP TABLE IF EXISTS ${db}.t04812_url_src;
DROP TABLE IF EXISTS ${db}.t04812_url_dst;
DROP TABLE IF EXISTS ${db}.t04812_url_buf;

-- Map over the equivalent Array(Tuple(...)): the divergence supportedPrewhereColumns admits.
CREATE TABLE ${db}.t04812_pq_dst (k UInt8, m Array(Tuple(String, UInt64))) ENGINE = File(Parquet);
INSERT INTO ${db}.t04812_pq_dst VALUES (1, [('a', 1), ('b', 2)]), (2, [('b', 2)]), (3, [('a', 1)]);
CREATE TABLE ${db}.t04812_pq_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(${db}, t04812_pq_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- A bare-column policy makes the predicate and the carried column the same node, and the
-- destination's String is not usable as a filter at all, so a converted predicate is rejected.
CREATE TABLE ${db}.t04812_str_dst (k UInt8, f String) ENGINE = File(Parquet);
INSERT INTO ${db}.t04812_str_dst VALUES (1, '1'), (2, '0'), (3, '1');
CREATE TABLE ${db}.t04812_str_buf (k UInt8, f UInt8)
    ENGINE = Buffer(${db}, t04812_str_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- Same shape where converting the predicate changes truthiness instead of failing:
-- 0.5 is true as Float64 and false once cast to the Buffer's UInt8.
CREATE TABLE ${db}.t04812_flt_dst (k UInt8, f Float64) ENGINE = File(Parquet);
INSERT INTO ${db}.t04812_flt_dst VALUES (1, 0.5), (2, 0.0), (3, 2.0);
CREATE TABLE ${db}.t04812_flt_buf (k UInt8, f UInt8)
    ENGINE = Buffer(${db}, t04812_flt_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

-- A URL destination reaches the same forwarded filter through a different reader.
CREATE TABLE ${db}.t04812_url_src (k UInt8, m Array(Tuple(String, UInt64))) ENGINE = MergeTree ORDER BY k;
INSERT INTO ${db}.t04812_url_src VALUES (1, [('a', 1), ('b', 2)]), (2, [('b', 2)]), (3, [('a', 1)]);
CREATE TABLE ${db}.t04812_url_dst (k UInt8, m Array(Tuple(String, UInt64)))
    ENGINE = URL('http://127.0.0.1:${CLICKHOUSE_PORT_HTTP}/?query=SELECT+*+FROM+${db}.t04812_url_src+ORDER+BY+k+FORMAT+Parquet', 'Parquet');
CREATE TABLE ${db}.t04812_url_buf (k UInt8, m Map(String, UInt64))
    ENGINE = Buffer(${db}, t04812_url_dst, 1, 100, 100, 1000, 10000, 1000000, 10000000);

DROP USER IF EXISTS ${user};
CREATE USER ${user} IDENTIFIED WITH no_password;
GRANT SELECT ON ${db}.* TO ${user};

DROP ROW POLICY IF EXISTS rp04812_pq ON ${db}.t04812_pq_buf;
DROP ROW POLICY IF EXISTS rp04812_str ON ${db}.t04812_str_buf;
DROP ROW POLICY IF EXISTS rp04812_flt ON ${db}.t04812_flt_buf;
DROP ROW POLICY IF EXISTS rp04812_url ON ${db}.t04812_url_buf;
CREATE ROW POLICY rp04812_pq ON ${db}.t04812_pq_buf FOR SELECT USING mapContains(m, 'a') TO ${user};
CREATE ROW POLICY rp04812_str ON ${db}.t04812_str_buf FOR SELECT USING f TO ${user};
CREATE ROW POLICY rp04812_flt ON ${db}.t04812_flt_buf FOR SELECT USING f TO ${user};
CREATE ROW POLICY rp04812_url ON ${db}.t04812_url_buf FOR SELECT USING mapContains(m, 'a') TO ${user};
EOF

run() { ${CLICKHOUSE_CLIENT} --user "${user}" --query "SET send_logs_level = 'fatal'; $1"; }

# The row policy has to reach the destination read for this table's converting prefix to run at
# all. If a later change stops forwarding it, every arm below still returns these rows through a
# filter above the read, so assert the plan too. The marker comes from query_info.row_level_filter,
# which both planners populate; pretty = 0 makes it unconditional.
pushed_down() {
    if ${CLICKHOUSE_CLIENT} --user "${user}" --query \
        "SET send_logs_level = 'fatal'; SET enable_analyzer = $1; EXPLAIN actions = 1, pretty = 0 $2" \
        2>/dev/null | grep -q 'Row level filter column:'
    then echo 1; else echo 0; fi
}

# The implicit move is randomized away in about one run in ten, and either setting alone disables
# it, so the arms that need it pin both. Without that they read like the control below them.
moved="SETTINGS optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1"

# The mistyped column only has to be projected: it need not appear in the filter.
echo "--- B policy on the converted column, WHERE moved to PREWHERE ---"
run "SELECT m FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- B the policy reached the destination read, analyzer and legacy ---"
pushed_down 1 "SELECT m FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k ${moved}"
pushed_down 0 "SELECT m FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- L the converted column beside another ---"
run "SELECT k, m FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- I the filter names the converted column too ---"
run "SELECT m FROM ${db}.t04812_pq_buf WHERE mapContains(m, 'b') ORDER BY k ${moved}"

echo "--- D policy and explicit PREWHERE on the converted column ---"
run "SELECT m FROM ${db}.t04812_pq_buf PREWHERE mapContains(m, 'b') ORDER BY k"

# Controls: each holds without the fix, so a change here is a regression, not a repair.
echo "--- C the same query with the move disabled ---"
run "SELECT m FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k SETTINGS optimize_move_to_prewhere = 0, query_plan_optimize_prewhere = 0"

echo "--- E no filter at all ---"
run "SELECT m FROM ${db}.t04812_pq_buf ORDER BY k"

echo "--- F the converted column is not projected ---"
run "SELECT k FROM ${db}.t04812_pq_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- P the read is above FetchColumns ---"
run "SELECT k, count() FROM ${db}.t04812_pq_buf WHERE k > 0 GROUP BY k ORDER BY k ${moved}"

# The predicate is the carried column itself, so it must keep both types at once.
echo "--- ZS bare-column policy, the destination type is no filter ---"
run "SELECT f FROM ${db}.t04812_str_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- ZF bare-column policy, converting the predicate would change truthiness ---"
run "SELECT f FROM ${db}.t04812_flt_buf WHERE k > 0 ORDER BY k ${moved}"

echo "--- ZF the same rows with no filter ---"
run "SELECT f FROM ${db}.t04812_flt_buf ORDER BY k"

echo "--- U1 URL destination, policy and WHERE ---"
run "SELECT m FROM ${db}.t04812_url_buf WHERE k > 0 ORDER BY k"

echo "--- U2 URL destination, policy alone ---"
run "SELECT m FROM ${db}.t04812_url_buf ORDER BY k"

echo "--- U2 the policy reached the second reader too, analyzer and legacy ---"
pushed_down 1 "SELECT m FROM ${db}.t04812_url_buf ORDER BY k"
pushed_down 0 "SELECT m FROM ${db}.t04812_url_buf ORDER BY k"

${CLICKHOUSE_CLIENT} --multiquery <<EOF
SET send_logs_level = 'fatal';
DROP ROW POLICY rp04812_pq ON ${db}.t04812_pq_buf;
DROP ROW POLICY rp04812_str ON ${db}.t04812_str_buf;
DROP ROW POLICY rp04812_flt ON ${db}.t04812_flt_buf;
DROP ROW POLICY rp04812_url ON ${db}.t04812_url_buf;
DROP USER ${user};
DROP TABLE ${db}.t04812_pq_buf;
DROP TABLE ${db}.t04812_pq_dst;
DROP TABLE ${db}.t04812_str_buf;
DROP TABLE ${db}.t04812_str_dst;
DROP TABLE ${db}.t04812_flt_buf;
DROP TABLE ${db}.t04812_flt_dst;
DROP TABLE ${db}.t04812_url_buf;
DROP TABLE ${db}.t04812_url_dst;
DROP TABLE ${db}.t04812_url_src;
EOF
