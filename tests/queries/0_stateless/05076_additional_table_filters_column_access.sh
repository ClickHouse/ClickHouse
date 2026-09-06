#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# additional_table_filters must be gated by column-level SELECT access on the columns it references.

DB=${CLICKHOUSE_DATABASE}
T='prod_customer_token_25_8_secret'

u_low="u_low_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_alias="u_alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_view="u_view_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_def="u_def_${CLICKHOUSE_TEST_UNIQUE_NAME}"
p_def="p_def_${CLICKHOUSE_TEST_UNIQUE_NAME}"

$CLICKHOUSE_CLIENT -n -q "
DROP USER IF EXISTS $u_low, $u_alias, $u_view, $u_def;
DROP SETTINGS PROFILE IF EXISTS $p_def;

CREATE TABLE t (id UInt8, public_label String, secret_token String, tup Tuple(a String), alias_col String ALIAS secret_token) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 'customer_1', 'prod_customer_token_25_8_secret', ('ta')), (2, 'customer_2', 'other_customer_private_token', ('tb'));
CREATE TABLE d (id UInt8, public_label String, secret_token String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d3 (id UInt8, public_label String, secret_token String) ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t);
CREATE VIEW v_inv SQL SECURITY INVOKER AS SELECT id, public_label FROM t;
-- A trivial view over the one-shard, three-replica Distributed table: the analyzer can inline the body and
-- read d3 directly (optimize_trivial_view_pushdown_to_distributed), so the custom key is shipped to the replicas.
CREATE VIEW v_inv_d3 SQL SECURITY INVOKER AS SELECT id, public_label FROM d3;
CREATE VIEW v_def SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT id, public_label FROM t;
-- The view column secret_token shadows the denied table column: a custom key over it resolves on the view,
-- and must not be re-evaluated over the table inside the DEFINER/NONE body.
CREATE VIEW v_def_shadow SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT id, public_label, '' AS secret_token FROM t;
CREATE VIEW v_none_shadow SQL SECURITY NONE AS SELECT id, public_label, '' AS secret_token FROM t;
-- The DEFINER body reads secret_token from t as the definer, so the view's own column-level grants are the only
-- gate for a filter keyed by the parameterized view.
CREATE VIEW pv_def SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT id, public_label, secret_token FROM t WHERE id >= {min_id:UInt8};

CREATE USER $u_low IDENTIFIED WITH plaintext_password BY 'password';
CREATE USER $u_alias IDENTIFIED WITH plaintext_password BY 'password';
CREATE USER $u_view IDENTIFIED WITH plaintext_password BY 'password';
-- The definer's profile activates custom-key parallel replicas, so the invoker only has to supply the key.
CREATE USER $u_def IDENTIFIED WITH plaintext_password BY 'password';
CREATE SETTINGS PROFILE $p_def SETTINGS cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling' TO $u_def;
GRANT SELECT ON $DB.t TO $u_def;
CREATE VIEW v_def_prof SQL SECURITY DEFINER DEFINER = $u_def AS SELECT id, public_label FROM t;

GRANT SELECT(id, public_label) ON $DB.t TO $u_low;
GRANT SELECT(id, public_label) ON $DB.d TO $u_low;
GRANT SELECT(id, public_label) ON $DB.d3 TO $u_low;
GRANT SELECT ON $DB.v_inv TO $u_low;
GRANT SELECT ON $DB.v_inv_d3 TO $u_low;
GRANT SELECT(alias_col) ON $DB.t TO $u_alias;
GRANT SELECT ON $DB.v_def TO $u_view;
GRANT SELECT ON $DB.v_def_shadow TO $u_view;
GRANT SELECT ON $DB.v_none_shadow TO $u_view;
GRANT SELECT ON $DB.v_def_prof TO $u_view;
GRANT SELECT(id, public_label) ON $DB.pv_def TO $u_low;
"

err_file="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.err"

# On success print only stdout; on failure print the error code name from the trailing (CODE_NAME) token.
function run()
{
    local user=$1
    local query=$2
    local out
    if out=$($CLICKHOUSE_CLIENT --user "$user" --password password -q "$query" 2>"$err_file"); then
        echo "$out"
    else
        grep -oE '\([A-Z_]+\)' "$err_file" | tail -1 | tr -d '()'
    fi
}

for analyzer in 1 0; do

echo "-- analyzer=$analyzer: 01 control"
run "$u_low" "SELECT count() FROM t WHERE secret_token = '$T' SETTINGS enable_analyzer=$analyzer"

echo "-- analyzer=$analyzer: 02 filter over denied column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 03 filter over denied column, qualified key and expression"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.t': '$DB.t.secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 04 filter over denied tuple subcolumn"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'tup.a = ''ta'''}"

echo "-- analyzer=$analyzer: 05 filter over alias of a denied column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'alias_col = ''$T'''}"

echo "-- analyzer=$analyzer: 06 filter over alias with alias grant"
run "$u_alias" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'alias_col = ''$T'''}"

echo "-- analyzer=$analyzer: 07 filter over denied column with PREWHERE"
run "$u_low" "SELECT count() FROM t PREWHERE id = 1 SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 08 filter over granted column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': 'public_label = ''customer_1'''}"

echo "-- analyzer=$analyzer: 09 constant filter"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'t': '1'}"

echo "-- analyzer=$analyzer: 10 invoker view, filter keyed by underlying table over denied column"
run "$u_low" "SELECT count() FROM v_inv SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.t': 'secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 11 definer view, filter keyed by underlying table over denied column"
run "$u_view" "SELECT count() FROM v_def SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.t': 'secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 12 definer view, filter keyed by the view itself"
run "$u_view" "SELECT count() FROM v_def SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.v_def': 'id = 1'}"

echo "-- analyzer=$analyzer: 13 distributed table, filter keyed by itself over denied column"
run "$u_low" "SELECT count() FROM d SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.d': 'secret_token = ''$T'''}"

echo "-- analyzer=$analyzer: 14 distributed table, filter keyed by itself over granted column"
run "$u_low" "SELECT count() FROM d SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.d': 'public_label = ''customer_1'''}"

echo "-- analyzer=$analyzer: 15 parallel replicas custom key (sampling) over denied column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 2, parallel_replica_offset = 1, parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 16 parallel replicas custom key (range) over denied column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_range', parallel_replicas_custom_key = 'cityHash64(secret_token)', parallel_replicas_custom_key_range_lower = 0, parallel_replicas_custom_key_range_upper = 1000, parallel_replicas_count = 2, parallel_replica_offset = 1"

echo "-- analyzer=$analyzer: 17 parallel replicas custom key (sampling) over granted column"
run "$u_low" "SELECT count() FROM t SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'id', parallel_replicas_count = 2, parallel_replica_offset = 1"

# One shard, three replicas: the key is shipped to the replicas; sum() merges the per-replica partial counts.
echo "-- analyzer=$analyzer: 18 custom key shipped to the replicas, over denied column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM d3) SETTINGS enable_analyzer=$analyzer, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 19 custom key shipped to the replicas, over granted column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM d3) SETTINGS enable_analyzer=$analyzer, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'id'"

# The invoker's key is applied to the view's own columns, where it is 0 for every row, so replica 0 of 2 keeps
# both rows. The DEFINER/NONE body reads without the key: re-evaluated over t as the definer, the key
# would be 1 for the row with the real secret, that row would drop out, and the count would reveal it.
echo "-- analyzer=$analyzer: 20 definer view, custom key over a view column shadowing a denied table column"
run "$u_view" "SELECT count() FROM v_def_shadow SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 2, parallel_replica_offset = 0, parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 21 none view, custom key over a view column shadowing a denied table column"
run "$u_view" "SELECT count() FROM v_none_shadow SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 2, parallel_replica_offset = 0, parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 22 definer view, custom key over a denied table column that the view does not expose"
run "$u_view" "SELECT count() FROM v_def SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 2, parallel_replica_offset = 1, parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 23 definer view, custom key over a view column"
run "$u_view" "SELECT count() FROM v_def SETTINGS enable_analyzer=$analyzer, cluster_for_parallel_replicas = 'test_shard_localhost', max_parallel_replicas = 2, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_count = 2, parallel_replica_offset = 1, parallel_replicas_custom_key = 'id'"

echo "-- analyzer=$analyzer: 24 none view, filter keyed by underlying table over denied column"
run "$u_view" "SELECT count() FROM v_none_shadow SETTINGS enable_analyzer=$analyzer, additional_table_filters = {'$DB.t': 'secret_token = ''$T'''}"

# The invoker's context does not activate custom-key parallel replicas, so the outer query never applies the key.
# The definer's profile activates them inside the body, where the invoker's key over the denied column would be
# evaluated as the definer: replica 1 of 2 would keep only the row with the real secret and the count would be 1.
echo "-- analyzer=$analyzer: 25 definer view, custom key supplied by the invoker, activated by the definer's profile"
run "$u_view" "SELECT count() FROM v_def_prof SETTINGS enable_analyzer=$analyzer, parallel_replicas_count = 2, parallel_replica_offset = 1, parallel_replicas_custom_key = 'secret_token = ''$T'''"

done

# The analyzer resolves a parameterized view as a table function node wrapping the view storage, and the filter
# access check has a dedicated branch for it. The legacy interpreter does not apply additional_table_filters
# keyed by a parameterized view at all, so these cases are analyzer-only.
echo "-- analyzer=1: 26 definer parameterized view, filter over denied view column"
run "$u_low" "SELECT count() FROM pv_def(min_id = 1) SETTINGS enable_analyzer=1, additional_table_filters = {'pv_def': 'secret_token = ''$T'''}"

echo "-- analyzer=1: 27 definer parameterized view, qualified key, filter over denied view column"
run "$u_low" "SELECT count() FROM pv_def(min_id = 1) SETTINGS enable_analyzer=1, additional_table_filters = {'$DB.pv_def': 'secret_token = ''$T'''}"

echo "-- analyzer=1: 28 definer parameterized view, filter over granted view column"
run "$u_low" "SELECT count() FROM pv_def(min_id = 1) SETTINGS enable_analyzer=1, additional_table_filters = {'pv_def': 'public_label = ''customer_1'''}"

echo "-- analyzer=1: 29 definer parameterized view, no filter"
run "$u_low" "SELECT count() FROM pv_def(min_id = 1) SETTINGS enable_analyzer=1"

# The pushdown replaces the read from the view with a read from d3; the key must be checked against d3 there too.
echo "-- analyzer=1: 30 trivial view over the replicas, pushdown, custom key over denied column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM v_inv_d3) SETTINGS enable_analyzer=1, optimize_trivial_view_pushdown_to_distributed = 1, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=1: 31 trivial view over the replicas, pushdown, custom key over granted column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM v_inv_d3) SETTINGS enable_analyzer=1, optimize_trivial_view_pushdown_to_distributed = 1, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'id'"

echo "-- analyzer=1: 32 trivial view over the replicas, no pushdown, custom key over denied column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM v_inv_d3) SETTINGS enable_analyzer=1, optimize_trivial_view_pushdown_to_distributed = 0, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'secret_token = ''$T'''"

$CLICKHOUSE_CLIENT -n -q "
DROP VIEW IF EXISTS pv_def;
DROP VIEW IF EXISTS v_def_prof;
DROP VIEW IF EXISTS v_none_shadow;
DROP VIEW IF EXISTS v_def_shadow;
DROP VIEW IF EXISTS v_def;
DROP VIEW IF EXISTS v_inv;
DROP VIEW IF EXISTS v_inv_d3;
DROP TABLE IF EXISTS d3;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS t;
DROP USER IF EXISTS $u_low, $u_alias, $u_view, $u_def;
DROP SETTINGS PROFILE IF EXISTS $p_def;
"

rm -f "$err_file"
