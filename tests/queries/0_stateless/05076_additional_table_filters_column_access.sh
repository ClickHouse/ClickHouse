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

$CLICKHOUSE_CLIENT -n -q "
DROP USER IF EXISTS $u_low, $u_alias, $u_view;

CREATE TABLE t (id UInt8, public_label String, secret_token String, tup Tuple(a String), alias_col String ALIAS secret_token) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1, 'customer_1', 'prod_customer_token_25_8_secret', ('ta')), (2, 'customer_2', 'other_customer_private_token', ('tb'));
CREATE TABLE d (id UInt8, public_label String, secret_token String) ENGINE = Distributed(test_shard_localhost, currentDatabase(), t);
CREATE TABLE d3 (id UInt8, public_label String, secret_token String) ENGINE = Distributed(test_cluster_one_shard_three_replicas_localhost, currentDatabase(), t);
CREATE VIEW v_inv SQL SECURITY INVOKER AS SELECT id, public_label FROM t;
CREATE VIEW v_def SQL SECURITY DEFINER DEFINER = CURRENT_USER AS SELECT id, public_label FROM t;

CREATE USER $u_low IDENTIFIED WITH plaintext_password BY 'password';
CREATE USER $u_alias IDENTIFIED WITH plaintext_password BY 'password';
CREATE USER $u_view IDENTIFIED WITH plaintext_password BY 'password';

GRANT SELECT(id, public_label) ON $DB.t TO $u_low;
GRANT SELECT(id, public_label) ON $DB.d TO $u_low;
GRANT SELECT(id, public_label) ON $DB.d3 TO $u_low;
GRANT SELECT ON $DB.v_inv TO $u_low;
GRANT SELECT(alias_col) ON $DB.t TO $u_alias;
GRANT SELECT ON $DB.v_def TO $u_view;
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

# `d3` spans one shard with three replicas, so the initiator ships the key to the replicas instead of applying it.
# The read is wrapped in a subquery because the initiator sets `distributed_group_by_no_merge` on this path and would
# otherwise return one unmerged partial count per replica, in a non-deterministic order.
echo "-- analyzer=$analyzer: 18 custom key shipped to the replicas, over denied column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM d3) SETTINGS enable_analyzer=$analyzer, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'secret_token = ''$T'''"

echo "-- analyzer=$analyzer: 19 custom key shipped to the replicas, over granted column"
run "$u_low" "SELECT sum(c) FROM (SELECT count() AS c FROM d3) SETTINGS enable_analyzer=$analyzer, max_parallel_replicas = 3, allow_experimental_parallel_reading_from_replicas = 1, parallel_replicas_mode = 'custom_key_sampling', parallel_replicas_custom_key = 'id'"

done

$CLICKHOUSE_CLIENT -n -q "
DROP VIEW IF EXISTS v_def;
DROP VIEW IF EXISTS v_inv;
DROP TABLE IF EXISTS d3;
DROP TABLE IF EXISTS d;
DROP TABLE IF EXISTS t;
DROP USER IF EXISTS $u_low, $u_alias, $u_view;
"

rm -f "$err_file"
