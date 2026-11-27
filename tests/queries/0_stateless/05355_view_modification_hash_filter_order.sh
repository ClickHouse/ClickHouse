#!/usr/bin/env bash
# Tags: no-ordinary-database

# The `modification_hash` of a view folds the applicable `additional_table_filters` of its effective
# settings and the tables read by them. Only the first entry whose key matches a table applies, and a
# bare and a qualified key can match the same table, so reversing such entries changes the rows and
# must move the hash.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -u

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE src_a (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE src_b (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE aux_a (x UInt64) ENGINE = MergeTree ORDER BY x;
    CREATE TABLE aux_b (x UInt64) ENGINE = MergeTree ORDER BY x;
    INSERT INTO src_a VALUES (1), (2);
    INSERT INTO src_b VALUES (1), (2);
    INSERT INTO aux_a VALUES (1);
    INSERT INTO aux_b VALUES (2);
    CREATE VIEW v AS SELECT x FROM src_a JOIN src_b USING x;
"

hash_of_v()
{
    $CLICKHOUSE_CLIENT --additional_table_filters "$1" -q "
        SELECT modification_hash FROM system.tables WHERE database = currentDatabase() AND name = 'v'"
}

filter_a="'src_a': 'x IN (SELECT x FROM aux_a)'"
filter_b="'src_b': 'x IN (SELECT x FROM aux_b)'"
forward=$(hash_of_v "{${filter_a}, ${filter_b}}")
[ -n "${forward}" ] && [ "${forward}" != "\\N" ] && echo "view has a hash: yes" || echo "view has a hash: no (${forward})"

# Both keys match `src_a`: the first one wins, so the two orders read different rows.
bare="'src_a': 'x = 1'"
qualified="'${CLICKHOUSE_DATABASE}.src_a': 'x = 2'"
[ "$($CLICKHOUSE_CLIENT --additional_table_filters "{${bare}, ${qualified}}" -q "SELECT x FROM v")" = 1 ] \
    && [ "$($CLICKHOUSE_CLIENT --additional_table_filters "{${qualified}, ${bare}}" -q "SELECT x FROM v")" = 2 ] \
    && echo "the first matching filter applies: yes" || echo "the first matching filter applies: no"
bare_first=$(hash_of_v "{${bare}, ${qualified}}")
qualified_first=$(hash_of_v "{${qualified}, ${bare}}")
[ "${bare_first}" != "${qualified_first}" ] && echo "the order of overlapping filters moves the hash: yes" || echo "the order of overlapping filters moves the hash: no (${bare_first})"

$CLICKHOUSE_CLIENT -q "INSERT INTO aux_b VALUES (1)"
changed=$(hash_of_v "{${filter_a}, ${filter_b}}")
[ "${forward}" != "${changed}" ] && echo "a table read by a filter moves the hash: yes" || echo "a table read by a filter moves the hash: no"

$CLICKHOUSE_CLIENT -q "
    DROP VIEW v SYNC;
    DROP TABLE src_a SYNC;
    DROP TABLE src_b SYNC;
    DROP TABLE aux_a SYNC;
    DROP TABLE aux_b SYNC;
"
