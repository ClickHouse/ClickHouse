#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test checks the `DETACH`/`ATTACH` the hook itself issues
# no-replicated-database: the hook never reattaches tables of a `Replicated` database

# Objects whose resolution has side effects the hook must not trigger before the query does:
# - `FunctionDictHelper::getDictionary` loads a dictionary before it checks the access, so a query naming a
#   missing or unloadable dictionary fails only then; the hook must not reattach the other tables before.
# - A table of a database with `lazy_load_tables` is a stand-in until first used; asking the stand-in about
#   the storage creates and starts it up, which a metadata-only query never implies.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

LAZY_DATABASE="${CLICKHOUSE_DATABASE}_lazy"

${CLICKHOUSE_CLIENT} -q "
    DROP DICTIONARY IF EXISTS d_reattach_loaded;
    DROP DICTIONARY IF EXISTS d_reattach_broken;
    DROP TABLE IF EXISTS t_reattach_dict_src;
    DROP TABLE IF EXISTS t_reattach_dict_values;
    CREATE TABLE t_reattach_dict_src (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_reattach_dict_src VALUES (1);
    CREATE TABLE t_reattach_dict_values (a UInt64, v UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO t_reattach_dict_values VALUES (1, 1);
    CREATE DICTIONARY d_reattach_loaded (a UInt64, v UInt64) PRIMARY KEY a
        SOURCE(CLICKHOUSE(TABLE 't_reattach_dict_values' DB '${CLICKHOUSE_DATABASE}'))
        LAYOUT(FLAT()) LIFETIME(0);
    CREATE DICTIONARY d_reattach_broken (a UInt64, v UInt64) PRIMARY KEY a
        SOURCE(CLICKHOUSE(TABLE 't_reattach_dict_missing' DB '${CLICKHOUSE_DATABASE}'))
        LAYOUT(FLAT()) LIFETIME(0);
    SYSTEM RELOAD DICTIONARY d_reattach_loaded;
"

# The query fails on the dictionary, and `t_reattach_dict_src` must not be detached before.
function check_fails_without_detach()
{
    check_if_detached_impl "$1" t_reattach_dict_src
    if [ "$REATTACH_STATUS" -eq 0 ]; then
        echo "FAIL (query unexpectedly succeeded)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE $CLICKHOUSE_DATABASE.t_reattach_dict_src"; then
        echo "FAIL (a table was detached for a query failing on its dictionary)"
    else
        echo "OK"
    fi
}

echo "dictionary"
check_fails_without_detach "SELECT count() FROM t_reattach_dict_src WHERE dictHas('d_reattach_missing', a)"
check_fails_without_detach "SELECT count() FROM t_reattach_dict_src WHERE dictHas('${CLICKHOUSE_DATABASE}.d_reattach_missing', a)"
check_fails_without_detach "SELECT count() FROM t_reattach_dict_src WHERE dictGet(d_reattach_broken, 'v', a) = 1"
# A loaded dictionary does not stop the randomization.
check_if_detached "SELECT count() FROM t_reattach_dict_src WHERE dictHas('d_reattach_loaded', a)" t_reattach_dict_src
check_if_detached "SELECT count() FROM t_reattach_dict_src WHERE dictGet('${CLICKHOUSE_DATABASE}.d_reattach_loaded', 'v', a) = 1" t_reattach_dict_src

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    CREATE DATABASE ${LAZY_DATABASE} ENGINE = Atomic SETTINGS lazy_load_tables = 1;
    CREATE TABLE ${LAZY_DATABASE}.t (a UInt64) ENGINE = MergeTree ORDER BY a;
    INSERT INTO ${LAZY_DATABASE}.t VALUES (1);
    DETACH DATABASE ${LAZY_DATABASE};
    ATTACH DATABASE ${LAZY_DATABASE};
"

function lazy_table_engine()
{
    ${CLICKHOUSE_CLIENT} -q "SELECT engine FROM system.tables WHERE database = '${LAZY_DATABASE}' AND name = 't'"
}

function check_lazy_table()
{
    check_if_detached_impl "$1" t
    if [ "$REATTACH_STATUS" -ne 0 ]; then
        echo "FAIL (client error: $REATTACH_OUTPUT)"
    elif echo "$REATTACH_OUTPUT" | grep -q "DETACH TABLE ${LAZY_DATABASE}.t"; then
        echo "detached"
    else
        echo "not detached"
    fi
}

echo "lazy table"
lazy_table_engine
# The stand-in is left alone: neither reattached nor loaded.
check_lazy_table "EXISTS TABLE ${LAZY_DATABASE}.t"
lazy_table_engine
# The query itself loads the table.
check_lazy_table "SELECT count() FROM ${LAZY_DATABASE}.t"
lazy_table_engine
# Once loaded, the table is reattached like any other.
check_lazy_table "SELECT count() FROM ${LAZY_DATABASE}.t"

${CLICKHOUSE_CLIENT} -q "
    DROP DATABASE IF EXISTS ${LAZY_DATABASE};
    DROP DICTIONARY IF EXISTS d_reattach_loaded;
    DROP DICTIONARY IF EXISTS d_reattach_broken;
    DROP TABLE IF EXISTS t_reattach_dict_src;
    DROP TABLE IF EXISTS t_reattach_dict_values;
"
