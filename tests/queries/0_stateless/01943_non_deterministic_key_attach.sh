#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# The full-definition ATTACH rows need an explicit per-copy UUID: a .sql file cannot interpolate one,
# an Ordinary database rejects the form, and a Replicated database assigns the UUID itself.

# The CREATE side of this rule is covered by 01943_non_deterministic_order_key.sql.
#
# A non-deterministic function in the sorting or partition key is refused for a new table, but a table
# already stored with such a key must keep loading and keep its rows readable: a function can start
# reporting itself as non-deterministic in a later version, and that must not put existing data out of
# reach. Only the determinism rule is relaxed on load - a key that is constant stays refused either way.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

CLIENT="${CLICKHOUSE_CLIENT} --server_logs_file=/dev/null --allow_introspection_functions=1"

# Report the error name once, whether or not the client repeats the message.
error_name() { grep -oE '\([A-Z_]+\)' | tail -1; }

echo '-- CREATE stays refused'
${CLIENT} -q "CREATE TABLE t_sort (d Date, addr UInt64) ENGINE = MergeTree ORDER BY (d, addressToSymbol(addr))" 2>&1 | error_name
${CLIENT} -q "CREATE TABLE t_part (d Date, addr UInt64) ENGINE = MergeTree PARTITION BY addressToSymbol(addr) ORDER BY d" 2>&1 | error_name
${CLIENT} -q "CREATE TABLE t_conc (s DateTime, f DateTime) ENGINE = MergeTree ORDER BY (s, runningConcurrency(s, f))" 2>&1 | error_name

echo '-- rows survive a reload from the stored definition'
attach_insert_reload() {
    local name="$1" definition="$2" rows="$3"
    local uuid err
    uuid=$(${CLIENT} -q "SELECT generateUUIDv4()")
    err=$(${CLIENT} -q "ATTACH TABLE ${name} UUID '${uuid}' ${definition}" 2>&1)
    # A refused attach is reported here, so it does not resurface as a missing table below.
    if [ -n "${err}" ]; then
        echo "${err}" | error_name
        return
    fi
    ${CLIENT} -q "INSERT INTO ${name} ${rows}"
    # The short ATTACH re-reads the stored definition - the same path a server restart takes.
    ${CLIENT} -q "DETACH TABLE ${name}"
    err=$(${CLIENT} -q "ATTACH TABLE ${name}" 2>&1)
    if [ -n "${err}" ]; then
        echo "${err}" | error_name
        return
    fi
    ${CLIENT} -q "SELECT count(), '${name}' FROM ${name}"
    ${CLIENT} -q "DROP TABLE ${name}"
}

attach_insert_reload t_sort \
    "(d Date, addr UInt64) ENGINE = MergeTree ORDER BY (d, addressToSymbol(addr))" \
    "SELECT toDate('2026-01-01') + number % 10, number * 64 FROM numbers(100)"
attach_insert_reload t_part \
    "(d Date, addr UInt64) ENGINE = MergeTree PARTITION BY addressToSymbol(addr) ORDER BY d" \
    "SELECT toDate('2026-01-01') + number % 10, number * 64 FROM numbers(100)"
attach_insert_reload t_conc \
    "(s DateTime, f DateTime) ENGINE = MergeTree ORDER BY (s, runningConcurrency(s, f))" \
    "SELECT toDateTime('2026-01-01 00:00:00') + number, toDateTime('2026-01-01 01:00:00') + number FROM numbers(100)"

echo '-- a constant key is still refused on attach'
UUID_CONST=$(${CLIENT} -q "SELECT generateUUIDv4()")
${CLIENT} -q "ATTACH TABLE t_const UUID '${UUID_CONST}' (n UInt64) ENGINE = MergeTree ORDER BY (n, now())" 2>&1 | error_name
