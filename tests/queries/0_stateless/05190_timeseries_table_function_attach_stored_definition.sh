#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.
# Tag no-replicated-database: this test replays stored definitions with `DETACH TABLE` + `ATTACH TABLE`,
#                             and `DatabaseReplicated::checkQueryValid` rejects a non-permanent
#                             `DETACH TABLE`; one arm also uses `ATTACH TABLE ... UUID`.
#
# `CREATE TABLE ... AS timeSeriesSelector(...)` / `AS prometheusQuery(...)` stores the table-function call
# as the table's definition. Replaying that definition must not require the objects it names to exist -
# otherwise a dropped source table makes the definition unloadable, and with async_load_databases = 0 one
# failed load job takes the whole server down. Only reading such a table may fail.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_time_series_table=1"
OTHER_DB="${CLICKHOUSE_DATABASE}_source"
LOADER_DB="${CLICKHOUSE_DATABASE}_loader"
# Every insert below is INSERT ... SELECT: the client keeps reading stdin for further rows of an inline
# VALUES payload, and stdin here is an inherited pipe that never ends, so the test would hang.

# Prints the error name of a failing query, or 'no error' when it unexpectedly succeeds.
error_of() {
    local err name
    err=$("$@" 2>&1 >/dev/null) && { echo 'no error'; return; }
    name=$(grep -oE '\([A-Z_]+\)$' <<<"$err" | head -1 | tr -d '()')
    echo "${name:-unexpected error: $(head -c 120 <<<"$err")}"
}

# Replays the stored definition of a table the way a server restart does.
reattach() { $CLIENT -q "DETACH TABLE $1" && $CLIENT -q "ATTACH TABLE $1" && echo "attached $1"; }

echo '--- (a) control: with its source intact a replayed definition still reads its row ---'
$CLIENT -mn -q "
    CREATE TABLE ts ENGINE = TimeSeries;
    INSERT INTO ts (metric_name, tags, time_series) SELECT 'up', map('job', 'prometheus'), [(1000, 42)];
    CREATE TABLE sel AS timeSeriesSelector(ts, 'up', 0, 9999999999);
    CREATE TABLE pq  AS prometheusQuery(ts, 'up', 1000);
    CREATE TABLE pqr AS prometheusQueryRange(ts, 'up', 1000, 1300, 60);
"
reattach sel
reattach pq
reattach pqr
$CLIENT -q "SELECT count() FROM sel"
$CLIENT -q "SELECT count() FROM pq"
# One row per series, so count() cannot see the step. The number of evaluation timestamps can: over
# 1000..1300 this is 5 at step 60, 10 at step 30 and 3 at step 120.
$CLIENT -q "SELECT sum(length(time_series)) FROM pqr"

echo '--- (b) after the source is dropped all three definitions still attach ---'
$CLIENT -q "DROP TABLE ts SYNC"
reattach sel
reattach pq
reattach pqr

echo '--- (c) and reading them reports the missing source ---'
error_of $CLIENT -q "SELECT count() FROM sel"
error_of $CLIENT -q "SELECT count() FROM pq"
error_of $CLIENT -q "SELECT sum(length(time_series)) FROM pqr"

echo '--- (d) a full definition naming a table that never existed attaches ---'
uuid=$($CLIENT -q "SELECT generateUUIDv4()")
# A full `ATTACH TABLE` warns about the explicit definition, which would pollute stderr.
$CLIENT --send_logs_level=fatal -q "
    ATTACH TABLE never_existed UUID '$uuid' (\`id\` Tuple(UInt64, LowCardinality(UUID)), \`timestamp\` DateTime64(3), \`value\` Float64)
    AS timeSeriesSelector(no_such_time_series_table, 'up', 0, 9999999999)" && echo 'attached never_existed'

echo '--- (e) after the source is renamed the definition still attaches, and the read reports it ---'
$CLIENT -mn -q "
    CREATE TABLE ts_renamed ENGINE = TimeSeries;
    CREATE TABLE sel_renamed AS timeSeriesSelector(ts_renamed, 'up', 0, 9999999999);
    RENAME TABLE ts_renamed TO ts_new_name;
"
reattach sel_renamed
error_of $CLIENT -q "SELECT count() FROM sel_renamed"

echo '--- (g) a fresh CREATE whose structure is inferred from the function still requires the source ---'
error_of $CLIENT -q "CREATE TABLE inferred_missing AS timeSeriesSelector(no_such_time_series_table, 'up', 0, 9999999999)"
error_of $CLIENT -q "CREATE TABLE inferred_missing_pq AS prometheusQuery(no_such_time_series_table, 'up', 1000)"

echo '--- (g2) with an explicit column list nothing is resolved at CREATE ---'
$CLIENT -q "
    CREATE TABLE explicit_columns (\`id\` Tuple(UInt64, LowCardinality(UUID)), \`timestamp\` DateTime64(3), \`value\` Float64)
    AS timeSeriesSelector(no_such_time_series_table, 'up', 0, 9999999999)" && echo 'created explicit_columns'

echo '--- (c2) so its missing source is reported on the first read, not hidden ---'
error_of $CLIENT -q "SELECT count() FROM explicit_columns"

echo '--- (h) the tags target is dropped while the TimeSeries table survives ---'
$CLIENT -mn -q "
    CREATE TABLE ts_tags_target (id UInt64, metric_name LowCardinality(String), tags Map(LowCardinality(String), String),
                                 min_time DateTime64(3), max_time DateTime64(3)) ENGINE = MergeTree() ORDER BY id;
    CREATE TABLE ts_samples_target (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = MergeTree() ORDER BY (id, timestamp);
    CREATE TABLE ts_split ENGINE = TimeSeries SAMPLES ts_samples_target TAGS ts_tags_target;
    CREATE TABLE sel_no_tags AS timeSeriesSelector(ts_split, 'up', 0, 9999999999);
    DROP TABLE ts_tags_target SYNC;
"
reattach sel_no_tags
error_of $CLIENT -q "SELECT count() FROM sel_no_tags"

echo '--- (i) the whole database of the source is dropped ---'
$CLIENT -mn -q "
    CREATE DATABASE ${OTHER_DB};
    CREATE TABLE ${OTHER_DB}.ts ENGINE = TimeSeries;
    CREATE TABLE sel_no_database AS timeSeriesSelector(${OTHER_DB}.ts, 'up', 0, 9999999999);
    DROP DATABASE ${OTHER_DB} SYNC;
"
reattach sel_no_database

echo '--- (l) the time bounds are still converted, so the checks that read them still fire ---'
$CLIENT -q "CREATE TABLE ts_checks ENGINE = TimeSeries"
error_of $CLIENT -q "SELECT * FROM timeSeriesSelector(ts_checks, 'up', 9999999999, 0)"
error_of $CLIENT -q "SELECT * FROM timeSeriesSelector(ts_checks, 'rate(up[5m])', 0, 9999999999)"

echo '--- (m) a stored definition binds to the database it was created in, not the replaying session ---'
$CLIENT -mn -q "
    CREATE DATABASE ${OTHER_DB}_replay;
    CREATE TABLE ts_qual ENGINE = TimeSeries;
    INSERT INTO ts_qual (metric_name, tags, time_series) SELECT 'up', map('job', 'prometheus'), [(1000, 42)];
    CREATE TABLE ${OTHER_DB}_replay.ts_qual ENGINE = TimeSeries;
    CREATE TABLE sel_qual AS timeSeriesSelector(ts_qual, 'up', 0, 9999999999);
    CREATE TABLE pq_qual AS prometheusQuery(ts_qual, 'up', 1000);
    DETACH TABLE sel_qual;
    DETACH TABLE pq_qual;
"
# Replay while a different database is current: an unqualified stored name would resolve against it and
# bind to the decoy above, which returns rows from the wrong table rather than an error. Both table
# functions carry their own copy of the qualification, so both are replayed here.
$CLIENT -mn -q "USE ${OTHER_DB}_replay; ATTACH TABLE ${CLICKHOUSE_DATABASE}.sel_qual;" && echo 'attached sel_qual'
$CLIENT -mn -q "USE ${OTHER_DB}_replay; ATTACH TABLE ${CLICKHOUSE_DATABASE}.pq_qual;" && echo 'attached pq_qual'
# Labelled: two bare counts are identical lines, so a diff could not name which one lost its binding.
$CLIENT -q "SELECT 'sel_qual', count() FROM sel_qual"
$CLIENT -q "SELECT 'pq_qual', count() FROM pq_qual"
$CLIENT -q "DROP DATABASE ${OTHER_DB}_replay SYNC"

echo '--- (n) a definition naming the table being created attaches, and the read reports the engine ---'
# An ATTACH runs under the query context, where the table's startup task has already finished, so this
# arm pins the post-fix behaviour only; arm (o) replays the same shape through the table loader.
$CLIENT -q "
    CREATE TABLE selfref (\`id\` Tuple(UInt64, LowCardinality(UUID)), \`timestamp\` DateTime64(3), \`value\` Float64)
    AS timeSeriesSelector(selfref, 'up', 0, 9999999999)" && echo 'created selfref'
reattach selfref
error_of $CLIENT -q "SELECT count() FROM selfref"

echo '--- (o) a real load replays every definition of a database at once ---'
# `ATTACH DATABASE` goes through the same `TablesLoader` as server startup, so unlike the arms above it
# replays under the loader's context, whose current database is the global one, and it wires each load
# job to the table's own startup job.
$CLIENT -mn -q "
    CREATE DATABASE ${LOADER_DB};
    USE ${LOADER_DB};
    CREATE TABLE ts_load ENGINE = TimeSeries;
    INSERT INTO ts_load (metric_name, tags, time_series) SELECT 'up', map('job', 'prometheus'), [(1000, 42)];
    CREATE TABLE ts_load_gone ENGINE = TimeSeries;
    CREATE TABLE sel_load AS timeSeriesSelector(ts_load, 'up', 0, 9999999999);
    CREATE TABLE pq_load AS prometheusQuery(ts_load, 'up', 1000);
    CREATE TABLE sel_load_gone AS timeSeriesSelector(ts_load_gone, 'up', 0, 9999999999);
    DROP TABLE ts_load_gone SYNC;
    CREATE TABLE selfref_load (\`id\` Tuple(UInt64, LowCardinality(UUID)), \`timestamp\` DateTime64(3), \`value\` Float64)
    AS timeSeriesSelector(selfref_load, 'up', 0, 9999999999);
"
# The source is named in full here: this definition isolates the stored *query*, which is not parsed
# until the read either, so a text this grammar rejects cannot make the definition unloadable.
$CLIENT -q "
    CREATE TABLE ${LOADER_DB}.bad_promql (\`tags\` Array(Tuple(String, String)), \`timestamp\` DateTime64(3), \`value\` Float64)
    AS prometheusQuery(${LOADER_DB}.ts_load, 'up{', 1000)" && echo 'created bad_promql'
$CLIENT -mn -q "DETACH DATABASE ${LOADER_DB}; ATTACH DATABASE ${LOADER_DB};" && echo 'attached database'
# The names the loader resolves are the ones written back at CREATE: unqualified, they would be looked
# up in the global current database, where this test has nothing.
$CLIENT -q "SELECT 'sel_load', count() FROM ${LOADER_DB}.sel_load"
$CLIENT -q "SELECT 'pq_load', count() FROM ${LOADER_DB}.pq_load"
error_of $CLIENT -q "SELECT count() FROM ${LOADER_DB}.sel_load_gone"
error_of $CLIENT -q "SELECT count() FROM ${LOADER_DB}.selfref_load"
error_of $CLIENT -q "SELECT count() FROM ${LOADER_DB}.bad_promql"
$CLIENT -q "DROP DATABASE ${LOADER_DB} SYNC"

# A stored table-function definition whose source is missing is exactly what this test creates, so it
# must not leave one behind for whatever runs next. No SYNC: a proxy whose materialization always
# throws still holds its table function, which a synchronous drop waits for.
$CLIENT -mn -q "
    DROP TABLE IF EXISTS sel, pq, pqr, never_existed, sel_renamed, explicit_columns, sel_no_tags,
                         sel_no_database, selfref, sel_qual, pq_qual;
    DROP TABLE IF EXISTS ts_new_name, ts_split, ts_checks, ts_qual;
    DROP TABLE IF EXISTS ts_samples_target;
"
