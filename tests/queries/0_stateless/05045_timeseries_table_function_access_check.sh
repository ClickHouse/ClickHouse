#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the timeSeriesSelector() and prometheusQuery() arms parse PromQL, which needs ANTLR4.
# no-replicated-database: on a replicated database the DDL runs with no user, so an access check
#                         asserted here is a no-op and the deny arms silently pass (issue #111561).

# Reaching a TimeSeries table through a table function requires the privileges the equivalent direct
# operation requires: on the TimeSeries table named in the call, and on the target table the rows or the
# column definitions actually come from.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user05045_${CLICKHOUSE_DATABASE}_$RANDOM"
policy="policy05045_${CLICKHOUSE_DATABASE}"
db=${CLICKHOUSE_DATABASE}

CLIENT_TS="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"

${CLIENT_TS} <<EOF
CREATE TABLE $db.ts_samples (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE $db.ts_tags (id UInt64, metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.ts_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE $db.ts ENGINE = TimeSeries DATA $db.ts_samples TAGS $db.ts_tags METRICS $db.ts_metrics;
INSERT INTO $db.ts_samples VALUES (1, '2026-01-01 00:00:00.000', 42);

CREATE TABLE $db.ts_samples_hidden (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE $db.ts_samples_alias ENGINE = Alias('$db', 'ts_samples_hidden');
CREATE TABLE $db.ts_via_alias ENGINE = TimeSeries
    DATA $db.ts_samples_alias TAGS $db.ts_tags METRICS $db.ts_metrics;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

CLIENT_USER="${CLICKHOUSE_CLIENT} --user $user"

# With no grants at all, a direct DESCRIBE is refused; so is every table function reaching the same table.
${CLIENT_USER} -q "DESCRIBE $db.ts FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "DESCRIBE timeSeriesTags($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "DESCRIBE timeSeriesMetrics($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "DESCRIBE prometheusQuery($db.ts, 'up', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }"

# Granting the target tables is not enough: the TimeSeries table named in the call is checked as well.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT, SHOW COLUMNS ON $db.ts_samples TO $user"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }"

${CLICKHOUSE_CLIENT} -q "GRANT SELECT, SHOW COLUMNS ON $db.ts_tags TO $user"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "SELECT * FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }"
# timeSeriesSelector() reads column types from the TimeSeries table as well as from its tags target,
# so holding the tags target alone does not describe it.
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"

# A persistent table built over timeSeriesSelector() is authorized on every read, not once at creation,
# so holding only the privilege that creating it needed does not make it readable.
${CLICKHOUSE_CLIENT} -q "GRANT CREATE TABLE ON $db.* TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON $db.sel_dst TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts TO $user"
${CLIENT_USER} -q "CREATE TABLE $db.sel_dst AS timeSeriesSelector($db.ts, 'up', 0, 9999999999)"
${CLIENT_USER} -q "SELECT * FROM $db.sel_dst FORMAT Null; -- { serverError ACCESS_DENIED }"
# reading it directly is refused for the same reason: describing the TimeSeries table is not reading it.
${CLIENT_USER} -q "SELECT * FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "SELECT * FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }"

# Symmetrically, granting only the TimeSeries table is not enough either.
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT, INSERT, SHOW COLUMNS ON $db.ts_samples FROM $user"
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT, SHOW COLUMNS ON $db.ts_tags FROM $user"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT, SHOW COLUMNS ON $db.ts TO $user"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }"
# and symmetrically for the tags target that timeSeriesSelector() reads its id type from.
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"

# SHOW COLUMNS on both tables describes, but does not read.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts_samples TO $user"
echo 'describe with SHOW COLUMNS on both tables'
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) FORMAT TSV"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
# describe_include_virtual_columns resolves the structure through the data path, which needs SELECT.
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) SETTINGS describe_include_virtual_columns = 1 FORMAT Null; -- { serverError ACCESS_DENIED }"

# An Alias target exposes another table's metadata, so that table is checked too. Nothing in this test
# ever grants on ts_samples_hidden, which is what ts_samples_alias points at.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts_via_alias TO $user"
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts_samples_alias TO $user"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts_via_alias) FORMAT Null; -- { serverError ACCESS_DENIED }"

# With both tables granted, every function works as before.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON $db.ts_samples TO $user"
echo 'select with SELECT on both tables'
${CLIENT_USER} -q "SELECT id, value FROM timeSeriesSamples($db.ts) ORDER BY id FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, SHOW COLUMNS ON $db.ts_tags TO $user"
echo 'selector, prometheusQuery and the persistent table over the selector'
${CLIENT_USER} -q "SELECT count() FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999)"
${CLIENT_USER} -q "SELECT value FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT TSV"
${CLIENT_USER} -q "SELECT count() FROM $db.sel_dst"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts) SETTINGS describe_include_virtual_columns = 1 FORMAT Null"

# A row policy on the TimeSeries table cannot be enforced on the target table's rows, so the read fails
# closed while the policy exists, and works again once it is dropped.
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY $policy ON $db.ts FOR SELECT USING 0 TO $user"
${CLIENT_USER} -q "SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY $policy ON $db.ts"
echo 'select again after the row policy is dropped'
${CLIENT_USER} -q "SELECT id, value FROM timeSeriesSamples($db.ts) ORDER BY id FORMAT TSV"

# The write direction needs INSERT on both tables.
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $db.ts_samples TO $user"
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7)"
echo 'rows after the insert'
${CLICKHOUSE_CLIENT} -q "SELECT count() FROM $db.ts_samples"

# These three functions hand back a pre-existing table that stores data on disk, which cannot back a
# persistent table.
${CLIENT_TS} -q "CREATE TABLE $db.dst AS timeSeriesSamples($db.ts); -- { serverError BAD_ARGUMENTS }"
echo 'the target table still exists under its own name'
${CLICKHOUSE_CLIENT} -q "EXISTS $db.ts_samples"

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
