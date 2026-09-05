#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the timeSeriesSelector() and prometheusQuery() arms parse PromQL, which needs ANTLR4.
# no-replicated-database: on a replicated database the DDL runs with no user, so an access check
#                         asserted here is a no-op and the deny arms silently pass (issue #111561).

# Reaching a TimeSeries table through a table function requires the privileges the equivalent direct
# operation requires: on the TimeSeries table named in the call, and on the target table the rows or the
# column definitions actually come from.

# Statements are batched per client on purpose: this file's runtime is client startup, not query work.

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

CREATE TABLE $db.f_samples (id UInt64, timestamp DateTime64(3), value Float64) ENGINE = File(TSV);
CREATE TABLE $db.ts_file ENGINE = TimeSeries
    DATA $db.f_samples TAGS $db.ts_tags METRICS $db.ts_metrics;

-- Two TimeSeries tables whose external target is configured and then dropped: one on the data target,
-- one on the tags target that timeSeriesSelector() reads its id type from.
CREATE TABLE $db.samples_gone (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE $db.ts_gone ENGINE = TimeSeries
    DATA $db.samples_gone TAGS $db.ts_tags METRICS $db.ts_metrics;
CREATE TABLE $db.tags_gone (id UInt64, metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.ts_tags_gone ENGINE = TimeSeries
    DATA $db.ts_samples TAGS $db.tags_gone METRICS $db.ts_metrics;
DROP TABLE $db.samples_gone;
DROP TABLE $db.tags_gone;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

CLIENT_USER="${CLICKHOUSE_CLIENT} --user $user"

${CLIENT_USER} <<EOF
-- With no grants at all, a direct DESCRIBE is refused; so is every table function reaching the same table.
DESCRIBE $db.ts FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesTags($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesMetrics($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE prometheusQuery($db.ts, 'up', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE prometheusQueryRange($db.ts, 'up', 1000, 2000, 60) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }

-- Reading the engine of the named table and the name of its target is refused too, so the argument a
-- caller may not describe cannot be told apart from one holding a different engine.
SELECT * FROM timeSeriesSamples($db.ts_samples) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

${CLIENT_TS} <<EOF
SELECT * FROM timeSeriesSamples($db.ts_samples) FORMAT Null; -- { serverError UNEXPECTED_TABLE_ENGINE }

-- Granting the target tables is not enough: the TimeSeries table named in the call is checked as well.
GRANT SELECT, INSERT, SHOW COLUMNS ON $db.ts_samples TO $user;
EOF

${CLIENT_USER} <<EOF
DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }
EOF

${CLICKHOUSE_CLIENT} -q "GRANT SELECT, SHOW COLUMNS ON $db.ts_tags TO $user"

${CLIENT_USER} <<EOF
SELECT * FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }
-- timeSeriesSelector() reads column types from the TimeSeries table as well as from its tags target,
-- so holding the tags target alone does not describe it.
DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# A persistent table built over timeSeriesSelector() is authorized on every read, not once at creation,
# so holding only the privilege that creating it needed does not make it readable.
${CLICKHOUSE_CLIENT} <<EOF
GRANT CREATE TABLE ON $db.* TO $user;
GRANT SELECT ON $db.sel_dst TO $user;
GRANT SELECT ON $db.pq_dst TO $user;
GRANT SHOW COLUMNS ON $db.ts TO $user;
EOF

${CLIENT_USER} <<EOF
CREATE TABLE $db.sel_dst AS timeSeriesSelector($db.ts, 'up', 0, 9999999999);
SELECT * FROM $db.sel_dst FORMAT Null; -- { serverError ACCESS_DENIED }
-- prometheusQuery() is a separate storage, authorized in its own read path, so it needs its own case.
CREATE TABLE $db.pq_dst AS prometheusQuery($db.ts, '1 + 2', 1000);
SELECT * FROM $db.pq_dst FORMAT Null; -- { serverError ACCESS_DENIED }
-- reading it directly is refused for the same reason: describing the TimeSeries table is not reading it.
SELECT * FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# Symmetrically, granting only the TimeSeries table is not enough either.
${CLICKHOUSE_CLIENT} <<EOF
REVOKE SELECT, INSERT, SHOW COLUMNS ON $db.ts_samples FROM $user;
REVOKE SELECT, SHOW COLUMNS ON $db.ts_tags FROM $user;
GRANT SELECT, INSERT, SHOW COLUMNS ON $db.ts TO $user;
EOF

${CLIENT_USER} <<EOF
DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }
-- and symmetrically for the tags target that timeSeriesSelector() reads its id type from.
DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# SHOW COLUMNS on both tables describes, but does not read.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts_samples TO $user"
echo 'describe with SHOW COLUMNS on both tables'
${CLIENT_USER} <<EOF
DESCRIBE timeSeriesSamples($db.ts) FORMAT TSV;
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
-- describe_include_virtual_columns resolves the structure through the data path, which needs SELECT.
DESCRIBE timeSeriesSamples($db.ts) SETTINGS describe_include_virtual_columns = 1 FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# An Alias target exposes another table's metadata, so that table is checked too. Nothing in this test
# ever grants on ts_samples_hidden, which is what ts_samples_alias points at.
${CLICKHOUSE_CLIENT} <<EOF
GRANT SHOW COLUMNS ON $db.ts_via_alias TO $user;
GRANT SHOW COLUMNS ON $db.ts_samples_alias TO $user;
EOF
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts_via_alias) FORMAT Null; -- { serverError ACCESS_DENIED }"

# A target's engine selects the source privilege the call is checked against, so the target is checked
# before that engine is read: nothing here ever grants on f_samples, and the denial names it rather than
# the source privilege its File engine would ask for.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.ts_file TO $user"
file_denial=$(${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts_file) FORMAT Null" 2>&1)
echo "$file_denial" | grep -q "SHOW COLUMNS ON $db.f_samples" \
    && echo 'File target: denial names the target' || echo "File target: UNEXPECTED [$file_denial]"
echo "$file_denial" | grep -q 'READ ON FILE' \
    && echo "File target: UNEXPECTED source privilege [$file_denial]" || echo 'File target: no source privilege named'

# With both tables granted, every function works as before.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON $db.ts_samples TO $user"
echo 'select with SELECT on both tables'
${CLIENT_USER} -q "SELECT id, value FROM timeSeriesSamples($db.ts) ORDER BY id FORMAT TSV"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, SHOW COLUMNS ON $db.ts_tags TO $user"
echo 'selector, prometheusQuery and the persistent tables over them'
${CLIENT_USER} <<EOF
SELECT count() FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999);
SELECT value FROM prometheusQuery($db.ts, '1 + 2', 1000) FORMAT TSV;
SELECT count() FROM $db.sel_dst;
SELECT value FROM $db.pq_dst FORMAT TSV;
DESCRIBE timeSeriesSamples($db.ts) SETTINGS describe_include_virtual_columns = 1 FORMAT Null;
EOF

# A row policy on the TimeSeries table cannot be enforced on the target table's rows, so the read fails
# closed while the policy exists, and works again once it is dropped. The two siblings hold that contract
# in their own read paths, so each is refused on its own; both tables are read successfully just above.
${CLICKHOUSE_CLIENT} -q "CREATE ROW POLICY $policy ON $db.ts FOR SELECT USING 0 TO $user"
${CLIENT_USER} <<EOF
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM $db.sel_dst FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM $db.pq_dst FORMAT Null; -- { serverError ACCESS_DENIED }
EOF
${CLICKHOUSE_CLIENT} -q "DROP ROW POLICY $policy ON $db.ts"
echo 'select again after the row policy is dropped'
${CLIENT_USER} <<EOF
SELECT id, value FROM timeSeriesSamples($db.ts) ORDER BY id FORMAT TSV;
-- The write direction needs INSERT on both tables. Holding SELECT on the target and INSERT on the
-- TimeSeries table only is not enough, which is what separates the two directions.
INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7); -- { serverError ACCESS_DENIED }
EOF
${CLICKHOUSE_CLIENT} -q "GRANT INSERT ON $db.ts_samples TO $user"
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(2), toDateTime64('2026-01-01 00:00:02.000', 3), toFloat64(7)"
echo 'rows after the insert'
${CLICKHOUSE_CLIENT} <<EOF
SELECT count() FROM $db.ts_samples;
-- and symmetrically for the TimeSeries table: holding INSERT on the target alone does not write either.
REVOKE INSERT ON $db.ts FROM $user;
GRANT SELECT, INSERT, SHOW COLUMNS ON $db.ts_gone TO $user;
GRANT SHOW COLUMNS ON $db.ts_tags_gone TO $user;
EOF
${CLIENT_USER} -q "INSERT INTO FUNCTION timeSeriesSamples($db.ts) SELECT toUInt64(3), toDateTime64('2026-01-01 00:00:03.000', 3), toFloat64(9); -- { serverError ACCESS_DENIED }"

# Whether a target exists is that target's own metadata, so a caller holding the TimeSeries table but not
# the target is refused before the catalog is consulted, and cannot tell a missing target from a hidden one.
# Nothing here ever grants on samples_gone or tags_gone, which are the dropped targets.
${CLIENT_USER} <<EOF
DESCRIBE timeSeriesSamples($db.ts_gone) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM timeSeriesSamples($db.ts_gone) FORMAT Null; -- { serverError ACCESS_DENIED }
INSERT INTO FUNCTION timeSeriesSamples($db.ts_gone) SELECT toUInt64(1), toDateTime64('2026-01-01 00:00:01.000', 3), toFloat64(1); -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesSelector($db.ts_tags_gone, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# A caller entitled to the target still learns that it is missing: the check narrows what is disclosed
# rather than hiding breakage from whoever may see it.
${CLICKHOUSE_CLIENT} -q "GRANT SHOW COLUMNS ON $db.samples_gone TO $user"
${CLIENT_USER} -q "DESCRIBE timeSeriesSamples($db.ts_gone) FORMAT Null; -- { serverError UNKNOWN_TABLE }"

# These three functions hand back a pre-existing table that stores data on disk, which cannot back a
# persistent table.
${CLIENT_TS} -q "CREATE TABLE $db.dst AS timeSeriesSamples($db.ts); -- { serverError BAD_ARGUMENTS }"
echo 'the target table still exists under its own name'
${CLICKHOUSE_CLIENT} <<EOF
EXISTS $db.ts_samples;
DROP USER IF EXISTS $user;
EOF
