#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# no-fasttest: the timeSeriesSelector() and prometheusQuery() arms parse PromQL, which needs ANTLR4.
# no-replicated-database: on a replicated database the DDL runs with no user, so an access check
#                         asserted here is a no-op and the deny arms silently pass (issue #111561).

# The second half of 05045: a persistent table built over one of these functions, and the grants that
# are checked on a target table rather than on the TimeSeries table itself.

# Statements are batched per client on purpose: these files' runtime is client startup, not query work,
# and holding both halves in one file runs past the 180-second limit under asan+ubsan.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

user="user05137_${CLICKHOUSE_DATABASE}_$RANDOM"
db=${CLICKHOUSE_DATABASE}

CLIENT_TS="${CLICKHOUSE_CLIENT} --allow_experimental_time_series_table 1"
CLIENT_USER="${CLICKHOUSE_CLIENT} --user $user"

# Only the tables the arms below read, and a user holding nothing on them: every grant an arm needs, it
# makes itself, so that what each one proves is visible where it is written.
${CLIENT_TS} <<EOF
CREATE TABLE $db.ts_samples (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE $db.ts_tags (id UInt64, metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.ts_metrics (metric_family_name String, type String, unit String, help String)
    ENGINE = ReplacingMergeTree ORDER BY metric_family_name;
CREATE TABLE $db.ts ENGINE = TimeSeries DATA $db.ts_samples TAGS $db.ts_tags METRICS $db.ts_metrics;
INSERT INTO $db.ts_samples VALUES (1, '2026-01-01 00:00:00.000', 42), (2, '2026-01-01 00:00:02.000', 7);

CREATE TABLE $db.ts_samples_hidden (id UInt64, timestamp DateTime64(3), value Float64)
    ENGINE = MergeTree ORDER BY (id, timestamp);
CREATE TABLE $db.ts_samples_alias ENGINE = Alias('$db', 'ts_samples_hidden');
CREATE TABLE $db.ts_via_alias ENGINE = TimeSeries
    DATA $db.ts_samples_alias TAGS $db.ts_tags METRICS $db.ts_metrics;

DROP USER IF EXISTS $user;
CREATE USER $user;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user;
EOF

# A persistent table over these functions authorizes its TimeSeries table on each read, and whether that
# table is still the one the persistent table was built over is covered by the grant on it too: the reader
# granted on the persistent table alone is refused the same way once the TimeSeries table is dropped, or
# renamed so that only its stored id resolves. Both storages keep that order in their own read path.
${CLIENT_TS} <<EOF
CREATE TABLE $db.ts_src_gone ENGINE = TimeSeries
    DATA $db.ts_samples TAGS $db.ts_tags METRICS $db.ts_metrics;
CREATE TABLE $db.ts_src_ren ENGINE = TimeSeries
    DATA $db.ts_samples TAGS $db.ts_tags METRICS $db.ts_metrics;
CREATE TABLE $db.sel_src_gone AS timeSeriesSelector($db.ts_src_gone, 'up', 0, 9999999999);
CREATE TABLE $db.pq_src_gone AS prometheusQuery($db.ts_src_gone, '1 + 2', 1000);
CREATE TABLE $db.sel_src_ren AS timeSeriesSelector($db.ts_src_ren, 'up', 0, 9999999999);
GRANT SELECT ON $db.sel_src_gone TO $user;
GRANT SELECT ON $db.pq_src_gone TO $user;
GRANT SELECT ON $db.sel_src_ren TO $user;
EOF
${CLIENT_USER} <<EOF
SELECT * FROM $db.sel_src_gone FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM $db.pq_src_gone FORMAT Null; -- { serverError ACCESS_DENIED }
EOF
${CLICKHOUSE_CLIENT} <<EOF
DROP TABLE $db.ts_src_gone;
RENAME TABLE $db.ts_src_ren TO $db.ts_src_ren_new;
EOF
${CLIENT_USER} <<EOF
SELECT * FROM $db.sel_src_gone FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM $db.pq_src_gone FORMAT Null; -- { serverError ACCESS_DENIED }
EOF
# Separately, so that a regression here is reported as its own failure rather than skipped behind one above.
${CLIENT_USER} -q "SELECT * FROM $db.sel_src_ren FORMAT Null; -- { serverError ACCESS_DENIED }"
# and a reader entitled to it is told what became of it instead.
${CLICKHOUSE_CLIENT} -q "GRANT SELECT ON $db.ts_src_gone TO $user"
${CLIENT_USER} -q "SELECT * FROM $db.sel_src_gone FORMAT Null; -- { serverError UNKNOWN_TABLE }"
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON $db.ts_src_gone FROM $user"

# timeSeriesSelector() reads one column of the tags target, its id, so a grant on that column is enough
# for it: what the generated query reads is authorized by that query, per column, which the last arm here
# pins by revoking a single column the query needs. No SHOW COLUMNS is granted on the tags target below;
# SELECT on a column implies it for that column.
user_tags_cols="user05045c_${CLICKHOUSE_DATABASE}_$RANDOM"
${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_tags_cols;
CREATE USER $user_tags_cols;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_tags_cols;
GRANT SELECT, SHOW COLUMNS ON $db.ts TO $user_tags_cols;
GRANT SELECT, SHOW COLUMNS ON $db.ts_samples TO $user_tags_cols;
GRANT SELECT(id, metric_name, tags, min_time, max_time) ON $db.ts_tags TO $user_tags_cols;
EOF

echo 'selector with a column-scoped grant on the tags target'
${CLICKHOUSE_CLIENT} --user "$user_tags_cols" <<EOF
DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT TSV;
SELECT count() FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT TSV;
EOF

${CLICKHOUSE_CLIENT} -q "REVOKE SELECT(min_time) ON $db.ts_tags FROM $user_tags_cols"
echo 'and again with one of those columns revoked'
${CLICKHOUSE_CLIENT} --user "$user_tags_cols" <<EOF
SELECT count() FROM timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
-- Describing it reads no row of the tags target, so it still works.
DESCRIBE timeSeriesSelector($db.ts, 'up', 0, 9999999999) FORMAT TSV;
EOF
${CLICKHOUSE_CLIENT} -q "DROP USER $user_tags_cols"

# The functions that hand the target table back whole check it whole, so a column-scoped grant on it is
# not enough for them, however many columns it names: it reads that column directly and gets no further.
user_target_col="user05045t_${CLICKHOUSE_DATABASE}_$RANDOM"
${CLICKHOUSE_CLIENT} <<EOF
DROP USER IF EXISTS $user_target_col;
CREATE USER $user_target_col;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_target_col;
GRANT SELECT, SHOW COLUMNS ON $db.ts TO $user_target_col;
GRANT SELECT(id, timestamp, value) ON $db.ts_samples TO $user_target_col;
EOF
echo 'ids read directly with a column-scoped grant on the samples target'
${CLICKHOUSE_CLIENT} --user "$user_target_col" <<EOF
SELECT id FROM $db.ts_samples ORDER BY id FORMAT TSV;
DESCRIBE timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
SELECT * FROM timeSeriesSamples($db.ts) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF
${CLICKHOUSE_CLIENT} -q "DROP USER $user_target_col"

# The same holds behind an `Alias` target, where the alias leg carries the rule: this caller holds the
# alias whole and the table behind it column by column, so it reads that column through the alias and
# is still refused the target whole.
user_alias_col="user05045a_${CLICKHOUSE_DATABASE}_$RANDOM"
${CLICKHOUSE_CLIENT} <<EOF
INSERT INTO $db.ts_samples_hidden VALUES (5, '2026-01-01 00:00:05.000', 11);
DROP USER IF EXISTS $user_alias_col;
CREATE USER $user_alias_col;
GRANT CREATE TEMPORARY TABLE ON *.* TO $user_alias_col;
GRANT SELECT, SHOW COLUMNS ON $db.ts_via_alias TO $user_alias_col;
GRANT SELECT, SHOW COLUMNS ON $db.ts_samples_alias TO $user_alias_col;
GRANT SHOW COLUMNS, SELECT(id) ON $db.ts_samples_hidden TO $user_alias_col;
EOF
echo 'ids read through an Alias target with a column-scoped grant on the table behind it'
${CLICKHOUSE_CLIENT} --user "$user_alias_col" <<EOF
SELECT id FROM $db.ts_samples_alias ORDER BY id FORMAT TSV;
SELECT id FROM timeSeriesSamples($db.ts_via_alias) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF
${CLICKHOUSE_CLIENT} -q "DROP USER $user_alias_col"

# An `Alias` tags target carries that same column through to the table it points at. Nothing grants on
# tags_hidden until the last arm here, while the alias itself is granted throughout.
${CLIENT_TS} <<EOF
CREATE TABLE $db.tags_hidden (id UInt64, metric_name LowCardinality(String),
    tags Map(LowCardinality(String), String), min_time DateTime64(3), max_time DateTime64(3))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE $db.ts_tags_alias ENGINE = Alias('$db', 'tags_hidden');
CREATE TABLE $db.ts_via_tags_alias ENGINE = TimeSeries
    DATA $db.ts_samples TAGS $db.ts_tags_alias METRICS $db.ts_metrics;
EOF
${CLICKHOUSE_CLIENT} <<EOF
GRANT SHOW COLUMNS ON $db.ts_via_tags_alias TO $user;
GRANT SELECT(id) ON $db.ts_tags_alias TO $user;
EOF
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts_via_tags_alias, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(id) ON $db.tags_hidden TO $user"
echo 'selector over an Alias tags target, granted the id column on the alias and on its target'
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts_via_tags_alias, 'up', 0, 9999999999) FORMAT TSV" | cut -f1 | paste -sd,

# An inner target has no configured name of its own to authorize before the lookup, so there the identity
# the lookup returns is what the check covers. Nothing grants on this table's inner targets until below.
${CLIENT_TS} -q "CREATE TABLE $db.ts_inner ENGINE = TimeSeries"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, SHOW COLUMNS ON $db.ts_inner TO $user"
${CLIENT_USER} <<EOF
DESCRIBE timeSeriesSamples($db.ts_inner) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesTags($db.ts_inner) FORMAT Null; -- { serverError ACCESS_DENIED }
DESCRIBE timeSeriesSelector($db.ts_inner, 'up', 0, 9999999999) FORMAT Null; -- { serverError ACCESS_DENIED }
EOF

# An inner table is named after the UUID of the TimeSeries table it belongs to. Granting the tags one the
# single column the selector reads there describes it, as it does for an external target.
inner_tags=$(${CLICKHOUSE_CLIENT} -q "SELECT concat('.inner_id.tags.', toString(uuid)) FROM system.tables WHERE database = '$db' AND name = 'ts_inner'")
${CLICKHOUSE_CLIENT} -q "GRANT SELECT(id) ON $db.\`$inner_tags\` TO $user"
echo 'selector over an inner tags target, granted its id column'
${CLIENT_USER} -q "DESCRIBE timeSeriesSelector($db.ts_inner, 'up', 0, 9999999999) FORMAT TSV" | cut -f1 | paste -sd,

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS $user"
