#!/usr/bin/env bash
# Tags: no-parallel
#
# Refresh-task lifecycle for local named scalars: scheduled refresh fires;
# missing-table flap preserves the last-good value with current_value_is_valid=0
# and an exception column populated; recovery clears the error.
# Async refresh => poll system.named_scalars; never sleep on a fixed deadline.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
CLICKHOUSE_CLIENT="$CLICKHOUSE_CLIENT --allow_experimental_named_scalars=1"

# Poll a query until it returns the expected output (one or more lines,
# tab-separated columns). Times out after 15 s; on timeout, prints whatever
# the last attempt returned so the diff vs. reference is informative.
wait_for() {
    local expected="$1"
    local query="$2"
    local deadline=$((SECONDS + 15))
    local actual=""
    while [ "$SECONDS" -lt "$deadline" ]; do
        actual=$(${CLICKHOUSE_CLIENT} -q "$query" 2>/dev/null)
        if [ "$actual" = "$expected" ]; then
            echo "$expected"
            return 0
        fi
        sleep 0.25
    done
    echo "$actual"
    return 1
}

${CLICKHOUSE_CLIENT} -m -q "
DROP NAMED SCALAR IF EXISTS cv_refresh;
DROP NAMED SCALAR IF EXISTS cv_flap;
DROP TABLE IF EXISTS default.cv_flap_src;
"

# -------- cv_refresh: scheduled refresh fires and the row appears valid --------
${CLICKHOUSE_CLIENT} -m -q "
CREATE NAMED SCALAR cv_refresh REFRESH EVERY 1 SECOND AS SELECT now();
SYSTEM REFRESH NAMED SCALAR cv_refresh;
"
wait_for "local	cv_refresh	1	1" "
SELECT kind, name, has_value, current_value_is_valid
FROM system.named_scalars
WHERE kind = 'local' AND name = 'cv_refresh'
ORDER BY name
"

# -------- cv_flap: backing table flaps; last-good preserved across the gap --------
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE default.cv_flap_src (x UInt8) ENGINE = Memory;
INSERT INTO default.cv_flap_src VALUES (7);
CREATE NAMED SCALAR cv_flap REFRESH EVERY 36500 DAYS AS (SELECT count() FROM default.cv_flap_src);
"
${CLICKHOUSE_CLIENT} -q "SELECT getNamedScalar('cv_flap')"

# Drop the table; force a refresh; the scalar enters a stale-with-error state
# while keeping the previous good value visible.
${CLICKHOUSE_CLIENT} -m -q "
DROP TABLE default.cv_flap_src;
SYSTEM REFRESH NAMED SCALAR cv_flap;
"
wait_for "1	0	1	1" "
SELECT has_value, current_value_is_valid, coalesce(exception, '') != '' AS has_error,
       last_success_time > toDateTime('1970-01-01 00:00:00', 'UTC') AS kept_prior_success
FROM system.named_scalars
WHERE kind = 'local' AND name = 'cv_flap'
"
# During the outage, getNamedScalar still serves the last-good value.
${CLICKHOUSE_CLIENT} -q "SELECT getNamedScalar('cv_flap')"

# Recovery: re-create the table; force a refresh; current_value_is_valid
# returns to 1 and exception is cleared.
${CLICKHOUSE_CLIENT} -m -q "
CREATE TABLE default.cv_flap_src (x UInt8) ENGINE = Memory;
INSERT INTO default.cv_flap_src VALUES (1), (2);
SYSTEM REFRESH NAMED SCALAR cv_flap;
"
wait_for "1	1	1" "
SELECT has_value, current_value_is_valid, coalesce(exception, '') = '' AS cleared_error
FROM system.named_scalars
WHERE kind = 'local' AND name = 'cv_flap'
"
${CLICKHOUSE_CLIENT} -q "SELECT getNamedScalar('cv_flap')"

${CLICKHOUSE_CLIENT} -m -q "
DROP NAMED SCALAR cv_refresh;
DROP NAMED SCALAR cv_flap;
DROP TABLE IF EXISTS default.cv_flap_src;
"
