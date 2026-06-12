#!/usr/bin/env bash
# Tags: no-replicated-database
# Tag no-replicated-database: relies on a single server's user_files directory.

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106474.
# The reproduction needs the same client session to (a) pre-warm the
# `LowCardinality(DateTime)` cache entry under one effective timezone and then
# (b) write a CSV under a different `session_timezone`. Earlier in-memory
# variants did not catch the bug because the cache was warmed inside the same
# query that wrote the CSV, so the test runs as a `.sh` script using the
# canonical `INSERT INTO TABLE FUNCTION file(...)` write path that was reported
# in the issue.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

prefix=${CLICKHOUSE_TEST_UNIQUE_NAME}
LC_THIMPHU="${USER_FILES_PATH}/${prefix}_lc_thimphu.csv"
DT_THIMPHU="${USER_FILES_PATH}/${prefix}_dt_thimphu.csv"
LC_TOKYO="${USER_FILES_PATH}/${prefix}_lc_tokyo.csv"
DT_TOKYO="${USER_FILES_PATH}/${prefix}_dt_tokyo.csv"

cleanup() {
    rm -f "${LC_THIMPHU}" "${DT_THIMPHU}" "${LC_TOKYO}" "${DT_TOKYO}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} -nm --query "
DROP TABLE IF EXISTS t_106474_lc;
DROP TABLE IF EXISTS t_106474_dt;

SET allow_suspicious_low_cardinality_types = 1;
SET engine_file_truncate_on_insert = 1;

CREATE TABLE t_106474_lc (c0 LowCardinality(DateTime)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t_106474_dt (c0 DateTime) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_106474_lc (c0) VALUES ('2026-06-01 10:00:00');
INSERT INTO t_106474_dt (c0) SELECT c0 FROM t_106474_lc;

-- Pre-warm the LowCardinality(DateTime) pool entry under the connection's
-- effective timezone. Without the fix this freezes the cached nested
-- SerializationDateTime so the next CSV write ignores session_timezone.
SELECT c0 FROM t_106474_lc FORMAT TSV;

SET session_timezone = 'Asia/Thimphu';
INSERT INTO TABLE FUNCTION file('${prefix}_lc_thimphu.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('${prefix}_dt_thimphu.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

-- Switch to a second timezone in the same connection. With the buggy pool
-- key the cached SerializationLowCardinality(DateTime) still held the first
-- timezone, so this write would emit the same wall-clock string as the
-- previous one. With the fixed pool key each effective timezone gets its
-- own cache entry.
SET session_timezone = 'Asia/Tokyo';
INSERT INTO TABLE FUNCTION file('${prefix}_lc_tokyo.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('${prefix}_dt_tokyo.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

DROP TABLE t_106474_lc;
DROP TABLE t_106474_dt;
"

# Both tables held the same unix timestamp, so under any session_timezone the
# CSV output for LowCardinality(DateTime) and DateTime must be identical.
echo -n 'thimphu-bytes-equal	'
if cmp -s "${LC_THIMPHU}" "${DT_THIMPHU}"; then echo 1; else echo 0; fi
echo -n 'tokyo-bytes-equal	'
if cmp -s "${LC_TOKYO}" "${DT_TOKYO}"; then echo 1; else echo 0; fi
