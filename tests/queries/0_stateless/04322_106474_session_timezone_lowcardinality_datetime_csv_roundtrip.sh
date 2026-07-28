#!/usr/bin/env bash

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/106474.
# The reproduction needs the same process to (a) pre-warm the
# `LowCardinality(DateTime)` cache entry under one effective timezone and then
# (b) write a CSV under a different `session_timezone`. Earlier in-memory
# variants did not catch the bug because the cache was warmed inside the same
# query that wrote the CSV, so the test exercises the canonical
# `INSERT INTO TABLE FUNCTION file(...)` write path reported in the issue.
#
# The whole repro runs inside a single private `clickhouse-local` process with
# an isolated working directory. The bug lives in the process-global
# `SerializationObjectPool`, so running against the shared stateless server made
# an earlier variant of this test flaky (other parallel tests warmed the pool);
# `clickhouse-local` keeps the pool uncontended and the result deterministic.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

workdir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.workdir"
rm -rf "${workdir}"
mkdir -p "${workdir}"

cleanup() {
    rm -rf "${workdir}"
}
trap cleanup EXIT

# `file()` resolves relative paths against the current working directory in
# clickhouse-local, so run from the isolated workdir to keep the CSV files there.
(
    cd "${workdir}"
    ${CLICKHOUSE_LOCAL} -nm --query "
SET allow_suspicious_low_cardinality_types = 1;
SET engine_file_truncate_on_insert = 1;

CREATE TABLE t_106474_lc (c0 LowCardinality(DateTime)) ENGINE = Memory;
CREATE TABLE t_106474_dt (c0 DateTime) ENGINE = Memory;

INSERT INTO t_106474_lc (c0) VALUES ('2026-06-01 10:00:00');
INSERT INTO t_106474_dt (c0) SELECT c0 FROM t_106474_lc;

-- Pre-warm the LowCardinality(DateTime) pool entry under the process's
-- effective timezone. Without the fix this freezes the cached nested
-- SerializationDateTime so the next CSV write ignores session_timezone.
SELECT c0 FROM t_106474_lc FORMAT TSV;

SET session_timezone = 'Asia/Thimphu';
INSERT INTO TABLE FUNCTION file('lc_thimphu.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('dt_thimphu.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

-- Switch to a second timezone in the same process. With the buggy pool key the
-- cached SerializationLowCardinality(DateTime) still held the first timezone, so
-- this write would emit the same wall-clock string as the previous one. With the
-- fixed pool key each effective timezone gets its own cache entry.
SET session_timezone = 'Asia/Tokyo';
INSERT INTO TABLE FUNCTION file('lc_tokyo.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('dt_tokyo.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;
"
)

# Both tables held the same unix timestamp, so under any session_timezone the
# CSV output for LowCardinality(DateTime) and DateTime must be identical.
echo -n 'thimphu-bytes-equal	'
if cmp -s "${workdir}/lc_thimphu.csv" "${workdir}/dt_thimphu.csv"; then echo 1; else echo 0; fi
echo -n 'tokyo-bytes-equal	'
if cmp -s "${workdir}/lc_tokyo.csv" "${workdir}/dt_tokyo.csv"; then echo 1; else echo 0; fi
