#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: uses clickhouse-local.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

set -e

# Run in a private clickhouse-local process so the global serialization pool is
# uncontended: the bug only reproduces when an entry warmed under one timezone
# is reused for output under another, and a fresh process makes that
# deterministic regardless of what other tests run concurrently on a shared server.
workdir="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.d"
rm -rf "${workdir}"
mkdir -p "${workdir}"

cleanup() {
    rm -rf "${workdir}"
}
trap cleanup EXIT

(
cd "${workdir}"
${CLICKHOUSE_LOCAL} --path . -nm --query "
SET allow_suspicious_low_cardinality_types = 1;
SET engine_file_truncate_on_insert = 1;

CREATE TABLE t_106474_lc (c0 LowCardinality(DateTime)) ENGINE = MergeTree() ORDER BY tuple();
CREATE TABLE t_106474_dt (c0 DateTime) ENGINE = MergeTree() ORDER BY tuple();

INSERT INTO t_106474_lc (c0) VALUES ('2026-06-01 10:00:00');
INSERT INTO t_106474_dt (c0) SELECT c0 FROM t_106474_lc;

-- Warm the LowCardinality(DateTime) serialization under the default timezone.
SELECT c0 FROM t_106474_lc FORMAT TSV;

SET session_timezone = 'Asia/Thimphu';
INSERT INTO TABLE FUNCTION file('lc_thimphu.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('dt_thimphu.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;

SET session_timezone = 'Asia/Tokyo';
INSERT INTO TABLE FUNCTION file('lc_tokyo.csv', 'CSV', 'c0 LowCardinality(DateTime)') SELECT c0 FROM t_106474_lc;
INSERT INTO TABLE FUNCTION file('dt_tokyo.csv', 'CSV', 'c0 DateTime') SELECT c0 FROM t_106474_dt;
"
)

# Both tables hold the same unix timestamp, so under any session_timezone the
# CSV output for LowCardinality(DateTime) and DateTime must be identical.
echo -n 'thimphu-bytes-equal	'
if cmp -s "${workdir}/lc_thimphu.csv" "${workdir}/dt_thimphu.csv"; then echo 1; else echo 0; fi
echo -n 'tokyo-bytes-equal	'
if cmp -s "${workdir}/lc_tokyo.csv" "${workdir}/dt_tokyo.csv"; then echo 1; else echo 0; fi
