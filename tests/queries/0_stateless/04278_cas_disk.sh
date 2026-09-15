#!/usr/bin/env bash
# Tags: no-fasttest
# ^ cas is an object-storage metadata type; keep it off the minimal fasttest image.

# Natural black-box oracle: a table on a `cas` disk must behave
# identically to a normal MergeTree table for the same data. We compare the two
# directly so the test is deterministic regardless of environment, and we also
# exercise INSERT (content-addressed write), SELECT (ref->part_id->footer->blob
# resolution), blob-level dedup of identical inserts, a merge, and DROP (removal).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --multiquery <<EOF
DROP TABLE IF EXISTS t_cas;
DROP TABLE IF EXISTS t_ref;

CREATE TABLE t_cas (a UInt64, s String, d Date)
ENGINE = MergeTree ORDER BY a
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = local,
    metadata_type = cas,
    cas_server_root_id = '${CLICKHOUSE_DATABASE}_04278',
    name = '${CLICKHOUSE_DATABASE}_04278_cas',
    path = '${CLICKHOUSE_DATABASE}_04278_cas_pool/');

CREATE TABLE t_ref (a UInt64, s String, d Date)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_cas SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000);
INSERT INTO t_ref SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000);

SELECT 'basic', count(), sum(a), uniqExact(s), min(d), max(d) FROM t_cas;

SELECT 'oracle_full_match',
       (SELECT groupArray((a, s, d)) FROM (SELECT * FROM t_cas ORDER BY a))
     = (SELECT groupArray((a, s, d)) FROM (SELECT * FROM t_ref ORDER BY a));

-- Second identical insert: rows double (plain MergeTree does not dedup rows);
-- the content blobs are deduplicated internally by content-addressing.
INSERT INTO t_cas SELECT number, toString(number % 7), toDate('2020-01-01') + number FROM numbers(1000);
SELECT 'after_second_insert', count() FROM t_cas;

OPTIMIZE TABLE t_cas FINAL;
SELECT 'after_merge', count(), sum(a) FROM t_cas;
SELECT 'active_parts', count() FROM system.parts
WHERE database = currentDatabase() AND table = 't_cas' AND active;

SELECT 'slice', a, s, count() FROM t_cas WHERE a IN (0, 500, 999) GROUP BY a, s ORDER BY a;

DROP TABLE t_cas;
DROP TABLE t_ref;
SELECT 'dropped_ok';

-- FORGET logs an operator WARNING (the decommission is deliberately prominent in the server log); the
-- clickhouse-test harness runs the client at --send_logs_level=warning, which would stream that expected
-- warning to stderr and be flagged as a failure. Suppress it for the FORGET call only.
SET send_logs_level = 'fatal';
SYSTEM CAS FORGET '${CLICKHOUSE_DATABASE}_04278_cas';
EOF
