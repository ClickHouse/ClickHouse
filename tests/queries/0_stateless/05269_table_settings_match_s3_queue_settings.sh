#!/usr/bin/env bash
# Tags: no-fasttest, zookeeper
# Tag justification: needs the S3Queue engine, which is an optional build, and Keeper, which holds its metadata.
#
# `system.s3_queue_settings` describes a queue table's settings too, so the two tables have to agree on every
# setting - its name, type, description and value - except where `system.table_settings` knows more, and each such
# difference is listed here, so that a new one fails the test. The two render values differently: a `Bool` as
# `true`/`false` against `1`/`0`, a float as `0.` against `0`; those are compared as values.
#
# The known differences: a format setting the definition states, which the queue table's rebuilt settings object
# never assigns, so `system.s3_queue_settings` shows the default while the engine honours the stated value; and a
# setting held in Keeper, which `system.s3_queue_settings` counts as changed only where the `CREATE` query states it.
#
# The bucket is never read, so this needs no S3 endpoint. A shell test because `keeper_path` is server-wide, so it
# carries this test's database name.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

KEEPER_PATH="/clickhouse/test_05269_${CLICKHOUSE_DATABASE}"

# Re-runnable: the flaky check runs a new test many times against the same database.
$CLICKHOUSE_CLIENT -q "DROP TABLE IF EXISTS queue_parity SYNC"

$CLICKHOUSE_CLIENT -q "
CREATE TABLE queue_parity (a String)
ENGINE = S3Queue('http://localhost:1/bucketname/data/*', 'key', 'secret', 'TSV')
SETTINGS mode = 'unordered', keeper_path = '${KEEPER_PATH}', loading_retries = 42,
    enable_logging_to_queue_log = 0, input_format_tsv_skip_first_lines = 2"

$CLICKHOUSE_CLIENT -q "
SELECT '-- the same settings, with the same type and description';
SELECT 'only in system.s3_queue_settings', name FROM (
    SELECT name FROM system.s3_queue_settings WHERE database = currentDatabase() AND table = 'queue_parity'
    EXCEPT
    SELECT name FROM system.table_settings WHERE database = currentDatabase() AND table = 'queue_parity' AND alias_for = '')
ORDER BY name;
SELECT 'only in system.table_settings', name FROM (
    SELECT name FROM system.table_settings WHERE database = currentDatabase() AND table = 'queue_parity' AND alias_for = ''
    EXCEPT
    SELECT name FROM system.s3_queue_settings WHERE database = currentDatabase() AND table = 'queue_parity')
ORDER BY name;

SELECT 'rows compared', count() > 300, countIf(q.type != t.type OR q.description != t.description)
FROM system.s3_queue_settings AS q
INNER JOIN system.table_settings AS t ON t.database = q.database AND t.table = q.table AND t.name = q.name
WHERE q.database = currentDatabase() AND q.table = 'queue_parity';

SELECT '-- values that differ, compared as values';
SELECT q.name, q.value, t.value, t.source
FROM system.s3_queue_settings AS q
INNER JOIN system.table_settings AS t ON t.database = q.database AND t.table = q.table AND t.name = q.name
WHERE q.database = currentDatabase() AND q.table = 'queue_parity'
    AND NOT multiIf(
        q.type = 'Bool', (q.value IN ('true', '1')) = (t.value IN ('true', '1')),
        q.type IN ('Float', 'Double'), toFloat64OrNull(q.value) = toFloat64OrNull(t.value),
        q.value = t.value)
ORDER BY q.name;

SELECT '-- changed that differs';
SELECT t.source, q.changed, t.changed, count() > 0
FROM system.s3_queue_settings AS q
INNER JOIN system.table_settings AS t ON t.database = q.database AND t.table = q.table AND t.name = q.name
WHERE q.database = currentDatabase() AND q.table = 'queue_parity' AND q.changed != t.changed
GROUP BY t.source, q.changed, t.changed
ORDER BY t.source;
"

$CLICKHOUSE_CLIENT -q "DROP TABLE queue_parity SYNC"
