# Tags: distributed

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -nm -q "
-- Settings attached to a view's DEFINER user survive the settings of the
-- query that triggers the view only when constrained as READONLY, and the
-- surviving value is what is sent along with distributed INSERTs.

DROP USER IF EXISTS ${CLICKHOUSE_DATABASE}_readonly, ${CLICKHOUSE_DATABASE}_writable;

-- The value must differ from the server's own (a matching value is omitted
-- from the wire) and stay below the 32G limit of the CI test profile.
-- distributed_foreground_insert is also pinned to the ci_logs_sender default
-- (0) and locked, so the readonly user cannot be forced into a synchronous
-- (foreground) send by the triggering query either.
CREATE USER ${CLICKHOUSE_DATABASE}_readonly SETTINGS max_memory_usage_for_user = 123456789 READONLY, distributed_foreground_insert = 0 READONLY;
CREATE USER ${CLICKHOUSE_DATABASE}_writable SETTINGS max_memory_usage_for_user = 123456789, distributed_foreground_insert = 0;
GRANT SELECT, INSERT ON *.* TO ${CLICKHOUSE_DATABASE}_readonly, ${CLICKHOUSE_DATABASE}_writable;

CREATE TABLE src (x UInt64) ENGINE = Null;

CREATE TABLE dst_readonly (x UInt64, mem_setting UInt64, insert_setting UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_readonly (x UInt64, mem_setting UInt64, insert_setting UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst_readonly, x);
CREATE MATERIALIZED VIEW mv_readonly TO dist_readonly
DEFINER = ${CLICKHOUSE_DATABASE}_readonly
AS SELECT x, getSetting('max_memory_usage_for_user') AS mem_setting, getSetting('distributed_foreground_insert') AS insert_setting FROM src;

CREATE TABLE dst_writable (x UInt64, mem_setting UInt64, insert_setting UInt8) ENGINE = MergeTree ORDER BY x;
CREATE TABLE dist_writable (x UInt64, mem_setting UInt64, insert_setting UInt8) ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), dst_writable, x);
CREATE MATERIALIZED VIEW mv_writable TO dist_writable
DEFINER = ${CLICKHOUSE_DATABASE}_writable
AS SELECT x, getSetting('max_memory_usage_for_user') AS mem_setting, getSetting('distributed_foreground_insert') AS insert_setting FROM src;

-- x = 1 goes to the second shard (127.0.0.2), written over the network.
-- Also try to force a synchronous send: blocked for the readonly definer
-- (still queued, needs the FLUSH below), honored for the writable one.
INSERT INTO src SETTINGS max_memory_usage_for_user = 5000000000, distributed_foreground_insert = 1 VALUES (1);

SYSTEM FLUSH DISTRIBUTED dist_readonly;
-- No need to flush dist_writable, since distributed_foreground_insert=1 is applied
SYSTEM FLUSH LOGS query_log;

-- The settings as seen inside the views.
SELECT 'readonly', x, mem_setting, insert_setting FROM dst_readonly;
SELECT 'writable', x, mem_setting, insert_setting FROM dst_writable;

-- The setting that was sent to the remote server with each INSERT.
SELECT DISTINCT
    replaceOne(arrayFirst(t -> startsWith(t, currentDatabase() || '.dst_'), tables), currentDatabase() || '.', '') AS dst,
    Settings['max_memory_usage_for_user'] AS forwarded
FROM system.query_log
WHERE event_date >= yesterday()
    AND type = 'QueryFinish'
    AND query_kind = 'Insert'
    AND NOT is_initial_query
    AND arrayExists(t -> startsWith(t, currentDatabase() || '.dst_'), tables) -- NOLINT: current_database = currentDatabase()
ORDER BY dst;

DROP TABLE mv_readonly, mv_writable, dist_readonly, dst_readonly, dist_writable, dst_writable, src;
DROP USER ${CLICKHOUSE_DATABASE}_readonly, ${CLICKHOUSE_DATABASE}_writable;
"
