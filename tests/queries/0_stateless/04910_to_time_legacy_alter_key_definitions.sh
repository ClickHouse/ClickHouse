#!/usr/bin/env bash
# ALTER key definitions must use the same persisted legacy spelling as CREATE definitions.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. "${CUR_DIR}/../shell_config.sh"

${CLICKHOUSE_CLIENT} --query "
SET allow_experimental_time_time64_type = 1;
SET use_legacy_to_time = 1;

DROP TABLE IF EXISTS t_totime_alter_key;

CREATE TABLE t_totime_alter_key (c0 DateTime, c1 UInt32, v UInt32)
ENGINE = MergeTree() ORDER BY (toUInt32(toTime(c0)), c1) SAMPLE BY toUInt32(toTime(c0));

ALTER TABLE t_totime_alter_key MODIFY ORDER BY (toUInt32(toTime(c0)), c1);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key';

ALTER TABLE t_totime_alter_key MODIFY TTL c0 + INTERVAL 1 DAY GROUP BY toUInt32(toTime(c0)), c1 SET v = max(v);
SELECT extract(create_table_query, 'GROUP BY toUInt32\\(toTime[A-Za-z]*')
FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key';

ALTER TABLE t_totime_alter_key MODIFY SAMPLE BY toUInt32(toTime(c0));
SELECT sampling_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key';

DROP TABLE t_totime_alter_key;

CREATE FUNCTION f_totime_alter_key_${CLICKHOUSE_DATABASE} AS (x) -> toTime(x);

CREATE TABLE t_totime_alter_key_udf (c0 DateTime, c1 UInt32)
ENGINE = MergeTree() ORDER BY (toUInt32(f_totime_alter_key_${CLICKHOUSE_DATABASE}(c0)), c1) SAMPLE BY toUInt32(f_totime_alter_key_${CLICKHOUSE_DATABASE}(c0));

ALTER TABLE t_totime_alter_key_udf MODIFY ORDER BY (toUInt32(f_totime_alter_key_${CLICKHOUSE_DATABASE}(c0)), c1);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key_udf';

ALTER TABLE t_totime_alter_key_udf MODIFY SAMPLE BY toUInt32(f_totime_alter_key_${CLICKHOUSE_DATABASE}(c0));
SELECT sampling_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key_udf';

DROP TABLE t_totime_alter_key_udf;
DROP FUNCTION f_totime_alter_key_${CLICKHOUSE_DATABASE};

SET use_legacy_to_time = 0;
CREATE TABLE t_totime_clone_source (c0 DateTime, c1 UInt32)
ENGINE = MergeTree() ORDER BY (toUInt32(toTime(c0)), c1);
INSERT INTO t_totime_clone_source VALUES ('2026-01-01 01:02:03', 1);
SET use_legacy_to_time = 1;
CREATE TABLE t_totime_clone CLONE AS t_totime_clone_source;
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_clone';
SELECT count() FROM t_totime_clone;
DROP TABLE t_totime_clone;
DROP TABLE t_totime_clone_source;
"
