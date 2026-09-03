#!/usr/bin/env bash
# The UDF name includes the test database because SQL UDFs are server-global and this test runs
# concurrently with itself in flaky-check mode.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

UDF="totime_04910_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --allow_experimental_time_time64_type 1 --use_legacy_to_time 1 --multiquery -q "
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

CREATE FUNCTION ${UDF} AS (x) -> toTime(x);

CREATE TABLE t_totime_alter_key_udf (c0 DateTime, c1 UInt32)
ENGINE = MergeTree() ORDER BY (toUInt32(${UDF}(c0)), c1) SAMPLE BY toUInt32(${UDF}(c0));

ALTER TABLE t_totime_alter_key_udf MODIFY ORDER BY (toUInt32(${UDF}(c0)), c1);
SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key_udf';

ALTER TABLE t_totime_alter_key_udf MODIFY SAMPLE BY toUInt32(${UDF}(c0));
SELECT sampling_key FROM system.tables WHERE database = currentDatabase() AND name = 't_totime_alter_key_udf';

DROP TABLE t_totime_alter_key_udf;
DROP FUNCTION ${UDF};
"
