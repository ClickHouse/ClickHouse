#!/usr/bin/env bash
# Tags: no-random-merge-tree-settings

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
CREATE TABLE IF NOT EXISTS ts_data_double_raw
(
   device_id UInt32 NOT NULL CODEC(ZSTD),
   data_item_id UInt32 NOT NULL CODEC(ZSTD),
   data_time DateTime64(3, 'UTC') NOT NULL CODEC(Delta, ZSTD),
   data_value Float64 NOT NULL CODEC(Delta, ZSTD),
   is_deleted Bool CODEC(ZSTD),
   ingestion_time DateTime64(3, 'UTC') NOT NULL CODEC(Delta, ZSTD)
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(data_time)
ORDER BY (device_id, data_item_id, data_time)
SETTINGS index_granularity = 8192;


CREATE VIEW ts_data_double AS
SELECT
    device_id,
    data_item_id,
    data_time,
    argMax(data_value, ingestion_time) data_value,
    max(ingestion_time) version,
    argMax(is_deleted, ingestion_time) is_deleted
FROM ts_data_double_raw
GROUP BY device_id, data_item_id, data_time
HAVING is_deleted = 0;

INSERT INTO ts_data_double_raw VALUES (100, 1, fromUnixTimestamp64Milli(1697547086760), 3.6, false, fromUnixTimestamp64Milli(1)), (100, 1, fromUnixTimestamp64Milli(1697547086761), 4.6, false, fromUnixTimestamp64Milli(1));
INSERT INTO ts_data_double_raw VALUES (100, 1, fromUnixTimestamp64Milli(1697547086760), 3.6, true, fromUnixTimestamp64Milli(5)), (100, 1, fromUnixTimestamp64Milli(1697547086761), 4.6, false, fromUnixTimestamp64Milli(4));
"

$CLICKHOUSE_CLIENT -q "select 1697547086760 format RowBinary" | ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query=INSERT%20INTO%20ts_data_double_raw%20%28device_id%2C%20data_item_id%2C%20data_time%2C%20data_value%2C%20is_deleted%2C%20ingestion_time%29%0ASELECT%0A%20%20%20device_id%2C%0A%20%20%20data_item_id%2C%0A%20%20%20data_time%2C%0A%20%20%20data_value%2C%0A%20%20%201%2C%20%20--%20mark%20as%20deleted%0A%20%20%20fromUnixTimestamp64Milli%281697547088995%2C%20%27UTC%27%29%20--%20all%20inserted%20records%20have%20new%20ingestion%20time%0AFROM%20ts_data_double%0AWHERE%20%28device_id%20%3D%20100%29%20AND%20%28data_item_id%20%3D%201%29%0A%20%20%20%20AND%20%28data_time%20%3E%3D%20fromUnixTimestamp64Milli%280%2C%20%27UTC%27%29%29%0A%20%20%20%20AND%20%28data_time%20%3C%3D%20fromUnixTimestamp64Milli%281697547086764%2C%20%27UTC%27%29%29%0A%20%20%20%20AND%20version%20%3C%20fromUnixTimestamp64Milli%281697547088995%2C%20%27UTC%27%29%0A%20%20%20%20AND%20%28toUnixTimestamp64Milli%28data_time%29%20IN%20%28SELECT%20timestamp%20FROM%20input%28%27timestamp%20UInt64%27%29%29%29%20SETTINGS%20insert_quorum%3D1%0A%20FORMAT%20RowBinary" --data-binary @-
