#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest -- requires Kafka

# Regression test for proper StorageKafka shutdown
# https://github.com/ClickHouse/ClickHouse/issues/80674
#
# NOTE: this test differs from 03522_storage_kafka_shutdown_smoke, since it creates topic w/o topic group (using named collections)

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_LOCAL -nm -q "
CREATE NAMED COLLECTION kafka_config AS kafka_broker_list = '0.0.0.0:9092';

CREATE TABLE dummy
(
    raw_message String
)
ENGINE = Kafka(kafka_config)
SETTINGS kafka_topic_list = 'dummy', kafka_format = 'RawBLOB', kafka_consumers_pool_ttl_ms=500;

SELECT * FROM dummy LIMIT 1 settings stream_like_engine_allow_direct_select=1; -- { serverError 1001 }
DROP TABLE dummy;
"
