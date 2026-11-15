#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

echo "
<clickhouse>
    <max_table_num_to_throw>1</max_table_num_to_throw>
</clickhouse>
" > $CLICKHOUSE_TEST_UNIQUE_NAME.xml

$CLICKHOUSE_LOCAL --config $CLICKHOUSE_TEST_UNIQUE_NAME.xml -m -q "
CREATE TABLE test (x UInt32) ENGINE=Memory;

SET enable_json_type = 1;

CREATE TABLE IF NOT EXISTS test2
(
    a UInt32
) ENGINE = Kafka SETTINGS kafka_broker_list = 'abc:9000',
                            kafka_topic_list = 'abc',
                            kafka_group_name = 'abc',
                            kafka_format = 'JSONEachRow'; --{serverError TOO_MANY_TABLES}
"

rm $CLICKHOUSE_TEST_UNIQUE_NAME.xml
