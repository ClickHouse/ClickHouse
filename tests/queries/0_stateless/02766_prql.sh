#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -n -q "
CREATE TEMPORARY TABLE IF NOT EXISTS aboba
(
    user_id UInt32,
    message String,
    creation_date DateTime64,
    metric Float32
)
ENGINE = MergeTree
ORDER BY user_id;

INSERT INTO aboba (user_id, message, creation_date, metric) VALUES (101, 'Hello, ClickHouse!', toDateTime('2019-01-01 00:00:00', 3, 'Europe/Amsterdam'), -1.0), (102, 'Insert a lot of rows per batch', toDateTime('2019-02-01 00:00:00', 3, 'Europe/Amsterdam'), 1.41421 ), (102, 'Sort your data based on your commonly-used queries', toDateTime('2019-03-01 00:00:00', 3, 'Europe/Amsterdam'), 2.718), (101, 'Granules are the smallest chunks of data read', toDateTime('2019-05-01 00:00:00', 3, 'Europe/Amsterdam'), 3.14159), (103, 'This is an awesome message', toDateTime('2019-04-01 00:00:00', 3, 'Europe/Amsterdam'), 42);

SET dialect = 'prql';

from aboba
derive [
    a = 2,
    b = s\"LEFT(message, 2)\"
]
select [ user_id, message, a, b ];

from aboba
filter user_id > 101
group user_id (
    aggregate [
        metrics = sum metric
    ]
);

SET dialect = 'clickhouse';

SELECT '---';
SELECT
    user_id,
    message,
    toTimeZone(creation_date, 'Europe/Amsterdam') as creation_date,
    metric
FROM aboba;
SELECT '---';

SET dialect = 'prql';

from aboba
select [ user_id, message, metric ]
derive creation_date = s\"toTimeZone(creation_date, 'Europe/Amsterdam')\"
select [ user_id, message, creation_date, metric];
"

function check_error_code()
{
    if $CLICKHOUSE_CLIENT --dialect="prql" -q "$1" 2>&1 | grep -q "$2"
    then
        echo "OK"
    else
        echo "FAIL"
    fi
}

check_error_code "from s\"SELECT system.users\" | select non_existent_column" "UNKNOWN_IDENTIFIER"

check_error_code "from non_existent_table" "UNKNOWN_TABLE"
