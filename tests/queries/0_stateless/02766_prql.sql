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
    b = s"LEFT(message, 2)"
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
SELECT * FROM aboba;
SELECT '---';

SET dialect = 'prql';

from aboba
select [ user_id, message, creation_date, metric ];

from aboba
select [non_existing_column]; -- { serverError UNKNOWN_IDENTIFIER }


from non_existing_table
select [a]; -- { serverError UNKNOWN_TABLE }
