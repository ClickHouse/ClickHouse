
DROP TABLE IF EXISTS order_by_key_filter_test_local;

CREATE TABLE order_by_key_filter_test_local
(
    `shop_id` Nullable(Int64),
    `attribute_date` String,
    `id` Int64,
    `date` Date
)
ENGINE = MergeTree
PARTITION BY date
ORDER BY (attribute_date, date, intHash64(id))
SETTINGS index_granularity=3;

insert into order_by_key_filter_test_local select 1 as shop_id, toDate('2023-12-01') + number as attribute_date, number as id, '2023-12-15' as date from (select number from  system.numbers limit 15);

--- fixed case: string as input argument of toDayOfWeek
SELECT
    attribute_date,
    toDayOfWeek(attribute_date) AS wk,
    count()
FROM order_by_key_filter_test_local
WHERE (date = '2023-12-15') AND (attribute_date = '2023-12-12') AND (toDayOfWeek(attribute_date) = 2)
GROUP BY
    attribute_date, wk
ORDER BY
    attribute_date ASC, wk ASC;

--- normal case: cast String to Date, it's ok as the input argument of toDayOfWeek
SELECT
    attribute_date,
    toDayOfWeek(attribute_date) AS wk,
    count()
FROM order_by_key_filter_test_local
WHERE (date = '2023-12-15') AND (attribute_date = '2023-12-12') AND (toDayOfWeek(CAST(attribute_date, 'Date')) = 2)
GROUP BY
    attribute_date, wk
ORDER BY
    attribute_date ASC, wk ASC;


DROP TABLE order_by_key_filter_test_local;
