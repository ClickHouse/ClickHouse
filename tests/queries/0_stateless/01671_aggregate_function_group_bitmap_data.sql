CREATE TABLE group_bitmap_data_test
(
    `pickup_date` Date,
    `city_id` UInt32,
    `uid` UInt32
)
ENGINE = Memory;

INSERT INTO group_bitmap_data_test SELECT
    '2019-01-01',
    1,
    number
FROM numbers(1, 50);

INSERT INTO group_bitmap_data_test SELECT
    '2019-01-02',
    1,
    number
FROM numbers(11, 60);

INSERT INTO group_bitmap_data_test SELECT
    '2019-01-03',
    2,
    number
FROM numbers(1, 10);

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapCardinality(bitmapOr(day_today, day_before)) AS ll_users,
    bitmapCardinality(bitmapAnd(day_today, day_before)) AS old_users,
    bitmapCardinality(bitmapAndnot(day_today, day_before)) AS new_users,
    bitmapCardinality(bitmapXor(day_today, day_before)) AS diff_users
FROM
(
    SELECT
        city_id,
        groupBitmapState(uid) AS day_today
    FROM group_bitmap_data_test
    WHERE pickup_date = '2019-01-02'
    GROUP BY
        uid,
        city_id
) AS js1
ALL LEFT JOIN
(
    SELECT
        city_id,
        groupBitmapState(uid) AS day_before
    FROM group_bitmap_data_test
    WHERE pickup_date = '2019-01-01'
    GROUP BY city_id
) AS js2 USING (city_id);

DROP TABLE IF EXISTS group_bitmap_data_test;
