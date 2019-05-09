USE test;
SELECT bitmapToArray(bitmapBuild([1, 2, 3, 4, 5]));
SELECT bitmapToArray(bitmapAnd(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])));
SELECT bitmapToArray(bitmapOr(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])));
SELECT bitmapToArray(bitmapXor(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])));
SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1,2,3]),bitmapBuild([3,4,5])));
SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5]));
SELECT bitmapAndCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]));
SELECT bitmapOrCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]));
SELECT bitmapXorCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]));
SELECT bitmapAndnotCardinality(bitmapBuild([1,2,3]),bitmapBuild([3,4,5]));

DROP TABLE IF EXISTS bitmap_test;
CREATE TABLE bitmap_test(pickup_date Date, city_id UInt32, uid UInt32)ENGINE = Memory;
INSERT INTO bitmap_test SELECT '2019-01-01', 1, number FROM numbers(1,50);
INSERT INTO bitmap_test SELECT '2019-01-02', 1, number FROM numbers(11,60);


SELECT groupBitmap( uid ) AS user_num FROM bitmap_test;

SELECT pickup_date, groupBitmap( uid ) AS user_num, bitmapToArray(groupBitmapState( uid )) AS users FROM bitmap_test GROUP BY pickup_date;

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapOrCardinality(day_today, day_before) AS all_users,
    bitmapAndCardinality(day_today, day_before) AS old_users,
    bitmapAndnotCardinality(day_today, day_before) AS new_users,
    bitmapXorCardinality(day_today, day_before) AS diff_users
FROM
(
 SELECT city_id, groupBitmapState( uid ) AS day_today FROM bitmap_test WHERE pickup_date = '2019-01-02' GROUP BY city_id
 )
ALL LEFT JOIN
(
 SELECT city_id, groupBitmapState( uid ) AS day_before FROM bitmap_test WHERE pickup_date = '2019-01-01' GROUP BY city_id
)
USING city_id;

SELECT
    bitmapCardinality(day_today) AS today_users,
    bitmapCardinality(day_before) AS before_users,
    bitmapCardinality(bitmapOr(day_today, day_before))ll_users,
    bitmapCardinality(bitmapAnd(day_today, day_before)) AS old_users,
    bitmapCardinality(bitmapAndnot(day_today, day_before)) AS new_users,
    bitmapCardinality(bitmapXor(day_today, day_before)) AS diff_users
FROM
(
 SELECT city_id, groupBitmapState( uid ) AS day_today FROM bitmap_test WHERE pickup_date = '2019-01-02' GROUP BY city_id
 )
ALL LEFT JOIN
(
 SELECT city_id, groupBitmapState( uid ) AS day_before FROM bitmap_test WHERE pickup_date = '2019-01-01' GROUP BY city_id
)
USING city_id;

-- bitmap state test
DROP TABLE IF EXISTS bitmap_state_test;
CREATE TABLE bitmap_state_test
(
	pickup_date Date,
	city_id UInt32,
    uv AggregateFunction( groupBitmap, UInt32 )	
)
ENGINE = AggregatingMergeTree( pickup_date, ( pickup_date, city_id ), 8192);

INSERT INTO bitmap_state_test SELECT 
    pickup_date, 
    city_id,
    groupBitmapState(uid) AS uv
FROM bitmap_test
GROUP BY pickup_date, city_id;
	
SELECT pickup_date, groupBitmapMerge(uv) AS users from bitmap_state_test group by pickup_date;

-- between column and expression test
DROP TABLE IF EXISTS bitmap_column_expr_test;
CREATE TABLE bitmap_column_expr_test
(
    t DateTime,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(t)
ORDER BY t;

INSERT INTO bitmap_column_expr_test VALUES (now(), bitmapBuild(cast([3,19,47] as Array(UInt32))));

SELECT bitmapAndCardinality( bitmapBuild(cast([19,7] as Array(UInt32))), z) from bitmap_column_expr_test;
SELECT bitmapAndCardinality( z, bitmapBuild(cast([19,7] as Array(UInt32))) ) from bitmap_column_expr_test;

select bitmapCardinality(bitmapAnd(bitmapBuild(cast([19,7] as Array(UInt32))), z )) from bitmap_column_expr_test;
select bitmapCardinality(bitmapAnd(z, bitmapBuild(cast([19,7] as Array(UInt32))))) from bitmap_column_expr_test;

DROP TABLE IF EXISTS bitmap_test;
DROP TABLE IF EXISTS bitmap_state_test;
DROP TABLE IF EXISTS bitmap_column_expr_test;

