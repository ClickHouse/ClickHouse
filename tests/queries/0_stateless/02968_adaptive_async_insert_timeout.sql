DROP TABLE IF EXISTS async_insert_mt_test;
CREATE TABLE async_insert_mt_test (a UInt64, b Array(UInt64)) ENGINE=MergeTree() ORDER BY a;

SET async_insert_use_adaptive_busy_timeout = 1;

INSERT INTO async_insert_mt_test
    SETTINGS
        async_insert=1,
        wait_for_async_insert=1,
        async_insert_busy_timeout_min_ms=10,
        async_insert_busy_timeout_max_ms=500,
        async_insert_busy_timeout_increase_rate=1.0,
        async_insert_busy_timeout_decrease_rate=1.0
    VALUES (3, []), (1, [1, 3]), (2, [7, 8]), (4, [5, 9]), (5, [2, 6]);


INSERT INTO async_insert_mt_test
    SETTINGS
        async_insert=1,
        wait_for_async_insert=1,
        async_insert_busy_timeout_ms=500,
        async_insert_busy_timeout_min_ms=500
    VALUES (3, []), (1, [1, 3]), (2, [7, 8]), (4, [5, 9]), (5, [2, 6]);


INSERT INTO async_insert_mt_test
    SETTINGS
        async_insert=1,
        wait_for_async_insert=1,
        async_insert_busy_timeout_ms=100,
        async_insert_busy_timeout_min_ms=500
    VALUES (3, []), (1, [1, 3]), (2, [7, 8]), (4, [5, 9]), (5, [2, 6]);


INSERT INTO async_insert_mt_test
    SETTINGS
        async_insert=1,
        wait_for_async_insert=1,
        async_insert_busy_timeout_increase_rate=-1.0
    VALUES (3, []), (1, [1, 3]), (2, [7, 8]), (4, [5, 9]), (5, [2, 6]); -- { serverError INVALID_SETTING_VALUE }


INSERT INTO async_insert_mt_test
    SETTINGS
        async_insert=1,
        wait_for_async_insert=1,
        async_insert_busy_timeout_decrease_rate=-1.0
    VALUES (3, []), (1, [1, 3]), (2, [7, 8]), (4, [5, 9]), (5, [2, 6]); -- { serverError INVALID_SETTING_VALUE }


DROP TABLE IF EXISTS async_insert_mt_test;
