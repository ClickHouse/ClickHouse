CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        WITH toString(user_id) as user
        SELECT user
        ORDER BY user_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        WITH toString(user_id) as user, toString(item_id) as item
        SELECT user, item
        ORDER BY user_id, item_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT _part_offset ORDER BY item_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT _part_offset, user_id ORDER BY item_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT _part_offset ORDER BY user_id, item_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT user_id GROUP BY user_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;

CREATE TEMPORARY TABLE test
(
    `user_id` UInt64,
    `item_id` UInt64,
    PROJECTION order_by_item_id
    (
        SELECT user_id, item_id GROUP BY user_id, item_id
    )
)
ENGINE = MergeTree ORDER BY ();
SHOW CREATE TEMPORARY test FORMAT LineAsString;
DROP TABLE test;
