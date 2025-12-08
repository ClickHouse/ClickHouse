DROP TABLE IF EXISTS alias_insert_test;

CREATE TABLE alias_insert_test
(
    -- Base physical columns
    `id` UInt64,
    `name` String,
    `value` Float64,

    -- Simple ALIAS columns (should be insertable with the new feature)
    `UserID` ALIAS `id`,
    `UserName` ALIAS `name`,

    -- Complex ALIAS columns (should remain non-insertable)
    `UpperName` ALIAS upper(name),
    `ValuePlusOne` ALIAS value + 1.0
)
ENGINE = MergeTree()
ORDER BY id;


INSERT INTO alias_insert_test (id, name, value) VALUES (0, 'zeno', 100.5);
INSERT INTO alias_insert_test (UserID, UserName, value) VALUES (1, 'alice', 10.5);

INSERT INTO alias_insert_test (id, UserName, value) VALUES (2, 'bob', 20.5);
INSERT INTO alias_insert_test (UserID, name, value) VALUES (3, 'charlie', 30.5);

SET async_insert = 1;
SET wait_for_async_insert = 1;

INSERT INTO alias_insert_test (UserID, UserName, value) VALUES (5, 'david_async', 500.0);
INSERT INTO alias_insert_test (id, UserName, value) VALUES (6, 'eve_async', 600.5);

SET async_insert = 0;

INSERT INTO alias_insert_test (id, UpperName, value) VALUES (99, 'FAIL', 999.0); -- { serverError NO_SUCH_COLUMN_IN_TABLE }
INSERT INTO alias_insert_test (id, name, ValuePlusOne) VALUES (99, 'FAIL', 999.0); -- { serverError NO_SUCH_COLUMN_IN_TABLE }

SELECT id, name, value FROM alias_insert_test ORDER BY id;

DROP TABLE IF EXISTS alias_insert_test;