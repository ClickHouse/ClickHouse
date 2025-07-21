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

SELECT id, name, value FROM alias_insert_test ORDER BY id;

DROP TABLE IF EXISTS alias_insert_test;