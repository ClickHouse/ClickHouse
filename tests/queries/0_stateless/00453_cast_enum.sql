DROP TABLE IF EXISTS cast_enums;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE cast_enums
(
    type Enum8('session' = 1, 'pageview' = 2, 'click' = 3),
    date Date,
    id UInt64
) ENGINE = MergeTree(date, (type, date, id), 8192);

INSERT INTO cast_enums SELECT 'session' AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;
INSERT INTO cast_enums SELECT 2 AS type, toDate('2017-01-01') AS date, number AS id FROM system.numbers LIMIT 2;

SELECT type, date, id FROM cast_enums ORDER BY type, id;

INSERT INTO cast_enums VALUES ('wrong_value', '2017-01-02', 7); -- { clientError UNKNOWN_ELEMENT_OF_ENUM }

DROP TABLE IF EXISTS cast_enums;
