-- Tags: no-random-settings

-- Regression test for repeating a NULL Dynamic value when the source and destination
-- columns have incompatible variant layouts.

SET allow_experimental_dynamic_type = 1;

DROP TABLE IF EXISTS dynamic_left;
DROP TABLE IF EXISTS dynamic_right;

CREATE TABLE dynamic_left (key UInt8) ENGINE = Memory;
CREATE TABLE dynamic_right (key UInt8, value Dynamic(max_types = 1)) ENGINE = Memory;

INSERT INTO dynamic_left VALUES (1), (1), (1);
INSERT INTO dynamic_right VALUES (1, 42), (1, NULL);

ALTER TABLE dynamic_right MODIFY COLUMN value Dynamic(max_types = 0);

SELECT count(), countIf(dynamicType(value) = 'None')
FROM
(
    SELECT r.value AS value
    FROM dynamic_left AS l
    ALL INNER JOIN dynamic_right AS r ON l.key = r.key
    SETTINGS join_algorithm = 'full_sorting_merge'
);

DROP TABLE dynamic_left;
DROP TABLE dynamic_right;
