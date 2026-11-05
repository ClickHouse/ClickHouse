-- Regression test for EnumValues::containsAll: a literal enum name that happens
-- to be a numeric string (e.g. '1') must not be reinterpreted as the numeric
-- value 1 during enum compatibility checks. See https://github.com/ClickHouse/ClickHouse/pull/95668
--
-- When the column is part of the sort key, the ALTER goes through
-- `isSafeForKeyConversion`, which calls `contains`/`containsAll`. Without the
-- fix, the source name '1' is reinterpreted as the numeric value 1, so the
-- compatibility check fails and the ALTER is rejected with
-- `ALTER_OF_COLUMN_IS_FORBIDDEN`.

DROP TABLE IF EXISTS enum_contains_numeric_name;

-- `x` is in the sort key. The source enum has a name '1' bound to value 2.
CREATE TABLE enum_contains_numeric_name
(
    x Enum8('1' = 2)
)
ENGINE = MergeTree
ORDER BY x;

INSERT INTO enum_contains_numeric_name VALUES ('1');

-- The target enum has names 'A' = 1, 'B' = 2. The source's name '1' is absent
-- from the target, but the value 2 is present, so this widening is compatible
-- and must be allowed for the key column.
ALTER TABLE enum_contains_numeric_name MODIFY COLUMN x Enum8('A' = 1, 'B' = 2);

SELECT * FROM enum_contains_numeric_name;

DROP TABLE enum_contains_numeric_name;
