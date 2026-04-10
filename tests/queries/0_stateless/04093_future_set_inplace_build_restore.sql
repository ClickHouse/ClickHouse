-- Regression test for non-destructive `buildOrderedSetInplace`.
-- If the in-place build gives up after `build()` has prepared a plan,
-- the delayed set-building path must still be able to execute that subquery.

DROP TABLE IF EXISTS 04093_data;
DROP TABLE IF EXISTS 04093_keys;

CREATE TABLE 04093_data
(
    key String,
    value UInt8
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE 04093_keys
(
    key String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO 04093_data VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO 04093_keys VALUES ('a'), ('x');

SET use_index_for_in_with_subqueries = 1;

SET enable_analyzer = 1;
SYSTEM ENABLE FAILPOINT future_set_from_subquery_skip_inplace_build;
SELECT count() == 1
FROM 04093_data
WHERE key IN (SELECT key FROM 04093_keys);
SYSTEM DISABLE FAILPOINT future_set_from_subquery_skip_inplace_build;

SET enable_analyzer = 0;
SYSTEM ENABLE FAILPOINT future_set_from_subquery_skip_inplace_build;
SELECT count() == 1
FROM 04093_data
WHERE key IN (SELECT key FROM 04093_keys);
SYSTEM DISABLE FAILPOINT future_set_from_subquery_skip_inplace_build;

DROP TABLE 04093_keys;
DROP TABLE 04093_data;
