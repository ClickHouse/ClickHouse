-- Regression test for preserving analyzer CTE planning on in-place set build fallback.

DROP TABLE IF EXISTS 04094_data;
DROP TABLE IF EXISTS 04094_keys;

CREATE TABLE 04094_data
(
    key String,
    value UInt8
)
ENGINE = MergeTree
ORDER BY key;

CREATE TABLE 04094_keys
(
    key String
)
ENGINE = MergeTree
ORDER BY key;

INSERT INTO 04094_data VALUES ('a', 1), ('b', 2), ('c', 3);
INSERT INTO 04094_keys VALUES ('a'), ('x');

SET use_index_for_in_with_subqueries = 1;
SET enable_analyzer = 1;

SYSTEM ENABLE FAILPOINT future_set_from_subquery_skip_inplace_build;
WITH A AS (SELECT key FROM 04094_keys)
SELECT count() == 1
FROM 04094_data
WHERE key IN (SELECT key FROM A);
SYSTEM DISABLE FAILPOINT future_set_from_subquery_skip_inplace_build;

DROP TABLE 04094_keys;
DROP TABLE 04094_data;
