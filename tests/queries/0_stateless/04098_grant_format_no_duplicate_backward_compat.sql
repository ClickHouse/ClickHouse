-- Regression test for duplicate grants in backward-compatible formatting.
-- When enable_read_write_grants=false (default), READ/WRITE grants on a named source
-- (e.g. FILE) are converted to old-style grants (e.g. FILE ON *.*).
-- Two separate elements (READ + WRITE) on the same source should be merged into
-- a single old-style grant, not duplicated.
-- https://github.com/ClickHouse/ClickHouse/issues/102325

-- READ + WRITE on a specific source should produce a single old-style grant.
SELECT formatQuery('GRANT READ, WRITE ON FILE TO x');

-- READ + WRITE on all sources should produce a single SOURCES grant.
SELECT formatQuery('GRANT READ, WRITE ON * TO x');

-- Single READ or WRITE should produce a single grant (no duplication possible).
SELECT formatQuery('GRANT READ ON FILE TO x');
SELECT formatQuery('GRANT WRITE ON S3 TO x');

-- Combined via SOURCE READ/WRITE aliases should also merge correctly.
SELECT formatQuery('GRANT SOURCE READ, SOURCE WRITE ON FILE TO x');

-- Different sources stay separate but should be grouped under one ON clause.
SELECT formatQuery('GRANT READ ON FILE, READ ON S3 TO x');

-- REVOKE follows the same formatting logic.
SELECT formatQuery('REVOKE READ, WRITE ON FILE FROM x');

-- Other source types.
SELECT formatQuery('GRANT READ, WRITE ON S3 TO x');
SELECT formatQuery('GRANT READ, WRITE ON URL TO x');
SELECT formatQuery('GRANT READ, WRITE ON HDFS TO x');

-- Column-scoped grants must NOT be deduplicated — different column lists are distinct grants.
SELECT formatQuery('GRANT SELECT(a), SELECT(b) ON db.t TO x');
SELECT formatQuery('GRANT SELECT(a, b), INSERT(c) ON db.t TO x');

-- Mixed: table-wide and column-scoped grants in the same statement.
SELECT formatQuery('GRANT SELECT, SELECT(a) ON db.t TO x');

-- Multiple column-scoped grants with the same flag on different tables stay separate.
SELECT formatQuery('GRANT SELECT(a) ON db.t1, SELECT(b) ON db.t2 TO x');
