-- `IN` consumes a `Set` table as a whole and the engine has no read path that could filter it, so a row
-- policy on the table cannot be applied. Such a query must fail instead of returning the rows the policy hides.

DROP TABLE IF EXISTS set_rp;
DROP TABLE IF EXISTS mt_rp;

CREATE TABLE set_rp (k UInt64) ENGINE = Set;
CREATE TABLE mt_rp (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO set_rp VALUES (1), (2);
INSERT INTO mt_rp VALUES (1), (2);

SELECT '-- without a policy the set is usable';
SELECT groupArray(number) FROM numbers(5) WHERE number IN set_rp;

CREATE ROW POLICY rp_set_rp ON set_rp USING k = 1 TO ALL;
CREATE ROW POLICY rp_mt_rp ON mt_rp USING k = 1 TO ALL;

SELECT '-- a MergeTree table on the right of IN honours its policy';
SELECT groupArray(number) FROM numbers(5) WHERE number IN mt_rp;

SELECT '-- a Set table cannot honour its policy, so the query is refused';
SELECT groupArray(number) FROM numbers(5) WHERE number IN set_rp; -- { serverError ACCESS_DENIED }
SELECT groupArray(number) FROM numbers(5) WHERE number GLOBAL IN set_rp; -- { serverError ACCESS_DENIED }
SELECT count() FROM mt_rp WHERE k IN set_rp; -- { serverError ACCESS_DENIED }
-- a mutation builds its filter through the legacy expression analyzer
ALTER TABLE mt_rp DELETE WHERE k IN set_rp; -- { serverError ACCESS_DENIED }

SELECT '-- a policy that keeps every row is not a restriction';
DROP ROW POLICY rp_set_rp ON set_rp;
CREATE ROW POLICY rp_set_rp ON set_rp USING 1 TO ALL;
SELECT groupArray(number) FROM numbers(5) WHERE number IN set_rp;
DROP ROW POLICY rp_set_rp ON set_rp;

SELECT '-- a database-wide policy covers the Set table as well';
CREATE ROW POLICY rp_db_rp ON * USING k = 1 TO ALL;
SELECT groupArray(number) FROM numbers(5) WHERE number IN mt_rp;
SELECT groupArray(number) FROM numbers(5) WHERE number IN set_rp; -- { serverError ACCESS_DENIED }
DROP ROW POLICY rp_db_rp ON *;

SELECT '-- the set is usable again once no policy applies';
SELECT groupArray(number) FROM numbers(5) WHERE number IN set_rp;

DROP ROW POLICY rp_mt_rp ON mt_rp;
DROP TABLE set_rp;
DROP TABLE mt_rp;
