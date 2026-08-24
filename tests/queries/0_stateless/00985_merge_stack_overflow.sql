-- Tags: no-parallel
--       ^^^^^^^^^^^ otherwise you may hit TOO_DEEP_RECURSION error during querying system.columns

DROP TABLE IF EXISTS merge1;
DROP TABLE IF EXISTS merge2;

CREATE TABLE IF NOT EXISTS merge1 (x UInt64) ENGINE = Merge(currentDatabase(), '^merge\\d$');
CREATE TABLE IF NOT EXISTS merge2 (x UInt64) ENGINE = Merge(currentDatabase(), '^merge\\d$');

SELECT * FROM merge1; -- { serverError TOO_DEEP_RECURSION }
SELECT * FROM merge2; -- { serverError TOO_DEEP_RECURSION }

DROP TABLE merge1;
DROP TABLE merge2;
