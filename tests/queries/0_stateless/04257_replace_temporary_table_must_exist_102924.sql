-- Regression for https://github.com/ClickHouse/ClickHouse/issues/102924
-- Bare `REPLACE TEMPORARY TABLE` on a non-existent table must fail with UNKNOWN_TABLE,
-- matching `REPLACE TABLE` semantics. `CREATE OR REPLACE TEMPORARY TABLE` must keep
-- working in either state.

DROP TEMPORARY TABLE IF EXISTS __tmp_04257;

-- BUG: silently succeeded before the fix; must throw now.
REPLACE TEMPORARY TABLE __tmp_04257 (n UInt32) AS SELECT * FROM numbers(5); -- { serverError UNKNOWN_TABLE }

-- CREATE OR REPLACE on non-existent: creates.
CREATE OR REPLACE TEMPORARY TABLE __tmp_04257 (n UInt32) AS SELECT * FROM numbers(3);
SELECT n FROM __tmp_04257 ORDER BY n;

-- CREATE OR REPLACE on existing: replaces.
CREATE OR REPLACE TEMPORARY TABLE __tmp_04257 (n UInt32) AS SELECT * FROM numbers(7);
SELECT count() FROM __tmp_04257;

-- REPLACE on existing: replaces (this path stays working).
REPLACE TEMPORARY TABLE __tmp_04257 (n UInt32) AS SELECT * FROM numbers(9);
SELECT count() FROM __tmp_04257;

DROP TEMPORARY TABLE __tmp_04257;

-- REPLACE again after DROP must fail.
REPLACE TEMPORARY TABLE __tmp_04257 (n UInt32) AS SELECT * FROM numbers(5); -- { serverError UNKNOWN_TABLE }
