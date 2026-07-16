-- A row policy filter is applied as a per-row predicate at the storage read stage. `arrayJoin`
-- changes the number of rows, which used to abort with 'column->size() == num_rows' (a logical
-- error). It must be rejected instead: when the policy is created, when it is altered, and when
-- it is applied.

DROP ROW POLICY IF EXISTS policy_with_array_join ON row_policy_table;
DROP ROW POLICY IF EXISTS valid_policy ON row_policy_table;
DROP TABLE IF EXISTS row_policy_table;

CREATE TABLE row_policy_table (id UInt32, value UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO row_policy_table VALUES (1, 10), (2, 20);

-- Creating a policy whose filter contains arrayJoin is rejected.
CREATE ROW POLICY policy_with_array_join ON row_policy_table FOR SELECT USING arrayJoin([1, 2]) OR (value = 0) TO ALL; -- { serverError ILLEGAL_PREWHERE }

-- Altering a valid policy to introduce arrayJoin is rejected too; the original filter is kept.
CREATE ROW POLICY valid_policy ON row_policy_table FOR SELECT USING value > 0 TO ALL;
ALTER ROW POLICY valid_policy ON row_policy_table FOR SELECT USING arrayJoin([1, 2]) OR (value > 0); -- { serverError ILLEGAL_PREWHERE }
SELECT * FROM row_policy_table ORDER BY id;
DROP ROW POLICY valid_policy ON row_policy_table;

-- arrayJoin scoped inside a subquery does not change the outer row count and stays allowed.
CREATE ROW POLICY valid_policy ON row_policy_table FOR SELECT USING value IN (SELECT arrayJoin([10, 20])) TO ALL;
SELECT * FROM row_policy_table ORDER BY id;

DROP ROW POLICY valid_policy ON row_policy_table;
DROP TABLE row_policy_table;
