DROP TABLE IF EXISTS buf_no_dest_no_columns;
DROP TABLE IF EXISTS buf_no_dest_with_columns;

-- A Buffer with no destination has no structure to infer from, so an omitted column list leaves the
-- table with no columns at all and the definition has to be rejected.
CREATE TABLE buf_no_dest_no_columns ENGINE = Buffer('', '', 1, 10, 100, 10000, 1000000, 10000000, 100000000); -- { serverError EMPTY_LIST_OF_COLUMNS_PASSED }

-- Control: the same destination-less engine with an explicit structure is valid and usable, so the
-- rejection above is about the missing column list and not about the empty destination.
CREATE TABLE buf_no_dest_with_columns (x UInt64) ENGINE = Buffer('', '', 1, 10, 100, 10000, 1000000, 10000000, 100000000);
INSERT INTO buf_no_dest_with_columns VALUES (1);
SELECT x FROM buf_no_dest_with_columns;

DROP TABLE buf_no_dest_with_columns;
