-- the reference file for this test is empty because we only carea about errors
DROP TABLE IF EXISTS tab;
-- should fail on CREATE when user tries to create INDEX on subcolumn such as .size0
CREATE TABLE tab (c0 Nested(c1 Int),
    INDEX i0 `c0.c1.size0` TYPE set(1)) ENGINE = MergeTree() ORDER BY tuple(); -- { serverError UNKNOWN_IDENTIFIER }

-- same on alter
CREATE TABLE tab (c0 Nested(c1 Int)) ENGINE = MergeTree() ORDER BY tuple();
ALTER TABLE tab ADD INDEX i0 `c0.c1.size0` TYPE set(1); -- { serverError UNKNOWN_IDENTIFIER }
DROP TABLE tab;

-- ensure that nested columns still work
CREATE TABLE tab (c0 Nested(c1 Int), INDEX i0 `c0.c1` TYPE set(1)) ENGINE = MergeTree() ORDER BY tuple();

SET exclude_materialize_skip_indexes_on_insert = 'i0';
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);

-- this used to cause an error if the index column was invalid.
-- The index column here is valid but I am leaving this since it is a weird case
DELETE FROM tab WHERE TRUE;

-- check that normal index stuff works
SET exclude_materialize_skip_indexes_on_insert = '';
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);
INSERT INTO TABLE tab (`c0.c1`) SELECT [1] FROM numbers(1);

SET mutations_sync = 2;
ALTER TABLE tab MATERIALIZE INDEX i0;

DELETE FROM tab WHERE TRUE;

DROP TABLE tab;

