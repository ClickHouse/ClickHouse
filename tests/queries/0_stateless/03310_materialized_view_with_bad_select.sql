SET allow_materialized_view_with_bad_select = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS mv;

CREATE TABLE src (x int, y int) ENGINE = MergeTree ORDER BY ();

CREATE TABLE dst (x int, z int) ENGINE = MergeTree ORDER BY ();

CREATE MATERIALIZED VIEW mv TO dst AS SELECT x, y FROM src;

INSERT INTO src VALUES (1, 1);

SELECT * FROM dst;

SET allow_materialized_view_with_bad_select = 0;

-- Insert into existing bad MV is still possible
INSERT INTO src VALUES (2, 2);

SELECT * FROM dst ORDER BY ALL;

-- Re-creating it is not
DROP TABLE mv;

CREATE MATERIALIZED VIEW mv TO dst AS SELECT x, y FROM src; -- { serverError THERE_IS_NO_COLUMN }

-- Creating an MV with a nonexistent target table is no longer possible
SET allow_materialized_view_with_bad_select = 1;

CREATE MATERIALIZED VIEW mv TO nonexistent AS SELECT x, y FROM src;

INSERT INTO src VALUES (3, 3); -- { serverError UNKNOWN_TABLE }

DROP TABLE mv;

SET allow_materialized_view_with_bad_select = 0;

CREATE MATERIALIZED VIEW mv TO nonexistent AS SELECT x, y FROM src; -- { serverError UNKNOWN_TABLE }

DROP TABLE src;
DROP TABLE dst;
DROP TABLE IF EXISTS mv;
