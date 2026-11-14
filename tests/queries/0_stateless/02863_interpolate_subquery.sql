-- https://github.com/ClickHouse/ClickHouse/issues/53640
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (i UInt32, a UInt32) ENGINE=Memory;
SELECT i, col1 FROM (
    SELECT i, a AS col1, a AS col2 FROM tab ORDER BY i WITH FILL INTERPOLATE (col1 AS col1+col2, col2)
) SETTINGS enable_analyzer = 1;
-- With `enable_analyzer = 0` `col1` and `col2` are a single column at the point WITH FILL runs, so they
-- cannot be interpolated differently.
SELECT i, col1 FROM (
    SELECT i, a AS col1, a AS col2 FROM tab ORDER BY i WITH FILL INTERPOLATE (col1 AS col1+col2, col2)
) SETTINGS enable_analyzer = 0; -- { serverError NOT_IMPLEMENTED }
DROP TABLE tab;
