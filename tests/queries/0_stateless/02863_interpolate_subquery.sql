-- https://github.com/ClickHouse/ClickHouse/issues/53640
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (i UInt32, a UInt32) ENGINE=Memory;
SELECT i, col1 FROM (
    SELECT i, a AS col1, a AS col2 FROM tab ORDER BY i WITH FILL INTERPOLATE (col1 AS col1+col2, col2)
);
DROP TABLE tab;
