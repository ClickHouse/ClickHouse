-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET allow_experimental_statistics = 1;
SET allow_statistics_optimize = 1;

CREATE TABLE tab (c Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple();
INSERT INTO tab (c) VALUES (1);
DELETE FROM tab WHERE TRUE;
INSERT INTO tab (c) VALUES (2);
ALTER TABLE tab ADD STATISTICS c TYPE countmin;
OPTIMIZE TABLE tab;
SELECT 1 FROM tab WHERE tab.c = 0;
