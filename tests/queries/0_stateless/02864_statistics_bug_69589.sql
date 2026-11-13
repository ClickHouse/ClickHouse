-- Tags: no-fasttest
-- no-fasttest: 'countmin' sketches need a 3rd party library

SET allow_statistics = 1;
SET use_statistics = 1;

CREATE TABLE tab (c Nullable(Int)) ENGINE = MergeTree() ORDER BY tuple() SETTINGS auto_statistics_types = '';
INSERT INTO tab (c) VALUES (1);
DELETE FROM tab WHERE TRUE;
INSERT INTO tab (c) VALUES (2);
ALTER TABLE tab MODIFY SETTING auto_statistics_types = 'countmin';
OPTIMIZE TABLE tab;
SELECT 1 FROM tab WHERE tab.c = 0;
