SET flatten_nested = 0;

DROP TABLE IF EXISTS nested_table;
CREATE TABLE nested_table (id UInt64, first Nested(a Int8, b String)) ENGINE = MergeTree() ORDER BY id;
SHOW CREATE nested_table;

SET flatten_nested = 1;

ALTER TABLE nested_table ADD COLUMN second Nested(c Int8, d String) AFTER id;
SHOW CREATE nested_table;

SET flatten_nested = 0;

ALTER TABLE nested_table ADD COLUMN third Nested(e Int8, f String) FIRST;
SHOW CREATE nested_table;

SET flatten_nested = 1;

ALTER TABLE nested_table ADD COLUMN fourth Nested(g Int8, h String);
SHOW CREATE nested_table;

DROP TABLE nested_table;
