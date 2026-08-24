DROP TABLE IF EXISTS mt;
set allow_deprecated_syntax_for_merge_tree=1;
CREATE TABLE mt (d Date, x UInt8) ENGINE = MergeTree(d, x, 8192);
INSERT INTO mt VALUES (52392, 1), (62677, 2);
DROP TABLE mt;
