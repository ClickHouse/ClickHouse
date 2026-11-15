DROP TABLE IF EXISTS const_node;
CREATE TABLE const_node (`v` Nullable(UInt8)) ENGINE = MergeTree ORDER BY tuple();
SYSTEM STOP MERGES const_node;
INSERT INTO const_node VALUES (1);
INSERT INTO const_node VALUES (2);
INSERT INTO const_node VALUES (3);
-- Here we have condition with a constant "materialize(255)", for which convertToFullColumnIfConst() will return underlying column w/o copying,
-- and later shrinkToFit() will be called from multiple threads on this column, and leads to UB
SELECT v FROM const_node PREWHERE and(materialize(255), *) ORDER BY v;
