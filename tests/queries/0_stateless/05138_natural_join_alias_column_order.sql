DROP TABLE IF EXISTS o0;
DROP TABLE IF EXISTS o1;

-- `x` is declared between two physical columns, so a census that returns `ALIAS` columns after all
-- physical ones puts the common columns in a different order than the schema does.
CREATE TABLE o0 (a Int32, x Int32 ALIAS a + 100, b Int32) ENGINE = Memory;
CREATE TABLE o1 (a Int32, x Int32 ALIAS a + 100, b Int32) ENGINE = Memory;

INSERT INTO o0 VALUES (1, 2), (3, 4);
INSERT INTO o1 VALUES (1, 2), (3, 4);

SET enable_analyzer = 1;

-- The synthesized `NATURAL JOIN` key list follows the left side's schema order, whether that side is
-- spelled as a table or as a table function.
SELECT * FROM o0 NATURAL JOIN o1 ORDER BY ALL FORMAT TSVWithNames;
SELECT * FROM merge(currentDatabase(), '^o0$') AS m NATURAL JOIN o1 ORDER BY ALL FORMAT TSVWithNames;

DROP TABLE o1;
DROP TABLE o0;
