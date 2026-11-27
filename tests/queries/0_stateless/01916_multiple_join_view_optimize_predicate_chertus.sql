DROP TABLE IF EXISTS a;
DROP TABLE IF EXISTS j;

CREATE TABLE a(`id` UInt32, `val` UInt32) ENGINE = Memory;
CREATE TABLE j(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id);

INSERT INTO a VALUES (1,1)(2,2)(3,3);
INSERT INTO j VALUES (2,2)(4,4);

SELECT * FROM a ANY LEFT OUTER JOIN j USING id ORDER BY a.id, a.val SETTINGS enable_optimize_predicate_expression = 1;
SELECT * FROM a ANY LEFT OUTER JOIN j USING id ORDER BY a.id, a.val SETTINGS enable_optimize_predicate_expression = 0;

DROP TABLE a;
DROP TABLE j;

CREATE TABLE j (id UInt8, val UInt8) Engine = Join(ALL, INNER, id);
SELECT * FROM (SELECT 0 id, 1 val) _ JOIN j USING id;

DROP TABLE j;
