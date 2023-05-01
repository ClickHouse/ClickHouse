-- Tags: no-fasttest

SET input_format_defaults_for_omitted_fields=1;

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    x UInt32,
    y UInt32,
    a DEFAULT x + y,
    b Float32 DEFAULT round(log(1 + x + y), 5),
    c UInt32 DEFAULT 42,
    e MATERIALIZED x + y,
    f ALIAS x + y
) ENGINE = MergeTree ORDER BY x;

INSERT INTO defaults FORMAT JSONEachRow {"x":1, "y":1};
INSERT INTO defaults (x, y) SELECT x, y FROM defaults LIMIT 1;
INSERT INTO defaults FORMAT JSONEachRow {"x":2, "y":2, "c":2};
INSERT INTO defaults FORMAT JSONEachRow {"x":3, "y":3, "a":3, "b":3, "c":3};
INSERT INTO defaults FORMAT JSONEachRow {"x":4} {"y":5, "c":5} {"a":6, "b":6, "c":6};

SELECT * FROM defaults ORDER BY (x, y);

ALTER TABLE defaults ADD COLUMN n Nested(a UInt64, b String);
INSERT INTO defaults FORMAT JSONEachRow {"x":7, "y":7, "n.a":[1,2], "n.b":["a","b"]};
SELECT * FROM defaults WHERE x = 7 FORMAT JSONEachRow;

ALTER TABLE defaults ADD COLUMN n.c Array(UInt8) DEFAULT arrayMap(x -> 0, n.a) AFTER n.a;
INSERT INTO defaults FORMAT JSONEachRow {"x":8, "y":8, "n.a":[3,4], "n.b":["c","d"]};
INSERT INTO defaults FORMAT JSONEachRow {"x":9, "y":9};
SELECT * FROM defaults WHERE x > 7 ORDER BY x FORMAT JSONEachRow;

DROP TABLE defaults;
