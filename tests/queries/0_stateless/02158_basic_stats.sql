DROP TABLE IF EXISTS prewhere;

SET optimize_move_to_prewhere = 1;

CREATE TABLE prewhere
(
    a Int,
    b Int,
    c Int,
    heavy String,
    heavy2 String,
    STAT a_st a TYPE tdigest,
    STAT b_st b TYPE tdigest,
    STAT c_st c TYPE tdigest
)
ENGINE=MergeTree() ORDER BY a;

INSERT INTO prewhere VALUES (1, 1, 1, 'texttexttext', 'texttexttext');
INSERT INTO prewhere VALUES (10, 100, 0, 'texttexttext', 'texttexttext');
INSERT INTO prewhere VALUES (1, 1, 1, 'texttexttext', 'texttexttext');

INSERT INTO prewhere SELECT
    number AS a,
    number + 10 AS b,
    number % 10 AS c,
    format('test {} test {}', toString(number), toString(number + 10)) AS heavy,
    format('text {} tafst{}afsd', toString(cityHash64(number)), toString(cityHash64(number))) AS heavy2
FROM system.numbers
LIMIT 1000000


SELECT a, b, c, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0;
EXPLAIN SYNTAX SELECT a, b, c, heavy, heavy2 FROM prewhere WHERE a == 10 AND b == 100 AND c == 0;

DROP TABLE prewhere;
