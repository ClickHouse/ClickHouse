SET enable_analyzer=1;

SELECT *, ngramMinHash(*) AS minhash, mortonEncode(untuple(ngramMinHash(*))) AS z
FROM (SELECT toString(number) FROM numbers(10))
ORDER BY z LIMIT 100;

CREATE TABLE test (
    idx UInt64,
    coverage Array(UInt64),
    test_name String
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO test VALUES (10, [0,1,2,3], 'xx'), (20, [3,4,5,6], 'xxx'), (90, [3,4,5,6,9], 'xxxx');

WITH
    4096 AS w, 4096 AS h, w * h AS pixels,
    arrayJoin(coverage) AS num,
    num DIV (32768 * 32768 DIV pixels) AS idx,
    mortonDecode(2, idx) AS coord,
    255 AS b,
    least(255, uniq(test_name)) AS r,
    255 * uniq(test_name) / (max(uniq(test_name)) OVER ()) AS g
SELECT r::UInt8, g::UInt8, b::UInt8
FROM test
GROUP BY coord
ORDER BY coord.2 * w + coord.1
WITH FILL FROM 0 TO 10;


CREATE TABLE seq (
    number UInt64
)
ENGINE = MergeTree
ORDER BY tuple();

INSERT INTO seq VALUES (0), (6), (7);

WITH (Select min(number), max(number) from seq) as range Select * from numbers(range.1, range.2);
