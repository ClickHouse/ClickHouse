DROP TABLE IF EXISTS mem_test;

CREATE TABLE mem_test
(
    `a` Int64,
    `b` Int64
)
ENGINE = Memory;

SET max_block_size = 3;

INSERT INTO mem_test SELECT
    number,
    number
FROM numbers(100);

ALTER TABLE mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER TABLE mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER TABLE mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER TABLE mem_test
    UPDATE a = 0 WHERE b = 99;
ALTER TABLE mem_test
    UPDATE a = 0 WHERE b = 99;

SELECT *
FROM mem_test
FORMAT Null;

DROP TABLE mem_test;
