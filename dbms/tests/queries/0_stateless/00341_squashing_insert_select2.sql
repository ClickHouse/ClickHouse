DROP TABLE IF EXISTS test.numbers_squashed;
CREATE TABLE test.numbers_squashed (number UInt8) ENGINE = Memory;

SET min_insert_block_size_rows = 100;
SET min_insert_block_size_bytes = 0;
SET max_threads = 1;

INSERT INTO test.numbers_squashed
SELECT arrayJoin(range(10)) AS number
UNION ALL
SELECT arrayJoin(range(100))
UNION ALL
SELECT arrayJoin(range(10));

SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM test.numbers_squashed;

INSERT INTO test.numbers_squashed
SELECT arrayJoin(range(100)) AS number
UNION ALL
SELECT arrayJoin(range(10))
UNION ALL
SELECT arrayJoin(range(100));

SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM test.numbers_squashed;

INSERT INTO test.numbers_squashed
SELECT arrayJoin(range(10)) AS number
UNION ALL
SELECT arrayJoin(range(100))
UNION ALL
SELECT arrayJoin(range(100));

SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM test.numbers_squashed;

INSERT INTO test.numbers_squashed
SELECT arrayJoin(range(10)) AS number
UNION ALL
SELECT arrayJoin(range(10))
UNION ALL
SELECT arrayJoin(range(10))
UNION ALL
SELECT arrayJoin(range(100))
UNION ALL
SELECT arrayJoin(range(10));

SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM test.numbers_squashed;

SET min_insert_block_size_rows = 10;

INSERT INTO test.numbers_squashed
SELECT arrayJoin(range(10)) AS number
UNION ALL
SELECT arrayJoin(range(10))
UNION ALL
SELECT arrayJoin(range(10))
UNION ALL
SELECT arrayJoin(range(100))
UNION ALL
SELECT arrayJoin(range(10));

SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC, b ASC;
SELECT count() FROM test.numbers_squashed;

DROP TABLE test.numbers_squashed;
