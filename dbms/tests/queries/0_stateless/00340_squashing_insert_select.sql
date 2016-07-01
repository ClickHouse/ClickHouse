DROP TABLE IF EXISTS test.numbers_squashed;
CREATE TABLE test.numbers_squashed AS system.numbers ENGINE = StripeLog;

SET max_block_size = 10000;

SET min_insert_block_size_rows = 1000000;
SET min_insert_block_size_bytes = 0;

INSERT INTO test.numbers_squashed SELECT * FROM system.numbers LIMIT 10000000;
SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC;

SET min_insert_block_size_bytes = 1000000;
INSERT INTO test.numbers_squashed SELECT * FROM system.numbers LIMIT 10000000;
SELECT blockSize() AS b, count() / b AS c FROM test.numbers_squashed GROUP BY blockSize() ORDER BY c DESC;

SELECT count() FROM test.numbers_squashed;

DROP TABLE test.numbers_squashed;
