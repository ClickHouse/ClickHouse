DROP TABLE IF EXISTS numbers_squashed;
CREATE TABLE numbers_squashed AS system.numbers ENGINE = StripeLog;

SET optimize_trivial_insert_select = 'false';
SET max_block_size = 10000;

SET min_insert_block_size_rows = 1000000;
SET min_insert_block_size_bytes = 0;

INSERT INTO numbers_squashed SELECT * FROM system.numbers LIMIT 10000000;
SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC;

SET min_insert_block_size_bytes = 1000000;
INSERT INTO numbers_squashed SELECT * FROM system.numbers LIMIT 10000000;
SELECT blockSize() AS b, count() / b AS c FROM numbers_squashed GROUP BY blockSize() ORDER BY c DESC;

SELECT count() FROM numbers_squashed;

DROP TABLE numbers_squashed;
