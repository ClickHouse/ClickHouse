-- Tags: distributed

-- { echo }

DROP TABLE IF EXISTS test;
CREATE TABLE test (id UInt64) ENGINE=MergeTree ORDER BY tuple();
INSERT INTO test SELECT number FROM numbers(200000);

-- LIMIT -3 BY g (SortedStream)
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY g, id LIMIT -3 BY g;
-- LIMIT -3 BY g
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT -3 BY g;

-- LIMIT -3 OFFSET -50 BY g (SortedStream)
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY g, id LIMIT -3 OFFSET -50 BY g;
-- LIMIT -3 OFFSET -50 BY g
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT -3 OFFSET -50 BY g;

-- LIMIT -3 OFFSET 50 BY g (SortedStream)
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY g, id LIMIT -3 OFFSET 50 BY g;
-- LIMIT -3 OFFSET 50 BY g
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT -3 OFFSET 50 BY g;

-- LIMIT 3 OFFSET -50 BY g (SortedStream)
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY g, id LIMIT 3 OFFSET -50 BY g;
-- LIMIT 3 OFFSET -50 BY g
SELECT id, intDiv(id, 50000) AS g FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT 3 OFFSET -50 BY g;

-- INT_MIN LIMIT (SortedStream)
SELECT count(), sum(id) FROM (SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY intDiv(id, 50000), id LIMIT -9223372036854775808 BY intDiv(id, 50000));
-- INT_MIN LIMIT
SELECT count(), sum(id) FROM (SELECT id FROM remote('127.0.0.1,127.0.0.2', currentDatabase(), test) ORDER BY id LIMIT -9223372036854775808 BY intDiv(id, 50000));

DROP TABLE test;
