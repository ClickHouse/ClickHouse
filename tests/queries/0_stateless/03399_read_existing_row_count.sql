DROP TABLE IF EXISTS lwd_test;

CREATE TABLE lwd_test (id UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY id

INSERT INTO lwd_test SELECT number FROM numbers(100);

DELETE FROM lwd_test WHERE id = 1;

SELECT count() FROM lwd_test;

SELECT read_rows FROM system.query_log WHERE query = 'SELECT count() FROM lwd_test'

DROP TABLE IF EXISTS lwd_test;
