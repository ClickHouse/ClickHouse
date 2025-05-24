DROP TABLE IF EXISTS lwd_test;
DROP TABLE IF EXISTS lwd_test_1;

CREATE TABLE lwd_test (id UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY id;
CREATE TABLE lwd_test_1 (id UInt64 CODEC(NONE))
    ENGINE = MergeTree ORDER BY id
    SETTINGS exclude_deleted_rows_for_part_size_in_merge = 1, load_existing_rows_count_for_old_parts = 1;

INSERT INTO lwd_test SELECT number FROM numbers(20000);
INSERT INTO lwd_test_1 SELECT number FROM numbers(20000);

DELETE FROM lwd_test WHERE id = 0;
DELETE FROM lwd_test_1 WHERE id = 0;

SELECT count() FROM lwd_test;
SELECT count() FROM lwd_test_1;

SYSTEM FLUSH LOGS query_log;

SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query = 'SELECT count() FROM lwd_test;' AND type = 'QueryFinish';
SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query = 'SELECT count() FROM lwd_test_1;' AND type = 'QueryFinish';

SELECT read_rows < (
    SELECT read_rows FROM system.query_log WHERE current_database = currentDatabase() AND query ilike '%SELECT count() FROM lwd_test;%' AND type = 'QueryFinish'
) FROM system.query_log WHERE current_database = currentDatabase() AND query ilike '%SELECT count() FROM lwd_test_1;%' AND type = 'QueryFinish';

DROP TABLE IF EXISTS lwd_test;
DROP TABLE IF EXISTS lwd_test_1;
