DROP TABLE IF EXISTS 03720_table;
CREATE TABLE 03720_table
(
    id UInt32
) ENGINE = Memory();

SET async_insert = 1, async_insert_deduplicate = 1, wait_for_async_insert = 0;
set async_insert_use_adaptive_busy_timeout=0, async_insert_busy_timeout_min_ms=1000, async_insert_busy_timeout_max_ms=5000;

INSERT INTO 03720_table VALUES (1);
INSERT INTO 03720_table VALUES (1);

SYSTEM FLUSH ASYNC INSERT QUEUE 03720_table;

-- excpect 2 rows, because Memory engine does not support deduplication
SELECT count() FROM 03720_table;
