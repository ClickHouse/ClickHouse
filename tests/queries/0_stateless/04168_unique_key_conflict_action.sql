-- Tags: no-fasttest, no-ordinary-database, no-replicated-database, no-shared-merge-tree
-- UNIQUE KEY `unique_key_conflict_action = 'ignore'` kills an ignored row in the part it arrived in. Overwrite is 04105's.
-- Red if a re-inserted key under `abort` reports the dead copy instead of the old row.

SET enable_unique_key = 1;
SET async_insert = 0;

DROP TABLE IF EXISTS uk_ig;

CREATE TABLE uk_ig (id UInt32, v String)
ENGINE = MergeTree ORDER BY id UNIQUE KEY (id)
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, unique_key_conflict_action = 'ignore';

INSERT INTO uk_ig VALUES (1, 'a'), (2, 'b');
-- Not in key order, so the part's rows are not the block's.
INSERT INTO uk_ig VALUES (4, 'd'), (1, 'a_new'), (3, 'c');

ALTER TABLE uk_ig MODIFY SETTING unique_key_conflict_action = 'abort';
INSERT INTO uk_ig SETTINGS log_comment = 'probe_1' VALUES (1, 'x'); -- { serverError VIOLATED_CONSTRAINT }
INSERT INTO uk_ig SETTINGS log_comment = 'probe_3' VALUES (3, 'x'); -- { serverError VIOLATED_CONSTRAINT }

DETACH TABLE uk_ig;
ATTACH TABLE uk_ig;
INSERT INTO uk_ig SETTINGS log_comment = 'reattached_1' VALUES (1, 'x'); -- { serverError VIOLATED_CONSTRAINT }

SYSTEM FLUSH LOGS query_log;
SELECT 'conflict', log_comment, extract(exception, 'part \\S+ \\(row \\d+\\)')
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'ExceptionWhileProcessing' AND log_comment != ''
ORDER BY event_time_microseconds;

DROP TABLE uk_ig;
