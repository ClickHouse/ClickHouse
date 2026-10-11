-- Pending patches of columns that the text index does not depend on disable the direct read only in the patched parts.

SET enable_lightweight_update = 1;
SET query_plan_direct_read_from_text_index = 1;
SET use_skip_indexes = 1;
SET use_query_condition_cache = 0;
-- The distributed plan turns the direct read off.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS tab;

CREATE TABLE tab
(
    id UInt64,
    c UInt64,
    s String,
    INDEX idx s TYPE text(tokenizer = splitByNonAlpha)
)
ENGINE = MergeTree ORDER BY id
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1, apply_patches_on_merge = 0;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, 0, concat('tok', toString(number % 10), ' ', repeat('x', 100)) FROM numbers(0, 1000);
INSERT INTO tab SELECT number, 0, concat('tok', toString(number % 10), ' ', repeat('x', 100)) FROM numbers(1000, 1000);

-- A patch of a not indexed column in the first part only.
UPDATE tab SET c = 1 WHERE id < 10;

SELECT '-- patch of not indexed column';

SELECT count() > 0 FROM
(
    EXPLAIN actions = 1 SELECT sum(c) FROM tab WHERE hasToken(s, 'tok1')
) WHERE explain LIKE '%\_\_text_index%';

SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1');
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;
SELECT id, c FROM tab WHERE hasToken(s, 'tok1') AND id < 25 ORDER BY id;

-- The part without patches reads the index, not the column.
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') FORMAT Null SETTINGS log_comment = '05345_direct_read';
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') FORMAT Null SETTINGS log_comment = '05345_no_direct_read', query_plan_direct_read_from_text_index = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    (SELECT read_bytes FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05345_direct_read')
    < (SELECT read_bytes FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05345_no_direct_read');

SELECT '-- patch of indexed column';

UPDATE tab SET s = 'zebra word' WHERE id IN (1, 1001);

SELECT count() FROM tab WHERE hasToken(s, 'tok1');
SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS use_skip_indexes = 0;
SELECT count() FROM tab WHERE hasToken(s, 'zebra');
SELECT count() FROM tab WHERE hasToken(s, 'zebra') SETTINGS query_plan_direct_read_from_text_index = 0;

SELECT '-- delete-only patch';

DELETE FROM tab WHERE id = 11 SETTINGS lightweight_delete_mode = 'lightweight_update_force';

SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1');
SELECT count(), sum(c) FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;

SELECT '-- pending mutation of indexed column';

SET apply_mutations_on_fly = 1;

CREATE TABLE tab (id UInt64, s String, INDEX idx s TYPE text(tokenizer = splitByNonAlpha))
ENGINE = MergeTree ORDER BY id;

SYSTEM STOP MERGES tab;

INSERT INTO tab SELECT number, concat('tok', toString(number % 10), ' word') FROM numbers(0, 1000);
ALTER TABLE tab UPDATE s = 'zebra word' WHERE id IN (1, 11) SETTINGS mutations_sync = 0;
INSERT INTO tab SELECT number, concat('tok', toString(number % 10), ' word') FROM numbers(1000, 1000);

SELECT count() FROM tab WHERE hasToken(s, 'tok1');
SELECT count() FROM tab WHERE hasToken(s, 'tok1') SETTINGS query_plan_direct_read_from_text_index = 0;
SELECT count() FROM tab WHERE hasToken(s, 'zebra');
SELECT count() FROM tab WHERE hasToken(s, 'zebra') SETTINGS query_plan_direct_read_from_text_index = 0;

DROP TABLE tab;
