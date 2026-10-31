-- Random settings limits: optimize_move_to_prewhere=(1, 1); query_plan_optimize_prewhere=(1, 1)

-- A Merge over a text-indexed and a plain table, read with FINAL, returns the same rows whichever of them comes first.

DROP TABLE IF EXISTS merge_child_1_indexed;
DROP TABLE IF EXISTS merge_child_2_plain;
DROP TABLE IF EXISTS merge_child_3_indexed;

CREATE TABLE merge_child_1_indexed (id UInt64, doc String, INDEX idx doc TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY id;
CREATE TABLE merge_child_2_plain (id UInt64, doc String) ENGINE = MergeTree ORDER BY id;
CREATE TABLE merge_child_3_indexed (id UInt64, doc String, INDEX idx doc TYPE text(tokenizer = 'splitByNonAlpha')) ENGINE = MergeTree ORDER BY id;
INSERT INTO merge_child_1_indexed VALUES (1, 'a b'), (2, 'a b');
INSERT INTO merge_child_2_plain VALUES (1, 'a b'), (2, 'a b');
INSERT INTO merge_child_3_indexed VALUES (1, 'a b'), (2, 'a b');

SELECT count() FROM merge(currentDatabase(), '^merge_child_[12]_') FINAL WHERE hasAllTokens(doc, 'a') AND id > 0
SETTINGS optimize_move_to_prewhere_if_final = 1, use_skip_indexes_if_final = 0;
SELECT id FROM merge(currentDatabase(), '^merge_child_[12]_') FINAL WHERE hasAllTokens(doc, 'a') AND id > 0 ORDER BY id
SETTINGS optimize_move_to_prewhere_if_final = 1, use_skip_indexes_if_final = 0;
-- The same children in the other order.
SELECT count() FROM merge(currentDatabase(), '^merge_child_[23]_') FINAL WHERE hasAllTokens(doc, 'a') AND id > 0
SETTINGS optimize_move_to_prewhere_if_final = 1, use_skip_indexes_if_final = 0;

DROP TABLE merge_child_1_indexed;
DROP TABLE merge_child_2_plain;
DROP TABLE merge_child_3_indexed;
