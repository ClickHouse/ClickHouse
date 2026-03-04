-- limit > 64 throws INVALID_SETTING_VALUE.
-- BitSet is fixed-width (64 bits) so limits above 64 are rejected at
-- planning time before any join enumeration begins.

SET allow_experimental_analyzer = 1;
SET enable_parallel_replicas = 0;

CREATE TABLE lim_a (id UInt32) ENGINE = MergeTree() PRIMARY KEY id;
CREATE TABLE lim_b (id UInt32, a_id UInt32) ENGINE = MergeTree() PRIMARY KEY id;
INSERT INTO lim_a VALUES (1);
INSERT INTO lim_b VALUES (1, 1);

SELECT count() FROM lim_a a JOIN lim_b b ON a.id = b.a_id
SETTINGS query_plan_optimize_join_order_limit = 65; -- { serverError INVALID_SETTING_VALUE }

DROP TABLE lim_a;
DROP TABLE lim_b;
