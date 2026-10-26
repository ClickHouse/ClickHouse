-- Multi-step PREWHERE must see patched values of the columns it carries over to evaluate `DEFAULT`s.
-- https://github.com/ClickHouse/ClickHouse/issues/111757

DROP TABLE IF EXISTS t_multistep_prewhere_default_patch;

SET enable_lightweight_update = 1, apply_patch_parts = 1;
SET optimize_move_to_prewhere = 1, move_all_conditions_to_prewhere = 1, enable_multiple_prewhere_read_steps = 1;

CREATE TABLE t_multistep_prewhere_default_patch (k UInt32, s String) ENGINE = MergeTree ORDER BY tuple()
SETTINGS enable_block_number_column = 1, enable_block_offset_column = 1;

SYSTEM STOP MERGES t_multistep_prewhere_default_patch;

INSERT INTO t_multistep_prewhere_default_patch SELECT number, toString(number % 7) FROM numbers(1000);
ALTER TABLE t_multistep_prewhere_default_patch ADD COLUMN v Int64 DEFAULT k % 13;

UPDATE t_multistep_prewhere_default_patch SET k = k + 1 WHERE 1;

-- Both queries must agree: `v` is missing in the part, so it is evaluated from the patched `k`.
SELECT sum((k % 13)::Int64) FROM t_multistep_prewhere_default_patch WHERE k <= 396 AND s = '3';
SELECT sum(v) FROM t_multistep_prewhere_default_patch WHERE k <= 396 AND s = '3';

SELECT k, v FROM t_multistep_prewhere_default_patch WHERE k <= 30 AND s = '3' ORDER BY k;

DROP TABLE t_multistep_prewhere_default_patch;
