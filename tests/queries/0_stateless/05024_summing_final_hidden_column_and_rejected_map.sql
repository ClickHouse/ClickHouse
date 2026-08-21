-- A summed column that is invisible to both the query and the PREWHERE predicate still
-- decides whether the merge removes the row, so `FINAL` must read it anyway.

DROP TABLE IF EXISTS summing_final_hidden;
CREATE TABLE summing_final_hidden (a Int8, b Int32, note String, k UInt64) ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_hidden;

-- `a` sums to zero for key 1, `b` does not, so the row stays; for key 2 both sum to zero.
INSERT INTO summing_final_hidden VALUES (1, 5, 'x', 1), (1, 5, 'x', 2);
INSERT INTO summing_final_hidden VALUES (-1, 0, 'x', 1), (-1, -5, 'x', 2);

SELECT count() FROM summing_final_hidden FINAL PREWHERE note = 'x' SETTINGS enable_analyzer = 0;
SELECT count() FROM summing_final_hidden FINAL PREWHERE note = 'x' SETTINGS enable_analyzer = 1;
SELECT k FROM summing_final_hidden FINAL PREWHERE note = 'x' ORDER BY k;

-- The FINAL read must agree with the state after a real merge.
SYSTEM START MERGES summing_final_hidden;
OPTIMIZE TABLE summing_final_hidden FINAL;
SELECT count() FROM summing_final_hidden;

DROP TABLE summing_final_hidden;

-- A nested table whose name ends with `Map` is only merged with `sumMap` when the whole
-- group is a valid map: at least two columns, an integer-like key and summable values.
-- The groups below are rejected by the merge, which merely copies them, so a `FINAL` read
-- must not read those arrays either.

DROP TABLE IF EXISTS summing_final_rejected_map;
CREATE TABLE summing_final_rejected_map
(
    k UInt64,
    s Int64,
    OnlyOneColumnMap Nested(ID UInt32),
    NonArithmeticValueMap Nested(ID UInt32, D Date)
)
ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_rejected_map;

INSERT INTO summing_final_rejected_map SELECT number, 1, range(50), range(50), arrayMap(x -> toDate('2020-01-01'), range(50)) FROM numbers(100000);
INSERT INTO summing_final_rejected_map SELECT number, 1, range(50), range(50), arrayMap(x -> toDate('2020-01-01'), range(50)) FROM numbers(100000);

SELECT count() FROM summing_final_rejected_map FINAL SETTINGS log_comment = '05024_rejected_map';

SYSTEM FLUSH LOGS query_log;

-- Only `k` and `s` have to be read: the rejected map arrays alone are two orders of
-- magnitude larger than that.
SELECT read_bytes < 20000000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05024_rejected_map' AND type = 'QueryFinish';

-- A valid map group, in contrast, participates in the summation and has to be read.
DROP TABLE IF EXISTS summing_final_valid_map;
CREATE TABLE summing_final_valid_map (k UInt64, s Int64, GoodMap Nested(ID UInt32, V UInt64)) ENGINE = SummingMergeTree ORDER BY k;
SYSTEM STOP MERGES summing_final_valid_map;

INSERT INTO summing_final_valid_map SELECT number, 1, range(50), range(50) FROM numbers(100000);
INSERT INTO summing_final_valid_map SELECT number, 1, range(50), range(50) FROM numbers(100000);

SELECT count() FROM summing_final_valid_map FINAL SETTINGS log_comment = '05024_valid_map';

SYSTEM FLUSH LOGS query_log;

SELECT read_bytes > 20000000
FROM system.query_log
WHERE current_database = currentDatabase() AND log_comment = '05024_valid_map' AND type = 'QueryFinish';

DROP TABLE summing_final_rejected_map;
DROP TABLE summing_final_valid_map;
