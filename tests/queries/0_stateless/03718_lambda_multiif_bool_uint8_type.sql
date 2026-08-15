-- A lambda body that folds a constant-condition if/multiIf mixing a Bool literal branch with a
-- UInt8 comparison branch used to abort with LOGICAL_ERROR "Lambda resolved type Function(... -> Bool)
-- is not equal to type from actions DAG Function(... -> UInt8)" under the analyzer. See issue #111237.
SET enable_analyzer = 1;

SELECT arrayFilter(p -> multiIf(false, true, p = 'ALL'), array('ALL', 'X', 'ALL'));
SELECT arrayFilter(p -> if(false, true, p = 'ALL'), array('ALL', 'X'));
SELECT arrayMap(p -> multiIf(false, true, p = 'ALL'), array('ALL', 'X'));
SELECT arrayFilter(p -> multiIf(p = 'Z', true, false, true, p = 'ALL'), array('ALL', 'X', 'Z'));

-- Previously-working shapes must keep working.
SELECT arrayFilter(p -> multiIf(false, 1, p = 'ALL'), array('ALL', 'X'));
SELECT arrayFilter(p -> multiIf(false, true, 0), array('ALL', 'X'));
