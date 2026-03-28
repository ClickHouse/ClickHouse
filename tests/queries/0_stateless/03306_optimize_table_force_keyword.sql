CREATE TABLE tab (x UInt32) ENGINE = MergeTree ORDER BY x;

INSERT INTO tab values (1);
INSERT INTO tab values (2);

-- To force merge, traditionally keyword 'FINAL' is used.
-- Test that FORCE works as well.
OPTIMIZE TABLE tab FORCE;

SELECT count(*)
FROM system.parts
WHERE database = currentDatabase()
    AND table = 'tab'
    AND active = 1
