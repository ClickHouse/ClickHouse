SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (
    x UInt32,
    arr1 Array(Int32),
    arr2 Array(Int32)
) ENGINE = Memory;

INSERT INTO t1 VALUES (1, [10, 20], [30, 40]);

-- Test normal COLUMNS() ARRAY JOIN (should work)
SELECT x, arr1, arr2 FROM t1 ARRAY JOIN COLUMNS('arr.*') ORDER BY arr1, arr2;

-- Test COLUMNS() matching no columns (should fail)
SELECT * FROM t1 ARRAY JOIN COLUMNS('nonexistent'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

DROP TABLE t1;
