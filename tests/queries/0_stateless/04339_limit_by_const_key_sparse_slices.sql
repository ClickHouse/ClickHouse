-- Regression test for issue #107172: LimitByTransform aborted with
-- "Invalid number of rows in Chunk" (LOGICAL_ERROR) when the sparse-slice
-- branch of materializeSlicesIntoChunk kept rows for a ColumnConst LIMIT BY
-- key. The const key was sized by the slice-length sum while non-const
-- columns were sized by the filter mask popcount, so the output columns
-- could disagree on row count. This covers the sparse (gappy) multi-slice
-- path together with a constant LIMIT BY key.

DROP TABLE IF EXISTS t_lbsparse;
CREATE TABLE t_lbsparse (n UInt32, g UInt32) ENGINE = Memory;

-- Groups are interleaved within one block, so the kept rows form gaps and
-- the transform falls into the mask-based sparse-filter branch.
INSERT INTO t_lbsparse VALUES (0, 1) (1, 2) (2, 1) (3, 2) (4, 1) (5, 2) (6, 3);

SELECT n, g FROM t_lbsparse ORDER BY n LIMIT 1 BY 'C', g SETTINGS max_block_size = 1000;
SELECT n, g FROM t_lbsparse ORDER BY n LIMIT 2 BY 'C', g SETTINGS max_block_size = 1000;
SELECT n, g FROM t_lbsparse ORDER BY n LIMIT 1 OFFSET 1 BY 'C', g SETTINGS max_block_size = 1000;

-- The shape from the original report: a constant LIMIT BY key alongside a
-- function result column. Must not abort the server.
SELECT concatAssumeInjective('p', toString(g)) AS gg
FROM t_lbsparse
ORDER BY n
LIMIT 1 BY '2147483646', gg
SETTINGS max_block_size = 1000;

DROP TABLE t_lbsparse;
