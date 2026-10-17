-- Tags: no-old-analyzer
-- Tests QueryTreePassManager::dump(WriteBuffer &, size_t) and the run() error path
-- (src/Analyzer/QueryTreePassManager.cpp lines 221-225, 250-266).
-- dump(WriteBuffer &, size_t) lists pass names 0..N-1 to a buffer when dump_passes=1;
-- previously uncovered because no test set dump_passes=1.
-- The run() error path throws BAD_ARGUMENTS when the requested pass count exceeds
-- the total (46); previously uncovered because tests used valid pass counts or the default.

SET enable_analyzer = 1;

-- dump(WriteBuffer &, size_t): lists the first 3 pass names and descriptions, then the query tree
EXPLAIN QUERY TREE dump_passes = 1, passes = 3 SELECT 1 + 1;

-- run() error path: passes=999 exceeds total pass count -> BAD_ARGUMENTS (Code 36)
EXPLAIN QUERY TREE passes = 999 SELECT 1 + 1; -- { serverError BAD_ARGUMENTS }
