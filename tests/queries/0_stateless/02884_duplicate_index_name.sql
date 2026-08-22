DROP TABLE IF EXISTS test_dup_index;

CREATE TABLE test_dup_index
(
	a Int64,
	b Int64,
	INDEX idx_a a TYPE minmax,
	INDEX idx_a b TYPE minmax
) Engine = MergeTree()
ORDER BY a; -- { serverError ILLEGAL_INDEX }
