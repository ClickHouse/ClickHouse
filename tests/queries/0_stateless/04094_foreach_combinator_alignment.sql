-- Regression test: -ForEach with combinators whose sizeOfData is not a
-- multiple of alignOfData (e.g. -Distinct, -OrDefault, -OrNull) must not
-- trigger UBSan misaligned-address errors.  The ForEach combinator stores
-- an array of nested states; without proper stride padding the second and
-- subsequent elements can be misaligned.
--
-- Also covers the case where the nested aggregate has sizeOfData == 0
-- (e.g. `nothing`): arena.alignedAlloc(0) returns the same address for
-- new_state and old_state, so the merge loop in ensureAggregateData must
-- be skipped entirely to avoid calling IAggregateFunction::merge with
-- place == rhs (which is forbidden by the self-aliasing invariant).
-- Reproducer from AST fuzzer: SELECT arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]])

-- Distinct (original failing combo from AST fuzzer)
SELECT sumDistinctForEach(x) FROM (SELECT [number, number % 3] AS x FROM numbers(10));
SELECT countDistinctForEach(x) FROM (SELECT [number % 5, number % 3, number % 2] AS x FROM numbers(30));

-- OrDefault + Distinct
SELECT countOrDefaultDistinctForEach([1, 2, 3]);
SELECT sumOrDefaultDistinctForEach([10, 20]);

-- OrNull
SELECT sumOrNullForEach([100, 200, 300]);

-- OrDefault / OrNull without Distinct
SELECT sumOrDefaultForEach([1, 2, 3]) FROM (SELECT * FROM numbers(3));
SELECT sumOrNullForEach([1, 2]) FROM (SELECT * FROM numbers(3));

-- OrNull + Distinct
SELECT countOrNullDistinctForEach([10, 20, 30]);

-- Regression: -ForEach with zero-sized nested aggregate state (sizeOfData() == 0).
-- arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]) triggers growth in
-- ensureAggregateData from old_size=1 to new_size=3.  When nested_size_of_data == 0,
-- arena.alignedAlloc(0) returns the same address for new_state and old_state,
-- so the merge loop must be skipped (the merge is a no-op for zero-size states anyway).
-- Without the fix this caused a `LOGICAL_ERROR: IAggregateFunction::merge called with
-- the same source and destination state` assertion failure in the AST fuzzer.
SELECT ignore(arrayReduce('uniqStateForEach', [[NULL], [NULL, NULL, NULL]]));
