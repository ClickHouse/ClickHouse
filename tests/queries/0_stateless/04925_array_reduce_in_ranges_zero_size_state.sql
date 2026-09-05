-- Aggregates over only-NULL arguments have a zero-byte state. arrayReduceInRanges must behave the
-- same for them as for any other aggregate.

-- per-range merge: a range spanning a whole 64-row block
SELECT arrayReduceInRanges('groupArray', [(1, 64)],  arrayMap(x -> (x + NULL), range(64)));
SELECT arrayReduceInRanges('groupArray', [(1, 100)], arrayMap(x -> (x + NULL), range(100)));
-- pairwise level merge
SELECT arrayReduceInRanges('groupArray', [(1, 200)], arrayMap(x -> (x + NULL), range(257)));
SELECT arrayReduceInRanges('uniq',       [(1, 200)], arrayMap(x -> (x + NULL), range(257)));
SELECT arrayReduceInRanges('count',      [(1, 200)], arrayMap(x -> (x + NULL), range(257)));
-- an only-NULL argument collapses to the bare placeholder before the State combinator applies
SELECT arrayReduceInRanges('groupArrayState', [(1, 200)], arrayMap(x -> (x + NULL), range(257)));
-- -State delegates sizeOfData, so over a zero-size nested function its state is zero-size too.
-- The argument must be non-null, otherwise the Null combinator replaces the wrapper.
SELECT toTypeName(arrayReduceInRanges('nothingState', [(1, 200)], range(257)));
SELECT hex(arrayReduceInRanges('nothingState', [(1, 200)], range(257))[1]);
SELECT hex(arrayReduceInRanges('nothingState', [(1, 64)],  range(64))[1]);
-- -Tuple whose every element is zero-size
SELECT arrayReduceInRanges('sumTuple', [(1, 200)], arrayMap(x -> (x + NULL, x + NULL), range(257)));
-- several ranges
SELECT arrayReduceInRanges('groupArray', [(1, 200), (50, 150), (100, 100)],
                           arrayMap(x -> (x + NULL), range(257)));
-- zero-size state reaching neither merge
SELECT arrayReduceInRanges('groupArray', [(1, 60)], arrayMap(x -> (x + NULL), range(63)));

-- A window function is zero-size too, and stays rejected as an aggregate at every range length.
-- For these three the per-range finalize reports it, so they do not observe whether a merge ran.
SELECT arrayReduceInRanges('rankIf', [(1, 64)],  arrayMap(x -> (x + NULL), range(64)));  -- { serverError BAD_ARGUMENTS }
SELECT arrayReduceInRanges('rankIf', [(1, 200)], arrayMap(x -> (x + NULL), range(257))); -- { serverError BAD_ARGUMENTS }
SELECT arrayReduceInRanges('rankIf', [(1, 10)],  arrayMap(x -> (x + NULL), range(10)));  -- { serverError BAD_ARGUMENTS }
-- With no ranges the per-range loop never runs, so the merge over pre-aggregation places is the
-- only remaining path to the aggregate's merge. The CAST is required: a bare [] is Array(Nothing).
SELECT arrayReduceInRanges('rankIf', CAST([], 'Array(Tuple(Int64, UInt64))'),
                           arrayMap(x -> (x + NULL), range(257)));                       -- { serverError BAD_ARGUMENTS }

-- Non-zero-size states through the same paths are unchanged
SELECT arrayReduceInRanges('sum',        [(1, 200)], arrayMap(x -> 1, range(257)));
SELECT arrayReduceInRanges('groupArray', [(1, 5), (10, 5)], range(257));
SELECT arrayReduceInRanges('uniq',       [(1, 200)], arrayMap(x -> x % 7, range(257)));
