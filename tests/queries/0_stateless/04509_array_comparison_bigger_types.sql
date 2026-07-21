-- Comparison of arrays whose element types have no least common supertype
-- (mixed signed/unsigned integers wider than 32 bits)

-- Test all six operators, mix signed/unsigned
SELECT [-1]::Array(Int64) = [1]::Array(UInt64);
SELECT [-1]::Array(Int64) != [1]::Array(UInt64);
SELECT [-1]::Array(Int64) < [1]::Array(UInt64);
SELECT [-1]::Array(Int64) <= [1]::Array(UInt64);
SELECT [-1]::Array(Int64) > [1]::Array(UInt64);
SELECT [-1]::Array(Int64) >= [1]::Array(UInt64);

-- Length tie-breaking on equal common prefix
SELECT [1,2]::Array(Int64)   <  [1,2,3]::Array(UInt64);
SELECT [1,2,3]::Array(Int64) <  [1,2]::Array(UInt64);
SELECT [1,2]::Array(Int64)   =  [1,2,3]::Array(UInt64);
SELECT [1,2]::Array(Int64)   != [1,2,3]::Array(UInt64);

-- Empty arrays
SELECT []::Array(Int64) =  []::Array(UInt64);
SELECT []::Array(Int64) <  [1]::Array(UInt64);

-- Same, but non-constant (exercises the offset-gathering path)
SELECT materialize([-1]::Array(Int64)) <  materialize([1]::Array(UInt64));
SELECT materialize([-1]::Array(Int64)) <= materialize([1]::Array(UInt64));
SELECT materialize([-1]::Array(Int64)) >  materialize([1]::Array(UInt64));
SELECT materialize([-1]::Array(Int64)) >= materialize([1]::Array(UInt64));
SELECT materialize([-1]::Array(Int64)) =  materialize([1]::Array(UInt64));
SELECT materialize([-1]::Array(Int64)) != materialize([1]::Array(UInt64));

-- Length tie-breaking on equal common prefix
SELECT [1,2]::Array(Int64)   <  [1,2,3]::Array(UInt64);
SELECT [1,2,3]::Array(Int64) <  [1,2]::Array(UInt64);
SELECT [1,2]::Array(Int64)   =  [1,2,3]::Array(UInt64);
SELECT [1,2]::Array(Int64)   != [1,2,3]::Array(UInt64);

-- Empty arrays
SELECT []::Array(Int64) =  []::Array(UInt64);
SELECT []::Array(Int64) <  [1]::Array(UInt64);
SELECT [1]::Array(Int64) < []::Array(UInt64);

-- Wide integers
SELECT [-1]::Array(Int256)  <  [1]::Array(UInt256);
SELECT [100]::Array(Int256) =  [100]::Array(UInt256);

-- Nested arrays
SELECT [[1,2],[3]]::Array(Array(Int64)) < [[1,2],[4]]::Array(Array(UInt64));

-- Multi-row via table (per-row offsets)
DROP TABLE IF EXISTS t_arr_cmp;
CREATE TABLE t_arr_cmp (a Array(Int64), b Array(UInt64)) ENGINE = Memory;
INSERT INTO t_arr_cmp VALUES ([1,2], [1,3]), ([3,4], [1,3]), ([-1,5], [1,3]);
SELECT a < b FROM t_arr_cmp ORDER BY a;
DROP TABLE t_arr_cmp;

-- Consistency with tuple comparison
SELECT (tuple(-1::Int64) < tuple(1::UInt64)) = ([-1]::Array(Int64) < [1]::Array(UInt64));

-- Aligned String-vs-String subfield with a signed/unsigned mismatch in another subfield is
-- comparable (String field via the normal string path, the integer field via accurate comparison)
-- and must match the equivalent bare-tuple comparison.
SELECT ([tuple('a', -1::Int64)]::Array(Tuple(String, Int64)) <  [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64))) = (tuple('a', -1::Int64) <  tuple('a', 1::UInt64));
SELECT ([tuple('a', -1::Int64)]::Array(Tuple(String, Int64)) =  [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64))) = (tuple('a', -1::Int64) =  tuple('a', 1::UInt64));
SELECT ([tuple('a', -1::Int64)]::Array(Tuple(String, Int64)) != [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64))) = (tuple('a', -1::Int64) != tuple('a', 1::UInt64));
SELECT ([tuple('b',  5::Int64)]::Array(Tuple(String, Int64)) >  [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64))) = (tuple('b',  5::Int64) >  tuple('a', 1::UInt64));
-- non-constant (exercises the offset-gathering path)
SELECT (materialize([tuple('a', -1::Int64)]::Array(Tuple(String, Int64))) < materialize([tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)))) = (tuple('a', -1::Int64) < tuple('a', 1::UInt64));

-- Incomparable element types still throw
SELECT ['a']::Array(String) < [1]::Array(Int64); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT}
-- A String subfield aligned with a number (at any position) is still rejected: here both positions
-- are crossed String-vs-number, so there is no genuine element comparison.
SELECT [tuple('a', 1::UInt64)]::Array(Tuple(String, UInt64)) < [tuple(1::Int64, 'a')]::Array(Tuple(Int64, String)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT}