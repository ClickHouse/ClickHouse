-- Comparing two deeply nested `Array`/`Tuple`/`Map` values must visit each element once. Expressing
-- the order by probing both `a < b` and `b < a` per level instead doubles the work per nesting level,
-- so the nesting depth below is itself the assertion: no wall-clock measurement is needed, because
-- a comparison that doubles per level cannot finish these queries inside the suite's own timeout.
--
-- Every arm compares EQUAL values on purpose. A value that differs in a shallow element is fast
-- either way, because the first probe then settles the order without recursing any further.

SET max_block_size = 1;

-- The aggregate value is kept as a `Field`, so `min`/`argMin` and the `-ArgMin` combinator compare
-- two nested `Field`s per row.
SELECT sumArgMin(1, materialize([[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]])) FROM numbers(200);
SELECT min(materialize([[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]])) FROM numbers(200);
SELECT max(materialize((1, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]))) FROM numbers(200);
SELECT min(materialize(map(1, [[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]))) FROM numbers(200);

-- Merging two aggregate states compares the stored `Field`s directly, which is a different overload
-- from the per-row path above.
SELECT minMerge(s) FROM (
    SELECT minState(materialize([[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]])) AS s
    FROM numbers(200) GROUP BY number % 200
);

-- `groupArraySorted` sorts stored `Field`s, and neither ORDER BY nor GROUP BY reaches it (those use
-- `IColumn::compareAt`, which has always been one-pass).
SELECT length(groupArraySorted(50)(materialize([[[[[[[[[[[[[[[[[[[[[[[[[[[[[[1]]]]]]]]]]]]]]]]]]]]]]]]]]]]]]))) FROM numbers(200);

-- Ordering of shallow values is unchanged, including across element types and lengths.
SELECT min([number % 7, number % 3]), max([number % 7, number % 3]) FROM numbers(100);
SELECT min((number % 7, toString(number % 5))) FROM numbers(100);
SELECT min(arraySlice([1, 2, 3, 4, 5], 1, number % 5 + 1)) FROM numbers(20);
SELECT min([nan, toFloat64(number)]) FROM numbers(10);
SELECT min([NULL, CAST(number % 3 AS Nullable(UInt8))]) FROM numbers(50);

-- The `anyHeavy` arm pins that equality over a container of aggregate states still works. The `min`
-- arm pins that such a type is still refused while the aggregate function is constructed, before any
-- row is compared, so it never reaches `Field` ordering. That equality-versus-ordering contract is
-- asserted in src/Core/tests/gtest_field.cpp instead. Both arms read the state from a subquery so
-- that `min` does not nest one aggregate function inside another, which a separate check refuses
-- before the argument type is examined.
SELECT arrayMap(x -> finalizeAggregation(x), anyHeavy(arr))
FROM (SELECT [sumState(toUInt64(6))] AS arr FROM numbers(3) GROUP BY number);
SELECT min(arr) FROM (SELECT [sumState(toUInt64(6))] AS arr FROM numbers(3) GROUP BY number); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
