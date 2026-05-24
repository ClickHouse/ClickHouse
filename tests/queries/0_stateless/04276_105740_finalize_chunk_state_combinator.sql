-- Regression test for #105740 / #102460: `finalizeChunk` calls
-- `ColumnAggregateFunction::convertToValues` for every `-State` aggregate column
-- that flows through `CubeTransform`, `RollupTransform`, `TotalsHavingTransform`
-- or `AggregatingInOrderTransform`. Before #105749 the convertToValues loop
-- pushed raw source-state pointers via `func->insertResultInto` and relied on
-- `res.src` to keep the source column alive. The `-OrFill` flag=0 branch reset
-- `res.src`, so any subsequent flag=1 row would later read a destructed state
-- (jemalloc free, ASan use-after-free for heap-owning states like uniq,
-- MSan use-of-uninitialized for any State combinator) once the source column
-- was dropped.
--
-- Companion to `04275_105742_*` which covers the `RollupTransform` / `CubeTransform`
-- paths. This test covers the `TotalsHavingTransform` path (the MSan producer
-- from #105740) plus additional State combinators that share the same call site.
-- Row content is irrelevant, only successful execution under sanitizer builds.

CREATE TABLE _s1 (g Nullable(UInt64), a Nullable(UInt64), b Nullable(UInt64)) ENGINE = Memory;
CREATE TABLE _s2 (x Nullable(UInt64)) ENGINE = Memory;
INSERT INTO _s1 VALUES (1, 1, 1), (1, 2, 2), (2, 3, 3), (NULL, NULL, NULL);
INSERT INTO _s2 VALUES (1), (2), (3), (4), (5), (6);

-- `uniqStateOrDefault` + RIGHT JOIN + `WITH TOTALS` + `ORDER BY`. RIGHT JOIN
-- keeps unmatched _s2 rows so the group of unmatched rows has all-NULL inputs
-- and exercises the `-OrFill` flag=0 branch in `convertToValues`. Goes through
-- `TotalsHavingTransform::finalize` -> `finalizeChunk` -> `convertToValues`.
-- `uniqStateOrDefault`'s inner state owns a heap buffer (`UniquesHashSet::buf`),
-- so the dangling pointer surfaces as either SEGV (Debug/release) or
-- AddressSanitizer heap-use-after-free.
SELECT s1.g, uniqStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH TOTALS
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- MSan producer from #105740: `singleValueOrNullStateOrDefault` over the same
-- JOIN+TOTALS+ORDER BY shape. The inner state does not own heap memory, so
-- this case is MSan-only (`use-of-uninitialized-value` in the destructed state)
-- pre-fix; post-fix it just returns cleanly.
SELECT s1.g, singleValueOrNullStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH TOTALS
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- `CUBE` variant: routes through `CubeTransform::generate` -> `finalizeChunk`.
SELECT s1.g, uniqStateOrDefault(s1.b)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH CUBE
ORDER BY s1.g ASC NULLS LAST
FORMAT Null;

-- `argMaxStateOrDefault` is another State combinator that owns heap memory
-- (the chosen value when typed as a heap-allocating type); same convertToValues
-- path. Combined with `WITH ROLLUP WITH TOTALS` to exercise both transforms.
SELECT s1.g, argMaxStateOrDefault(s1.b, s1.a)
FROM _s1 AS s1 RIGHT JOIN _s2 ON s1.a = _s2.x
GROUP BY s1.g WITH ROLLUP WITH TOTALS
ORDER BY s1.g DESC NULLS FIRST
FORMAT Null;

SELECT 'ok';
