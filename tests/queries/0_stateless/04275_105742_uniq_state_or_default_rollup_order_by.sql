-- Regression test for #105742: SEGV in `UniquesHashSet::merge` when sorting
-- an aggregate-state column produced by `WITH ROLLUP` (or `CUBE` / `TOTALS`)
-- + `ORDER BY`, using `uniqStateOrDefault` over data where some groups have
-- no input rows (so the `-OrFill` flag byte is 0 for those rows).
-- See the PR description for the full root cause.

CREATE TABLE _r (k UInt32, v Nullable(Int64), g UInt8) ENGINE = MergeTree ORDER BY k;
INSERT INTO _r SELECT number, if(number%3 = 0, NULL, number*10), number%3 FROM numbers(100);

-- Should not SEGV. Row content is uninteresting, only successful execution.
SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH ROLLUP
ORDER BY g DESC
FORMAT Null;

SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH ROLLUP WITH TOTALS
ORDER BY g DESC NULLS FIRST
FORMAT Null;

SELECT g, uniqStateOrDefault(v)
FROM _r
GROUP BY g WITH CUBE
ORDER BY g DESC
FORMAT Null;

-- Bare `uniqState` shares the convertToValues path.
SELECT g, uniqState(toUInt32(k))
FROM _r
GROUP BY g WITH ROLLUP
ORDER BY g DESC
FORMAT Null;

SELECT 'ok';
