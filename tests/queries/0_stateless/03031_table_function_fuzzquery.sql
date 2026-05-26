-- Tags: no-llvm-coverage
-- no-llvm-coverage: although `fuzzQuery` is called with an explicit seed, the fuzzer
-- accumulates state (`column_like` / `table_like`) across invocations, so the set of
-- branches taken inside `QueryFuzzer.cpp` still depends on what other tests ran before
-- this one in the same server process.

SELECT * FROM fuzzQuery('SELECT 1', 500, 8956) LIMIT 0 FORMAT TSVWithNamesAndTypes;

SELECT * FROM fuzzQuery('SELECT *
FROM (
  SELECT
    ([toString(number % 2)] :: Array(LowCardinality(String))) AS item_id,
    count()
  FROM numbers(3)
  GROUP BY item_id WITH TOTALS
) AS l FULL JOIN (
  SELECT
    ([toString((number % 2) * 2)] :: Array(String)) AS item_id
  FROM numbers(3)
) AS r
ON l.item_id = r.item_id
ORDER BY 1,2,3;
', 500, 8956) LIMIT 10 FORMAT NULL;
