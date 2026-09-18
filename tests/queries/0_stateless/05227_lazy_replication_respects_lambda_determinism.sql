-- A lazily replicated argument used to let a non-deterministic lambda body run once per source row (#119135).
SELECT uniqExact(r), count() FROM (SELECT arrayMap(i -> rand64(i + 7), [number])[1] AS r FROM numbers(100) ARRAY JOIN range(4) AS x);
SELECT uniqExact(r), count() FROM (SELECT arrayMap(i -> rand64(i + 7), [number])[1] AS r FROM (SELECT number, arrayJoin(range(4)) AS x FROM numbers(100))) SETTINGS query_plan_lower_array_join_function = 1;
SELECT countIf(c < 4) > 0 FROM (SELECT number, count() AS c FROM (SELECT number FROM numbers(1000) ARRAY JOIN range(4) AS x WHERE arrayMap(i -> rand64(i + 7), [number])[1] % 4 = 0) GROUP BY number);
SELECT countIf(c < 4) > 0 FROM (SELECT number, count() AS c FROM (SELECT number FROM (SELECT number, arrayJoin(range(4)) AS x FROM numbers(1000)) WHERE arrayMap(i -> rand64(i + 7), [number])[1] % 4 = 0) GROUP BY number) SETTINGS query_plan_lower_array_join_function = 1;
-- a deterministic lambda keeps one value per source row
SELECT uniqExact(r), count() FROM (SELECT arrayMap(i -> i + 7, [number])[1] AS r FROM numbers(100) ARRAY JOIN range(4) AS x);
