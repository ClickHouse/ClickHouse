-- Tags: stateful
SELECT max(arrayJoin(arrayEnumerateUniq(arrayMap(x -> intDiv(x, 10), URLCategories)))) FROM test.hits
