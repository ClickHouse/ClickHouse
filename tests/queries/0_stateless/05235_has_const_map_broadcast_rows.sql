-- One constant map is searched for every row of the needle, so the result must carry one row per NEEDLE
-- row. At a single row that is indistinguishable from one row per haystack offset, hence several rows here.

-- A Map first argument is never rewritten to IN (HasToInPass.cpp:49-51 returns unless the first argument
-- is an Array), pinned so the test keeps exercising the search rather than a set.
SET optimize_rewrite_has_to_in = 0;

-- integral keys, non-constant needle
SELECT number, has(map(1, 'a', 3, 'b'), number) FROM numbers(4) ORDER BY number;

-- an Enum key matched by its name, non-constant needle: the case a Field-based search answers differently
SELECT number, has(map('a'::Enum8('a' = 1, 'b' = 2), 1), materialize('a')) FROM numbers(2) ORDER BY number;

-- String keys, non-constant needle
SELECT number, has(map('1', 'x', '3', 'y'), toString(number)) FROM numbers(4) ORDER BY number;

-- an onlyNull needle: the result size comes from the needle and not from the haystack's single offset
SELECT number, has(map('a', 1), NULL) FROM numbers(3) ORDER BY number;

-- a 5000-key constant map: a key at the start, around 4096, at the end, and absent keys
SELECT number, has(mapFromArrays(range(5000), range(5000)), number)
FROM (SELECT arrayJoin([0, 4095, 4096, 4097, 4999, 5000, 123456]) AS number) ORDER BY number;

-- the same for String keys
SELECT number, has(mapFromArrays(arrayMap(x -> toString(x), range(5000)), range(5000)), toString(number))
FROM (SELECT arrayJoin([0, 4095, 4096, 4097, 4999, 5000]) AS number) ORDER BY number;
