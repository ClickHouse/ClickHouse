-- Grouping by an aggregate function state serializes every key with `WriteBufferFromArena`, which returns the
-- unused tail of the arena chunk in `complete` after `finalize`. If the tail were not returned, every row would
-- take the rest of a chunk, and this query would need tens of GiB instead of a few hundred MiB.
SELECT count() FROM (SELECT s, count() FROM (SELECT groupArrayState(number) AS s FROM numbers(300000) GROUP BY number) GROUP BY s)
SETTINGS max_memory_usage = '1Gi', max_threads = 1;
