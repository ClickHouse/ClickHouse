-- Tags: no-random-detach
-- no-random-detach: the keys kept from the failed insert are only in memory, not in the on-disk backup of the `Join` table, so they are lost on reattach.

-- An insert into a `Join` table that throws partway through a block keeps the keys inserted before the throw.
-- The row count that a trivial `count()` reads must still match a scan, also after a later insert.

DROP TABLE IF EXISTS join_partial_insert;
CREATE TABLE join_partial_insert (k UInt64) ENGINE = Join(ANY, LEFT, k);

-- The hash table runs out of memory on one of its resizes, after some keys are in.
INSERT INTO join_partial_insert SELECT number FROM numbers(4000000)
SETTINGS max_block_size = 4000000, min_insert_block_size_rows = 4000000, min_insert_block_size_bytes = 0, max_memory_usage = '80Mi', max_untracked_memory = 0; -- { serverError MEMORY_LIMIT_EXCEEDED }

SELECT trivial = scan, scan > 0
FROM
(
    SELECT
        (SELECT count() FROM join_partial_insert) AS trivial,
        (SELECT count() FROM join_partial_insert SETTINGS optimize_trivial_count_query = 0) AS scan
);

INSERT INTO join_partial_insert SELECT number + 10000000 FROM numbers(10);

SELECT trivial = scan
FROM
(
    SELECT
        (SELECT count() FROM join_partial_insert) AS trivial,
        (SELECT count() FROM join_partial_insert SETTINGS optimize_trivial_count_query = 0) AS scan
);

DROP TABLE join_partial_insert;
