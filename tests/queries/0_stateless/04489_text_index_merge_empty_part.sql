-- Regression test: merging a text-indexed part that has no rows must not fail.
-- A part with zero rows (e.g. after `DELETE` removes all of its rows, after a TTL,
-- or when an outdated empty part is re-activated by a `DETACH`/`ATTACH` cycle) writes
-- empty text-index streams, but its empty `.idx` file is still listed in the checksums.
-- The text-index merge used to take such a part as a source "as is" and then fail while
-- reading the empty Regular substream with `ATTEMPT_TO_READ_AFTER_EOF`.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_text_empty;

CREATE TABLE t_text_empty
(
    id UInt64,
    s String,
    INDEX idx_text s TYPE text(tokenizer = 'splitByNonAlpha') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4;

INSERT INTO t_text_empty SELECT number, 'hello world' FROM numbers(20);

-- Delete all rows of the only part: the mutation produces an active part with zero rows
-- and empty text-index streams.
ALTER TABLE t_text_empty DELETE WHERE 1;

SELECT 'rows in active parts after delete', sum(rows) FROM system.parts
WHERE database = currentDatabase() AND table = 't_text_empty' AND active;

-- Add a non-empty part and merge it together with the empty one.
INSERT INTO t_text_empty SELECT number + 20, 'hello world' FROM numbers(20);

OPTIMIZE TABLE t_text_empty FINAL;

SELECT 'count after merge', count() FROM t_text_empty;
SELECT 'count by token after merge', count() FROM t_text_empty WHERE hasToken(s, 'hello');

DROP TABLE t_text_empty;
