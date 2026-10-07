SET allow_experimental_projection_text_index = 1;
SET enable_full_text_index = 1;

-- Tests projection text index behaviour with nested type wrappers around String.
-- Validation must accept what materialization can handle:
--   * Nullable(String): per-row null map
--   * LowCardinality(String): plain dictionary
--   * LowCardinality(Nullable(String)): null dictionary entries projected onto a row-level null map
-- and reject Nullable(LowCardinality(...)) at DDL time so it does not fail later at INSERT.

-- Nullable(String) — tokenization should skip NULL rows.

DROP TABLE IF EXISTS t_nullable;
CREATE TABLE t_nullable
(
    id UInt32,
    s Nullable(String),
    PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_nullable VALUES (1, 'hello world'), (2, NULL), (3, 'hello again');
SELECT count() FROM t_nullable WHERE hasToken(s, 'hello');
DROP TABLE t_nullable;

-- LowCardinality(String) — plain dictionary, no null map.

DROP TABLE IF EXISTS t_lc;
CREATE TABLE t_lc
(
    id UInt32,
    s LowCardinality(String),
    PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc VALUES (1, 'hello world'), (2, 'something else'), (3, 'hello again');
SELECT count() FROM t_lc WHERE hasToken(s, 'hello');
DROP TABLE t_lc;

-- LowCardinality(Nullable(String)) — dictionary's null entries must be respected.
-- The tokenize loop detects NULL rows via `lowcard_index[i] == 0`
-- (ClickHouse's `ColumnLowCardinality` invariant places the NULL placeholder at
-- dictionary position 0). This avoids relying on `data.empty()` to catch NULL rows,
-- which would let `LowCardinality(Nullable(FixedString(N)))` leak NUL bytes from the
-- dictionary's default value into tokenizers that treat them as content.

DROP TABLE IF EXISTS t_lc_nullable;
CREATE TABLE t_lc_nullable
(
    id UInt32,
    s LowCardinality(Nullable(String)),
    PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_lc_nullable VALUES (1, 'hello world'), (2, NULL), (3, 'hello again');
SELECT count() FROM t_lc_nullable WHERE hasToken(s, 'hello');
DROP TABLE t_lc_nullable;

-- Nullable(LowCardinality(...)) is rejected by ClickHouse's general type rules before
-- our projection-specific check sees it (Nullable cannot wrap LowCardinality globally,
-- so the type-construction layer rejects it with ILLEGAL_TYPE_OF_ARGUMENT). Our
-- projection-side check in ProjectionIndexText::fillProjectionDescription remains as
-- defence-in-depth for any future relaxation of that global rule.

CREATE TABLE t_n_lc
(
    id UInt32,
    s Nullable(LowCardinality(String)),
    PROJECTION idx INDEX s TYPE text(tokenizer = 'splitByNonAlpha')
)
ENGINE = MergeTree ORDER BY id; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
