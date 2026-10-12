-- Tags: no-replicated-database
-- A Replicated database refuses an ALTER that mixes command types, which is all this test does.

DROP TABLE IF EXISTS mv_mixed_src;
DROP TABLE IF EXISTS mv_mixed;

CREATE TABLE mv_mixed_src (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv_mixed (id UInt64 COMMENT 'initial')
    ENGINE = MergeTree ORDER BY id AS SELECT id FROM mv_mixed_src;

-- MODIFY QUERY rebuilds the view's columns without comments, so it must not erase one set before it.
ALTER TABLE mv_mixed COMMENT COLUMN id 'before query', MODIFY QUERY SELECT id FROM mv_mixed_src WHERE id > 0;
SELECT 'comment then query', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_mixed' AND name = 'id';

ALTER TABLE mv_mixed MODIFY QUERY SELECT id FROM mv_mixed_src WHERE id > 1, COMMENT COLUMN id 'after query';
SELECT 'query then comment', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_mixed' AND name = 'id';

-- Two comments on one column in one statement: the last one wins, on the view and after a reload.
ALTER TABLE mv_mixed COMMENT COLUMN id 'first', COMMENT COLUMN id 'second';
DETACH TABLE mv_mixed;
ATTACH TABLE mv_mixed;
SELECT 'last comment wins', comment FROM system.columns
WHERE database = currentDatabase() AND table = 'mv_mixed' AND name = 'id';

DROP TABLE mv_mixed;
DROP TABLE mv_mixed_src;
