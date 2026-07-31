-- Tags: no-ordinary-database, no-replicated-database
-- no-ordinary-database: CREATE OR REPLACE MATERIALIZED VIEW requires an Atomic database.
-- no-replicated-database: POPULATE is not supported in a Replicated database.

-- `CREATE OR REPLACE MATERIALIZED VIEW ... POPULATE` used to leave the new view unsubscribed from
-- its source table. The replace creates a temporary view, populates it (which caches the temporary
-- name -> temporary storage in the query context), then atomically swaps it with the target via
-- EXCHANGE. The internal DROP of the old table then resolved the temporary name through the stale
-- cache and shut down the live (new) view instead, detaching it from its source. As a result every
-- insert after the replace was silently dropped. See https://github.com/ClickHouse/ClickHouse/issues/108726

DROP TABLE IF EXISTS src SYNC;
DROP TABLE IF EXISTS mv SYNC;

CREATE TABLE src (id UInt64) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1);

-- The view must already exist so that CREATE OR REPLACE actually replaces it (EXCHANGE), not just creates it.
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
SELECT 'after first create', count() FROM mv;

CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
-- POPULATE re-captured the existing source data.
SELECT 'after replace, populated', count() FROM mv;

-- The crucial part: the view must still be subscribed to its source after the replace.
INSERT INTO src VALUES (2);
INSERT INTO src VALUES (3);
SELECT 'after inserts following replace', id FROM mv ORDER BY id;

-- A second replace must keep the subscription working as well.
CREATE OR REPLACE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY id POPULATE AS SELECT id FROM src;
INSERT INTO src VALUES (4);
SELECT 'after second replace and insert', id FROM mv ORDER BY id;

DROP TABLE mv SYNC;
DROP TABLE src SYNC;
