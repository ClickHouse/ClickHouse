-- Regression: `CREATE OR REPLACE TABLE` / `REPLACE TABLE` must keep source-side
-- view-dependency edges pinned to the replaced name. Previously the rename inside
-- `doCreateOrReplaceTable` upgraded to `EXCHANGE TABLES`, which swapped edges with
-- the data; the displaced table was then dropped, taking the dependent-views set
-- with it and leaving the live table with no `[mv]` in `system.tables.dependencies_table`.

DROP TABLE IF EXISTS mv;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS src;

CREATE TABLE src (id Int32, key String) ENGINE = Null;
CREATE TABLE dst (id Int32, key String) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW mv TO dst AS SELECT id, key FROM src;

INSERT INTO src VALUES (1, 'a');
SELECT count() FROM dst;

-- CREATE OR REPLACE TABLE on an existing target: the inner rename upgrades to
-- EXCHANGE. Source-view edges must stay on `src`; the displaced contents are
-- about to be dropped, and dropping them must not strip dependents from `src`.
CREATE OR REPLACE TABLE src (id Int32, key String) ENGINE = Null;

SELECT dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name = 'src';

INSERT INTO src VALUES (2, 'b');
SELECT count() FROM dst;

-- REPLACE TABLE: same code path (`doCreateOrReplaceTable`), same expectation.
REPLACE TABLE src (id Int32, key String) ENGINE = Null;

SELECT dependencies_table
FROM system.tables
WHERE database = currentDatabase() AND name = 'src';

INSERT INTO src VALUES (3, 'c');
SELECT count() FROM dst;

DROP TABLE mv;
DROP TABLE dst;
DROP TABLE src;
