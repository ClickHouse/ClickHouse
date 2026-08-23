CREATE TABLE src (a UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tgt (a UInt16) ENGINE = MergeTree ORDER BY tuple();

CREATE MATERIALIZED VIEW v_table_target TO tgt AS SELECT a FROM src;
CREATE MATERIALIZED VIEW v_view_target TO v_table_target AS SELECT a FROM src;

-- Inserting into a view whose target is another view is refused on both insert routes.
INSERT INTO v_view_target VALUES (1); -- { serverError NOT_IMPLEMENTED }
INSERT INTO v_view_target SELECT 1; -- { serverError NOT_IMPLEMENTED }

-- CREATE ... POPULATE reaches the same hop through the population insert, not a user INSERT.
INSERT INTO src VALUES (2);
CREATE MATERIALIZED VIEW v_populate TO v_table_target POPULATE AS SELECT a FROM src; -- { serverError NOT_IMPLEMENTED }

-- Reaching the same hop as a dependent view keeps the pre-existing behaviour: the insert addresses
-- `src`, the hop is skipped, and the rows that would have gone through `v_view_target` are dropped.
INSERT INTO src VALUES (3);
SELECT 'insert into the source table still succeeds', count() FROM src ORDER BY ALL;

-- A target that is an ordinary table keeps working, on the direct and the dependent route.
INSERT INTO v_table_target VALUES (4);
SELECT 'table target still works', count() FROM tgt;

DROP VIEW v_view_target;
DROP VIEW v_table_target;
DROP TABLE tgt;
DROP TABLE src;
