-- https://github.com/ClickHouse/ClickHouse/issues/107273
-- Tags: no-random-merge-tree-settings

DROP TABLE IF EXISTS t_sp;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS realt;
DROP TABLE IF EXISTS dst;
DROP VIEW IF EXISTS mv;

-- Source data whose Bool column serializes Sparse (mostly default values).
CREATE TABLE t_sp (id UInt64, _cdc_is_deleted Bool)
ENGINE = MergeTree ORDER BY id SETTINGS ratio_of_defaults_for_sparse_serialization = 0.95;
INSERT INTO t_sp SELECT number, 0 FROM numbers(10000);

CREATE TABLE src   (id UInt64, _cdc_is_deleted Bool) ENGINE = MergeTree ORDER BY id;
CREATE TABLE realt (id UInt64, _cdc_is_deleted Bool) ENGINE = MergeTree ORDER BY id;
INSERT INTO realt SELECT number, 1 FROM numbers(10);   -- full serialization
CREATE TABLE dst   (id UInt64, _cdc_is_deleted Bool) ENGINE = MergeTree ORDER BY id;

-- UNION nested inside a FROM subquery: one branch's header is Sparse, the sibling's is full.
CREATE MATERIALIZED VIEW mv TO dst AS
  SELECT id, _cdc_is_deleted FROM (
    SELECT id, _cdc_is_deleted FROM src
    UNION ALL
    SELECT id, _cdc_is_deleted FROM realt
  );

-- The inserted block carries a Sparse _cdc_is_deleted; previously threw
-- "Block structure mismatch in UnionStep stream" while pushing to the view.
INSERT INTO src SELECT id, _cdc_is_deleted FROM t_sp;

SELECT count(), sum(_cdc_is_deleted) FROM dst;

DROP VIEW mv;
DROP TABLE dst;
DROP TABLE realt;
DROP TABLE src;
DROP TABLE t_sp;
