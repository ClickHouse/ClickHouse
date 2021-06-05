CREATE TABLE mv_source (a UInt8, b String) ENGINE MergeTree ORDER BY a;
CREATE TABLE mv_target (a UInt8, b String) ENGINE MergeTree ORDER BY a;

CREATE MATERIALIZED VIEW mv1 TO mv_target AS SELECT * FROM mv_source;
CREATE MATERIALIZED VIEW mv2 ENGINE MergeTree ORDER BY a AS SELECT * FROM mv_source;

select 'ALTER TABLE mv1 REFRESH';
ALTER TABLE mv1 REFRESH; -- success
select 'ALTER TABLE mv2 REFRESH';
ALTER TABLE mv2 REFRESH; -- success
select 'ALTER TABLE mv_source REFRESH';
ALTER TABLE mv_source REFRESH;  -- { serverError 56 }

DROP TABLE mv1;
DROP TABLE mv2;
DROP TABLE mv_source;
DROP TABLE mv_target;
