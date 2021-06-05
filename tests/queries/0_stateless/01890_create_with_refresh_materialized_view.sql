CREATE TABLE mv_source (a UInt8, b String) ENGINE MergeTree ORDER BY a;
CREATE TABLE mv_target (a UInt8, b String) ENGINE MergeTree ORDER BY a;

INSERT INTO mv_source VALUES (1, 2);

CREATE MATERIALIZED VIEW mv1 TO mv_target WITH REFRESH 5 AS SELECT * FROM mv_source;
CREATE MATERIALIZED VIEW mv2 ENGINE MergeTree ORDER BY a WITH REFRESH 5 AS SELECT * FROM mv_source;

INSERT INTO mv_source VALUES (2, 3);

select 'SELECT * FROM mv1';
SELECT * FROM mv1;
select 'SELECT * FROM mv2';
SELECT * FROM mv2;

DROP TABLE mv1;
DROP TABLE mv2;
DROP TABLE mv_source;
DROP TABLE mv_target;
