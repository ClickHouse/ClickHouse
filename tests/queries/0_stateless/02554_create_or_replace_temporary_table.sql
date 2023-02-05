-- Tags: no-parallel

CREATE OR REPLACE TEMPORARY TABLE tmp (n UInt32) AS SELECT * FROM numbers(10);

SELECT * FROM tmp;

REPLACE TEMPORARY TABLE tmp (s String) AS SELECT 'a' FROM numbers(10);

SELECT * FROM tmp;

CREATE OR REPLACE TEMPORARY TABLE tmp (n UInt32, s String) AS SELECT number, 'a' FROM numbers(10);

SELECT * FROM tmp;

DROP TEMPORARY TABLE tmp;
