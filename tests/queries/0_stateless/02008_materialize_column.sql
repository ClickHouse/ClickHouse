DROP TABLE IF EXISTS tmp;

SET mutations_sync = 2;

CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 20;

ALTER TABLE tmp MATERIALIZE COLUMN x; -- { serverError BAD_ARGUMENTS }

ALTER TABLE tmp ADD COLUMN s String DEFAULT toString(x);
SELECT arraySort(arraySort(groupArray(x))), groupArray(s) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+1);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+2);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp CLEAR COLUMN s; -- Need to clear because MATERIALIZE COLUMN won't override past values;
ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+3);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;
ALTER TABLE tmp DROP COLUMN s;

ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+1);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+2);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+3);
SELECT arraySort(groupArray(x)), groupArray(s) FROM tmp;

ALTER TABLE tmp ADD COLUMN s1 String DEFAULT toString(x);
ALTER TABLE tmp MODIFY COLUMN s1 String MATERIALIZED toString(x+10);
ALTER TABLE tmp ADD COLUMN s2 String DEFAULT toString(x);
ALTER TABLE tmp MODIFY COLUMN s2 String MATERIALIZED toString(x+20);
ALTER TABLE tmp ADD COLUMN s3 String DEFAULT toString(x);
ALTER TABLE tmp MODIFY COLUMN s3 String MATERIALIZED toString(x+30);
ALTER TABLE tmp MATERIALIZE COLUMNS;
SELECT arraySort(groupArray(x)), groupArray(s), groupArray(s1), groupArray(s2), groupArray(s3) FROM tmp;
ALTER TABLE tmp DROP COLUMN s;
ALTER TABLE tmp DROP COLUMN s1;
ALTER TABLE tmp DROP COLUMN s2;
ALTER TABLE tmp DROP COLUMN s3;

ALTER TABLE tmp MATERIALIZE COLUMNS;  -- { serverError BAD_ARGUMENTS }

DROP TABLE tmp;
