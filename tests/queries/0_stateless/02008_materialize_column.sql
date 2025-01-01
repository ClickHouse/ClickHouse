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
ALTER TABLE tmp DROP COLUMN s;

DROP TABLE tmp;

