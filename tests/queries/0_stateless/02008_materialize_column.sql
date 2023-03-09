DROP TABLE IF EXISTS tmp;

SET mutations_sync = 2;

CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 20;

ALTER TABLE tmp MATERIALIZE COLUMN x; -- { serverError 36 }

ALTER TABLE tmp ADD COLUMN s String DEFAULT toString(x);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+1);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+2);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String DEFAULT toString(x+3);
SELECT groupArray(x), groupArray(s) FROM tmp;
ALTER TABLE tmp DROP COLUMN s;

ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+1);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+2);
SELECT groupArray(x), groupArray(s) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN s;
ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(x+3);
SELECT groupArray(x), groupArray(s) FROM tmp;
ALTER TABLE tmp DROP COLUMN s;

DROP TABLE tmp;

