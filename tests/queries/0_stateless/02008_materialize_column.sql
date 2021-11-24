DROP TABLE IF EXISTS tmp;

SET mutations_sync = 2;

CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 20;

ALTER TABLE tmp ADD COLUMN column_with_default String DEFAULT toString(x);
SELECT groupArray(x), groupArray(column_with_default) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN column_with_default String DEFAULT toString(x+1);
SELECT groupArray(x), groupArray(column_with_default) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN column_with_default;
ALTER TABLE tmp MODIFY COLUMN column_with_default String DEFAULT toString(x+2);
SELECT groupArray(x), groupArray(column_with_default) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN column_with_default;
ALTER TABLE tmp MODIFY COLUMN column_with_default String DEFAULT toString(x+3);
SELECT groupArray(x), groupArray(column_with_default) FROM tmp;
ALTER TABLE tmp DROP COLUMN column_with_default;

ALTER TABLE tmp ADD COLUMN column_with_materialized String MATERIALIZED toString(x);
SELECT groupArray(x), groupArray(column_with_materialized) FROM tmp;

ALTER TABLE tmp MODIFY COLUMN column_with_materialized String MATERIALIZED toString(x+1);
SELECT groupArray(x), groupArray(column_with_materialized) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN column_with_materialized;
ALTER TABLE tmp MODIFY COLUMN column_with_materialized String MATERIALIZED toString(x+2);
SELECT groupArray(x), groupArray(column_with_materialized) FROM tmp;

ALTER TABLE tmp MATERIALIZE COLUMN column_with_materialized;
ALTER TABLE tmp MODIFY COLUMN column_with_materialized String MATERIALIZED toString(x+3);
SELECT groupArray(x), groupArray(column_with_materialized) FROM tmp;
ALTER TABLE tmp DROP COLUMN column_with_materialized;

DROP TABLE tmp;

