DROP TABLE IF EXISTS dummy;
DROP TABLE IF EXISTS v_dummy;
CREATE TABLE dummy (date Date, value String) Engine = MergeTree(date, (date), 8192);
CREATE MATERIALIZED VIEW v_dummy Engine = MergeTree(date, (date), 8192) AS SELECT date, value FROM dummy;

SELECT '## Add 2 partitions';
INSERT INTO dummy VALUES ('2018-02-14', 'A'), ('2018-03-14', 'B');
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;
SELECT '## Drop partition 201802';
ALTER TABLE v_dummy DROP PARTITION 201802;
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;
SELECT '## Drop partition 201803';
ALTER TABLE v_dummy DROP PARTITION 201803;
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;

SELECT '## Add 3 partitions';
INSERT INTO dummy VALUES ('2018-01-14', 'A'), ('2018-02-14', 'B'), ('2018-03-14', 'C');
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;
SELECT '## Detach partition 201803';
ALTER TABLE v_dummy DETACH PARTITION 201803;
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;
SELECT '## Attach partition 201803';
ALTER TABLE v_dummy ATTACH PARTITION 201803;
SELECT DISTINCT partition FROM system.parts WHERE table = '.inner.v_dummy' AND active ORDER BY partition;