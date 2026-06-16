DROP TABLE IF EXISTS null_subcolumns;

SELECT 'Nullable';
CREATE TABLE null_subcolumns (id UInt32, n Nullable(String)) ENGINE = MergeTree ORDER BY id;

INSERT INTO null_subcolumns VALUES (1, 'foo') (2, NULL) (3, NULL) (4, 'abc');

SELECT count() FROM null_subcolumns WHERE n.null;
SELECT count() FROM null_subcolumns PREWHERE n.null;

-- Check, that subcolumns will be available after restart.
DETACH TABLE null_subcolumns;
ATTACH TABLE null_subcolumns;

SELECT count() FROM null_subcolumns WHERE n.null;
SELECT count() FROM null_subcolumns PREWHERE n.null;

DROP TABLE null_subcolumns;
DROP TABLE IF EXISTS map_subcolumns;

SELECT 'Map';
CREATE TABLE map_subcolumns (id UInt32, m Map(String, UInt32)) ENGINE = MergeTree ORDER BY id;
INSERT INTO map_subcolumns VALUES (1, map('a', 1, 'b', 2)) (2, map('a', 3, 'c', 4)), (3, map('b', 5, 'c', 6, 'd', 7));

SELECT count() FROM map_subcolumns WHERE has(m.keys, 'a');
SELECT count() FROM map_subcolumns PREWHERE has(m.keys, 'b');

SELECT count() FROM map_subcolumns WHERE arrayMax(m.values) > 3;
SELECT count() FROM map_subcolumns PREWHERE arrayMax(m.values) > 3;

DETACH TABLE map_subcolumns;
ATTACH TABLE map_subcolumns;

SELECT count() FROM map_subcolumns WHERE has(m.keys, 'a');
SELECT count() FROM map_subcolumns PREWHERE has(m.keys, 'b');

SELECT id, m.size0 FROM map_subcolumns;
SELECT count() FROM map_subcolumns WHERE m.size0 > 2;

DROP TABLE map_subcolumns;
