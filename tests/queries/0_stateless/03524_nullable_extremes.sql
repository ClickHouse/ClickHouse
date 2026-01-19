SELECT '--- int, single part';
CREATE TABLE single_int (k Int64, c Nullable(Int64)) Engine=MergeTree ORDER BY k;
INSERT INTO single_int VALUES (1, 1), (2, 2), (3, NULL);
SELECT c FROM single_int ORDER BY ALL SETTINGS extremes=1;

SELECT '--- int, multi part';
CREATE TABLE multi_int (k Int64, c Nullable(Int64)) Engine=MergeTree ORDER BY k PARTITION BY k;
INSERT INTO multi_int VALUES (1, 1), (2, 2), (3, NULL);
SELECT c FROM multi_int ORDER BY ALL SETTINGS extremes=1;

SELECT '--- float, single part';
CREATE TABLE single_float (k Int64, c Float64) Engine=MergeTree ORDER BY c;
INSERT INTO single_float VALUES (1, 1), (2, 2), (3, 0/0);
SELECT c FROM single_float ORDER BY ALL SETTINGS extremes=1;

SELECT '--- float, multi part';
CREATE TABLE multi_float (k Int64, c Float64) Engine=MergeTree ORDER BY k PARTITION BY k;
INSERT INTO multi_float VALUES (1, 1), (2, 2), (3, 0/0);
SELECT c FROM multi_float ORDER BY ALL SETTINGS extremes=1;
