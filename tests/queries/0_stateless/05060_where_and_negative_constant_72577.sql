-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/72577
-- A view and a `MergeTree` table with identical content returned different results for
-- `WHERE (-29184 AND c0)`: the view returned all rows, the table returned nothing.
-- While analyzing which parts of the table can be skipped, the conjunction was reduced to its
-- only remaining argument - the constant `-29184` - and a truncating cast of it to `UInt8`
-- gave `0`, because its low byte is zero. The filter therefore looked always false and every
-- part of the table was pruned.
-- Related: https://github.com/ClickHouse/ClickHouse/issues/101269

DROP TABLE IF EXISTS t0;
DROP VIEW IF EXISTS v1;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0 (c0 UInt16, c1 Float32, c2 Float32) ENGINE = MergeTree ORDER BY c0;

INSERT INTO t0 (c0, c1, c2) VALUES (-27014, 1.2868087092231241e+37, -9.341932810328251e+37), (11216, -4.665561590130922e+37, 6.035246731399415e+37), (18072, 3.4763063837489624e+37, 7.3840935694251035e+37), (-9660, 2.2517320078368264e+37, -6.772849060869504e+36), (467, -9.125825313458806e+37, 2.4000255594628933e+37), (-8721, -1.4349931791827511e+37, 9.63251307785958e+36), (-24755, 4.280030308988983e+36, 5.72775030945182e+37), (20857, 5.429209064295016e+37, 3.326420068276781e+37), (-27789, 1.1982731785700689e+36, 6.065375557970564e+37);

CREATE VIEW v1 AS SELECT groupBitAnd(c0) AS c0, c1 AS c1, c2 AS c2 FROM t0 GROUP BY c1, c2;
CREATE TABLE t1 ENGINE = MergeTree ORDER BY c0 AS SELECT groupBitAnd(c0) AS c0, c1 AS c1, c2 AS c2 FROM t0 GROUP BY c1, c2;

-- Both must return the same 9 rows.
SELECT 'view', count() FROM v1 WHERE (-29184 AND c0);
SELECT 'table', count() FROM t1 WHERE (-29184 AND c0);
SELECT c0 FROM t1 WHERE (-29184 AND c0) ORDER BY ALL;

-- The same, for integer constants of different types whose low byte is zero.
SELECT -256, count() FROM t1 WHERE (c0 > 0) AND -256;
SELECT 256, count() FROM t1 WHERE (c0 > 0) AND 256;
SELECT -65536, count() FROM t1 WHERE (c0 > 0) AND -65536;
SELECT -4294967296, count() FROM t1 WHERE (c0 > 0) AND -4294967296;

-- A constant that is actually false must still prune everything.
SELECT 0, count() FROM t1 WHERE (c0 > 0) AND 0;

DROP TABLE t1;
DROP VIEW v1;
DROP TABLE t0;
