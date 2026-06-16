-- Regression test for a logical error (chassert) in KeyCondition::extractAtomFromTree:
-- a constant pointInPolygon polygon argument wrapped in Variant/Dynamic (e.g. if(c, arr, NULL))
-- has a non-Array const type, which tripped `chassert(WhichDataType(const_type).isArray())`
-- during primary-key analysis. The skip-index atom is now skipped for such constants.

DROP TABLE IF EXISTS t_pip_variant;
CREATE TABLE t_pip_variant (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y);
INSERT INTO t_pip_variant VALUES (1, 1) (2, 2) (3, 3) (6, 4) (0, 0);

-- if(...) gives the polygon a Variant(Array(Tuple(...))) type; used to abort the server.
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), if(1, [(0., 0.), (8., 4.), (5., 8.)], NULL));
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), if(0, NULL, [(0., 0.), (8., 4.), (5., 8.)]));
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), multiIf(1, [(0., 0.), (8., 4.), (5., 8.)], NULL));

-- Explicit Variant / Dynamic wrappers around a constant polygon.
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), CAST([(0., 0.), (8., 4.), (5., 8.)] AS Variant(Array(Tuple(Float64, Float64)), UInt8)));
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), CAST([(0., 0.), (8., 4.), (5., 8.)] AS Dynamic));

-- A plain Array constant must still take the skip-index analysis path and return the same result.
SELECT count() FROM t_pip_variant WHERE pointInPolygon((x, y), [(0., 0.), (8., 4.), (5., 8.)]);

DROP TABLE t_pip_variant;
