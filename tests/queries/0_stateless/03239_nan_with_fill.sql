SELECT nan ORDER BY 1 WITH FILL;
SELECT -nan ORDER BY 1 WITH FILL;
SELECT 0./0. ORDER BY 1 WITH FILL;
SELECT 1 ORDER BY nan WITH FILL FROM 1;

CREATE TABLE t0 (c0 Float32) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (1),(inf),(2);
SELECT 1 FROM t0 ORDER BY t0.c0 WITH FILL;
DROP TABLE t0;

CREATE TABLE t0 (c0 Float64) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (-inf),(1),(2);
SELECT c0 FROM t0 ORDER BY t0.c0 WITH FILL;
DROP TABLE t0;

CREATE TABLE t0 (c0 Float64) ENGINE = Memory();
INSERT INTO TABLE t0 (c0) VALUES (inf),(1),(2);
SELECT c0 FROM t0 ORDER BY t0.c0 DESC WITH FILL;
DROP TABLE t0;

-- Rows up to TO are generated before a NaN or an infinity.
SELECT arrayJoin([0., inf]) AS x ORDER BY x WITH FILL TO 3;
SELECT arrayJoin([0., nan]) AS x ORDER BY x WITH FILL TO 3;
SELECT toFloat32(arrayJoin([0., inf])) AS x ORDER BY x WITH FILL TO 3;
SELECT arrayJoin([0., -inf]) AS x ORDER BY x DESC WITH FILL TO -3;
SELECT arrayJoin([0., 5., inf, nan]) AS x ORDER BY x WITH FILL TO 7;
SELECT x FROM (SELECT arrayJoin([inf]) AS x) ORDER BY x WITH FILL FROM 0 TO 3;
SELECT a, b FROM (SELECT arrayJoin([(1, 0.), (1, inf), (2, 0.)]) AS t, t.1 AS a, t.2 AS b) ORDER BY a WITH FILL, b WITH FILL TO 3;
SELECT a, b FROM (SELECT arrayJoin([(1, 0.), (2, inf)]) AS t, t.1 AS a, t.2 AS b) ORDER BY a WITH FILL, b WITH FILL FROM 0 TO 3;
SELECT arrayJoin([0., inf]) AS x ORDER BY x WITH FILL TO 5 STALENESS 3;
SELECT arrayJoin([0., nan]) AS x ORDER BY x WITH FILL STALENESS 3;
SELECT x FROM (SELECT arrayJoin([0., inf]) AS x ORDER BY x WITH FILL TO 3) ORDER BY x SETTINGS optimize_sorting_by_input_stream_properties = 1;

-- NaN sorts first under NULLS FIRST.
SELECT arraySort(groupArray(x)) FROM (SELECT arrayJoin([0., nan]) AS x ORDER BY x NULLS FIRST WITH FILL FROM -1 TO 3);

-- A non-finite value in an earlier fill key does not move the rows of the later key after it.
SELECT a, b FROM (SELECT arrayJoin([(0., 0), (inf, 0)]) AS t, t.1 AS a, t.2 AS b) ORDER BY a WITH FILL, b WITH FILL TO 3;
SELECT a, b FROM (SELECT arrayJoin([(-inf, 0), (0., 0)]) AS t, t.1 AS a, t.2 AS b) ORDER BY a WITH FILL, b WITH FILL TO 3;
