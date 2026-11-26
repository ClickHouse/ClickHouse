-- A value outside the set key type's range is not a member, even when the set holds 0 or the value the probe would wrap to.
DROP TABLE IF EXISTS t_set_placeholder;
DROP TABLE IF EXISTS t_set_placeholder_nullable;
DROP TABLE IF EXISTS t_set_placeholder_pair;

CREATE TABLE t_set_placeholder (k UInt64) ENGINE = Set;
INSERT INTO t_set_placeholder VALUES (0), (18446744073709551615);
SELECT v, v IN t_set_placeholder, v NOT IN t_set_placeholder, nullIn(v, t_set_placeholder) FROM (SELECT arrayJoin([-1, 0, 1, NULL]::Array(Nullable(Int64))) AS v) ORDER BY v NULLS LAST;
SELECT -1 IN (SELECT toUInt64(0)) SETTINGS transform_null_in = 1;

CREATE TABLE t_set_placeholder_nullable (k Nullable(UInt64)) ENGINE = Set;
INSERT INTO t_set_placeholder_nullable VALUES (0), (NULL);
SELECT v, nullIn(v, t_set_placeholder_nullable) FROM (SELECT arrayJoin([-1, 0, NULL]::Array(Nullable(Int64))) AS v) ORDER BY v NULLS LAST;

CREATE TABLE t_set_placeholder_pair (a UInt64, b UInt8) ENGINE = Set;
INSERT INTO t_set_placeholder_pair VALUES (0, 0);
SELECT a, b, (a, b) IN t_set_placeholder_pair FROM (SELECT arrayJoin([(-1, 0), (0, 300), (0, 0)]::Array(Tuple(Int64, Int64))) AS p, p.1 AS a, p.2 AS b) ORDER BY a, b;

DROP TABLE t_set_placeholder;
DROP TABLE t_set_placeholder_nullable;
DROP TABLE t_set_placeholder_pair;
