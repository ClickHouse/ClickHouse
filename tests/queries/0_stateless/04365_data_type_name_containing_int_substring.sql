DROP TABLE IF EXISTS t_04365;

-- A function-like name used in a data-type position whose uppercase contains the
-- substring "INT" (e.g. quantileInterpolatedWeighted) used to be mistaken for a MySQL
-- integer type: its leading (N) group was silently consumed as a display-width modifier
-- and dropped, so formatting was not idempotent and the AST round-trip check aborted
-- with "Inconsistent AST formatting" (LOGICAL_ERROR) in debug builds. These must now
-- produce a clean parse/type error instead.
CREATE TABLE t_04365 (`a` quantileInterpolatedWeighted(0.8)(a, 1)) ENGINE = Memory; -- { clientError SYNTAX_ERROR }
SELECT CAST([1, 2], 'Array(quantileInterpolatedWeighted(0.8)(a, 1))'); -- { serverError SYNTAX_ERROR }
CREATE TABLE t_04365 (`a` Nullable(modulo(quantileInterpolatedWeighted(a, 1), UInt64))) ENGINE = Memory; -- { serverError UNKNOWN_TYPE }

-- The MySQL integer display-width modifier must still be accepted (and ignored).
CREATE TABLE t_04365 (a TINYINT(8), b SMALLINT(16), c INT(32), d BIGINT(64), e INT(), f INT(11) UNSIGNED) ENGINE = Memory;
INSERT INTO t_04365 VALUES (1, 2, 3, 4, 5, 6);
SELECT toTypeName(a), toTypeName(b), toTypeName(c), toTypeName(d), toTypeName(e), toTypeName(f) FROM t_04365;

DROP TABLE t_04365;
