-- A `FixedString` is compared with a `String` zero-padded. An `Enum` is compared with it through its name, so
-- the padding of the `FixedString` must not make them different, although the conversion of `FixedString` to
-- `String` keeps it. The primary key must not prune the matching rows either.

SELECT materialize(toFixedString('7', 4)) = CAST('7', 'Enum8(\'7\' = 3)'), materialize(toFixedString('7', 4)) != CAST('7', 'Enum8(\'7\' = 3)');
SELECT materialize(toFixedString('7', 4)) = materialize(CAST('7', 'Enum8(\'7\' = 3)'));

DROP TABLE IF EXISTS t_fixed_string_enum;
CREATE TABLE t_fixed_string_enum (v FixedString(4)) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO t_fixed_string_enum VALUES ('7'), ('3'), ('V0'), ('zz'), ('aa'), ('bb');

SELECT count() FROM t_fixed_string_enum WHERE v = CAST('7', 'Enum8(\'7\' = 3)');
SELECT count() FROM t_fixed_string_enum WHERE v = CAST('7', 'Enum8(\'7\' = 3)') SETTINGS use_primary_key = 0, optimize_use_implicit_projections = 0;
SELECT count() FROM t_fixed_string_enum WHERE v != CAST('7', 'Enum8(\'7\' = 3)');
SELECT count() FROM t_fixed_string_enum WHERE v < CAST('8', 'Enum8(\'8\' = 3)');
SELECT count() FROM t_fixed_string_enum WHERE v < CAST('8', 'Enum8(\'8\' = 3)') SETTINGS use_primary_key = 0, optimize_use_implicit_projections = 0;
-- A name wider than the key matches no key value.
SELECT count() FROM t_fixed_string_enum WHERE v = CAST('77777', 'Enum8(\'77777\' = 3)');

-- The equality still prunes like the same comparison with a `String` constant.
SELECT (SELECT groupArray(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fixed_string_enum WHERE v = CAST('7', 'Enum8(\'7\' = 3)') SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Granules: %/%')
    = (SELECT groupArray(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fixed_string_enum WHERE v = '7' SETTINGS optimize_use_implicit_projections = 0) WHERE explain LIKE '%Granules: %/%');
SELECT count() > 0 FROM (EXPLAIN indexes = 1 SELECT count() FROM t_fixed_string_enum WHERE v = CAST('7', 'Enum8(\'7\' = 3)') SETTINGS optimize_use_implicit_projections = 0)
WHERE explain LIKE '%Granules: 2/6%';

DROP TABLE t_fixed_string_enum;
