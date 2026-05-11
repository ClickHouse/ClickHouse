-- Tags: no-parallel
SET allow_experimental_named_scalars = 1;

-- Query-snapshot semantics: the value resolved at analysis time is stable
-- for the whole query, even across multiple reads. Also exercises the
-- defined-scalar path of getNamedScalarOrDefault.
-- Refresh-fires-after-scheduled-tick is covered by 03801.

DROP NAMED SCALAR IF EXISTS snap_v;

CREATE NAMED SCALAR snap_v REFRESH EVERY 1 SECOND AS SELECT toUInt64(now());

-- Two reads of the same scalar in one query must return the same value.
SELECT getNamedScalar('snap_v') = getNamedScalar('snap_v');

-- Defined scalar path captures value/type before execution.
SELECT getNamedScalarOrDefault('snap_v', toUInt64(0)) > 0;

DROP NAMED SCALAR snap_v;
