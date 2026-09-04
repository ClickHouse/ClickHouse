-- A deferred number literal is parsed against the type of the other argument of the function. That
-- type is taken with its wrappers stripped, so a `LowCardinality` argument picks the same reference
-- type as the plain one instead of leaving the literal at its default `Float64`.
-- Note that `LowCardinality(Decimal)` is not a valid type, so only the other numeric types apply.

SET allow_suspicious_low_cardinality_types = 1;

SELECT 'reference type';
SELECT toTypeName(greatest(materialize(toFloat32(1.0)), 2.0)) SETTINGS enable_analyzer = 1;
SELECT toTypeName(greatest(materialize(toLowCardinality(toFloat32(1.0))), 2.0)) SETTINGS enable_analyzer = 1;
SELECT toTypeName(greatest(materialize(CAST(1.0, 'Nullable(Float32)')), 2.0)) SETTINGS enable_analyzer = 1;
SELECT toTypeName(greatest(materialize(CAST(1.0, 'LowCardinality(Nullable(Float32))')), 2.0)) SETTINGS enable_analyzer = 1;

SELECT 'all coordinates share one type';
SELECT length(geohashesInBox(materialize(toFloat32(1.0)), 2.0, 3.0, 4.0, 5)) SETTINGS enable_analyzer = 1;
SELECT length(geohashesInBox(materialize(toLowCardinality(toFloat32(1.0))), 2.0, 3.0, 4.0, 5)) SETTINGS enable_analyzer = 1;

SELECT 'comparison';
SELECT materialize(toLowCardinality(toFloat32(2.5))) = 2.5 SETTINGS enable_analyzer = 1;
SELECT materialize(toLowCardinality(toFloat32(2.5))) = 2.5 SETTINGS enable_analyzer = 0;
SELECT materialize(CAST(1.0, 'LowCardinality(Nullable(Float32))')) < 2.5 SETTINGS enable_analyzer = 1;
SELECT materialize(CAST(1.0, 'LowCardinality(Nullable(Float32))')) < 2.5 SETTINGS enable_analyzer = 0;
