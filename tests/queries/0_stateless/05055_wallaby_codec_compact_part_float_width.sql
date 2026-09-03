-- A compact part shares one `CompressedStream`, and one codec instance, between every substream whose
-- codec has the same hash. `Wallaby` carries the float width of the column it was created for, so the
-- hash has to include it - otherwise a `Float32` and a `Float64` column of the same part are encoded by
-- whichever of the two the part writer created first, and the result depends on the order of the columns.

SET enable_wallaby_codec = 1;

DROP TABLE IF EXISTS wallaby_narrow_first;
DROP TABLE IF EXISTS wallaby_wide_first;

CREATE TABLE wallaby_narrow_first (n UInt64 CODEC(NONE), f32 Float32 CODEC(Wallaby), f64 Float64 CODEC(Wallaby))
    ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
CREATE TABLE wallaby_wide_first (n UInt64 CODEC(NONE), f64 Float64 CODEC(Wallaby), f32 Float32 CODEC(Wallaby))
    ENGINE = MergeTree ORDER BY n SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO wallaby_narrow_first SELECT number, toFloat32(round(number * 0.25, 2)), round(number * 0.125, 3) FROM numbers(100000);
INSERT INTO wallaby_wide_first SELECT number, round(number * 0.125, 3), toFloat32(round(number * 0.25, 2)) FROM numbers(100000);

SELECT '# Both parts are compact';
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'wallaby_narrow_first' AND active;
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 'wallaby_wide_first' AND active;

SELECT '# The data round-trips exactly';
SELECT count() FROM wallaby_narrow_first WHERE f32 <> toFloat32(round(n * 0.25, 2)) OR f64 <> round(n * 0.125, 3);
SELECT count() FROM wallaby_wide_first WHERE f32 <> toFloat32(round(n * 0.25, 2)) OR f64 <> round(n * 0.125, 3);

SELECT '# The compressed size does not depend on the order of the columns';
SELECT
    (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'wallaby_narrow_first' AND active)
  = (SELECT sum(data_compressed_bytes) FROM system.parts WHERE database = currentDatabase() AND table = 'wallaby_wide_first' AND active);

DROP TABLE wallaby_narrow_first;
DROP TABLE wallaby_wide_first;
