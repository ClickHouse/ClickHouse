SET allow_projection_column_list_in_replicated_metadata = 1;
SET distributed_ddl_output_mode = 'none';

DROP TABLE IF EXISTS t_projection_codec_type_change;

-- An existing untyped declaration follows the `SELECT` output type. Changing that type must
-- resolve the codec again without requiring the settings that gated the original declaration.
CREATE TABLE t_projection_codec_type_change
(
    k UInt64,
    x Float32,
    y UInt8,
    PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
)
ENGINE = MergeTree ORDER BY k;

SELECT 'initial', codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_type_change';

-- `Gorilla` on an integer would be suspicious for a new declaration, but the stored declaration
-- remains valid and its inferred width must change from four bytes to eight.
ALTER TABLE t_projection_codec_type_change MODIFY COLUMN x UInt64;

SELECT 'widened', codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_type_change';

-- Changing only the projection settings in the same `ALTER` does not introduce a new codec.
ALTER TABLE t_projection_codec_type_change
    MODIFY COLUMN x Float32,
    MODIFY PROJECTION p (x CODEC(Gorilla)) AS (SELECT k, x ORDER BY k)
        WITH SETTINGS (index_granularity = 128);

ALTER TABLE t_projection_codec_type_change MODIFY COLUMN y UInt16;

SELECT 'altered', codecs FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_codec_type_change';

DROP TABLE t_projection_codec_type_change;
