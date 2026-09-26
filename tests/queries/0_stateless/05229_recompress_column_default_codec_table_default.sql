-- Tags: no-fasttest
-- no-fasttest: needs sz3 library

-- A column without an explicit `CODEC`, or with a `CODEC` that references `Default`, takes its
-- effective codec from the table's default codec. `MergeTreeData::checkLossyRecompressionIsPossible`
-- returns early for such a column, which is only correct because the table default can never be
-- lossy: it is applied without a column data type, and `CompressionCodecFactory::get` refuses to
-- build a lossy codec in that context. Pin both halves of that invariant.

SET enable_sz3_codec = 1;

DROP TABLE IF EXISTS t_recompress_table_default;

-- A lossy table default is rejected up front, at `CREATE` ...
CREATE TABLE t_recompress_table_default (id UInt64, v Float64)
ENGINE = MergeTree ORDER BY id
SETTINGS default_compression_codec = 'SZ3'; -- { serverError BAD_ARGUMENTS }

CREATE TABLE t_recompress_table_default
(
    id UInt64,
    v Float64 CODEC(Delta, Default),
    w Float64,
    INDEX idx v TYPE minmax GRANULARITY 1,
    PROJECTION p (SELECT sum(v), sum(w) GROUP BY id % 2)
)
ENGINE = MergeTree ORDER BY id;

-- ... and at `ALTER TABLE ... MODIFY SETTING`.
ALTER TABLE t_recompress_table_default MODIFY SETTING default_compression_codec = 'SZ3'; -- { serverError BAD_ARGUMENTS }

INSERT INTO t_recompress_table_default SELECT number, number / 3, number FROM numbers(1000);

-- Therefore recompressing a `CODEC(Delta, Default)` column, or one that inherits the table default,
-- is lossless and must not be blocked by the lossy-dependency guard, even though the column has a
-- dependent skip index and projection.
ALTER TABLE t_recompress_table_default RECOMPRESS COLUMN v SETTINGS mutations_sync = 2;
SELECT 'explicit Default', count(), round(sum(v), 6) FROM t_recompress_table_default;

ALTER TABLE t_recompress_table_default RECOMPRESS COLUMN w SETTINGS mutations_sync = 2;
SELECT 'inherited default', count(), round(sum(w), 6) FROM t_recompress_table_default;

SELECT 'projection', sum(v), sum(w) FROM t_recompress_table_default GROUP BY id % 2 ORDER BY 2;

DROP TABLE t_recompress_table_default;
