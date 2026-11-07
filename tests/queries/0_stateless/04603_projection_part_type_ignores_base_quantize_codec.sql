-- Tags: no-fasttest

-- A projection's part format is chosen from the projection's own columns, not the base table's. A
-- `Quantized('product')` column needs a Wide part (its per-part codebook cannot be split per granule),
-- so it forces the base part to Wide. A projection that does not materialize that column must not be
-- dragged to Wide by it: choosePartFormat must inspect the projection's columns, not the whole table.

SET allow_experimental_codecs = 1;

DROP TABLE IF EXISTS t_proj_pq_format SYNC;

CREATE TABLE t_proj_pq_format
(
    id UInt32,
    vec Array(Float32) CODEC(Quantized('product', 8, 4, 2)),
    PROJECTION p (SELECT id, count() GROUP BY id)
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '10G', min_rows_for_wide_part = 0;

INSERT INTO t_proj_pq_format SELECT number, [1., 2., 3., 4., 5., 6., 7., 8.] FROM numbers(10);

-- The base part carries the `Quantized('product')` column, so it must be Wide.
SELECT part_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_proj_pq_format' AND active;

-- The projection materializes only id and count(), so the pq column does not apply and it stays Compact.
SELECT part_type FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_proj_pq_format' AND parent_name = 'all_1_1_0' AND name = 'p' AND active;

DROP TABLE t_proj_pq_format SYNC;
