-- NOT_FOUND_COLUMN_IN_BLOCK when querying a view over a table with a row policy
-- where the view does not select the row policy column and optimize_move_to_prewhere is enabled
--
-- The bug was in SourceStepWithFilter::updatePrewhereInfo which re-applied
-- row_level_filter on the already-transformed output_header instead of rebuilding it

DROP TABLE IF EXISTS t_rp;
DROP TABLE IF EXISTS ref_rp;
DROP VIEW IF EXISTS v_rp;

CREATE TABLE t_rp
(
    timestamp DateTime64(9),
    asset_id Int64,
    price Float64,
    source String,
    is_valid Bool DEFAULT true,
    inserted_at DateTime64(9)
)
ENGINE = ReplacingMergeTree(inserted_at)
ORDER BY (asset_id, timestamp);

CREATE TABLE ref_rp (id Int64, name String) ENGINE = MergeTree ORDER BY id;

INSERT INTO ref_rp VALUES (1, 'BTC'), (2, 'ETH'), (3, 'SOL');
INSERT INTO t_rp VALUES
    ('2020-01-01', 1, 100.0, 'src1', true, '2020-01-01'),
    ('2020-01-01', 2, 200.0, 'src2', true, '2020-01-01'),
    ('2020-01-01', 3, 0.0,   'src3', false, '2020-01-01');

CREATE ROW POLICY rp_100003 ON t_rp FOR SELECT USING is_valid = true TO ALL;

CREATE VIEW v_rp AS
SELECT
    toDate(timestamp) AS date,
    ref_rp.name AS asset_name,
    asset_id,
    argMax(price, p.inserted_at) AS price,
    argMax(source, p.inserted_at) AS source
FROM t_rp AS p
INNER JOIN ref_rp ON ref_rp.id = p.asset_id
GROUP BY date, asset_id, asset_name;

SELECT date, asset_name, price, source
FROM v_rp
WHERE date = '2020-01-01' AND price > 0
ORDER BY asset_name
SETTINGS
    optimize_move_to_prewhere = 1,
    allow_experimental_parallel_reading_from_replicas = 2,
    max_parallel_replicas = 3,
    cluster_for_parallel_replicas = 'parallel_replicas',
    parallel_replicas_min_number_of_rows_per_replica = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1;

DROP VIEW v_rp;
DROP ROW POLICY rp_100003 ON t_rp;
DROP TABLE ref_rp;
DROP TABLE t_rp;
