-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS compound_lambda_param_04839;

-- arrayMap(x -> x.doc, arr): member access on lambda formal x must attribute arr as donor,
-- preserving historical SHARED REGEXP provenance when the rule is retired at table level.
CREATE TABLE compound_lambda_param_04839
(
    id UInt64,
    arr Array(Tuple(doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'), n UInt8))
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO compound_lambda_param_04839 VALUES (1, [('{"tag_a":1}', 1)]);

ALTER TABLE compound_lambda_param_04839 MODIFY COLUMN arr Array(Tuple(doc JSON(max_dynamic_paths=5), n UInt8));

ALTER TABLE compound_lambda_param_04839
    ADD PROJECTION p (SELECT id, arrayMap(x -> x.doc, arr) WHERE id > 0 ORDER BY id);
ALTER TABLE compound_lambda_param_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: compound access on lambda formal retains historical SHARED REGEXP policy.
SELECT
    'compound lambda parameter access preserves provenance',
    countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='compound_lambda_param_04839' AND column != 'id' AND active;

DROP TABLE compound_lambda_param_04839;
