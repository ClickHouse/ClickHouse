-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS condition_only_provenance_04839;

-- getProjectionOutputToSourceIdentifiers collects every identifier referenced anywhere in a SELECT
-- item to resolve provenance (see 04839_json_shared_regexp_projection_expression.sql). An identifier
-- that appears only in the condition of if()/multiIf() never contributes a value to the output
-- though, so it must not donate its own SHARED REGEXP policy to the result.
CREATE TABLE condition_only_provenance_04839
(
    id UInt64,
    j JSON(max_dynamic_paths=5),
    meta JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO condition_only_provenance_04839 VALUES (1, '{"a":1}', '{"tag_a":1}');

ALTER TABLE condition_only_provenance_04839
    ADD PROJECTION p (SELECT id, if(JSONHas(meta, 'x'), j, j) WHERE id > 0 ORDER BY id);
ALTER TABLE condition_only_provenance_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: this must be 0. meta is referenced only inside the if() condition, never
-- contributing a value to the output, so its SHARED REGEXP rule must not leak into j's type.
SELECT
    'condition-only identifier provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='condition_only_provenance_04839' AND column != 'id' AND active;

DROP TABLE condition_only_provenance_04839;
