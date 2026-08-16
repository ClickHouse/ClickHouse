-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS array_predicate_provenance_04839;

-- collectValueCarryingIdentifierNames must not treat an identifier referenced only inside a
-- predicate/comparator lambda (arrayFilter, arraySort, ...) as value-carrying: the lambda's
-- return value never becomes part of such a function's output, only the array argument's own
-- elements do (see 04839_json_shared_regexp_projection_condition_only_identifier.sql for the
-- analogous if()/multiIf() case).
CREATE TABLE array_predicate_provenance_04839
(
    id UInt64,
    arr Array(JSON(max_dynamic_paths=5)),
    meta JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO array_predicate_provenance_04839 VALUES (1, ['{"a":1}'], '{"tag_a":1}');

ALTER TABLE array_predicate_provenance_04839
    ADD PROJECTION p (SELECT id, arrayFilter(x -> JSONHas(meta, 'flag'), arr) WHERE id > 0 ORDER BY id);
ALTER TABLE array_predicate_provenance_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: this must be 0. meta is referenced only inside arrayFilter's predicate lambda,
-- never contributing a value to the output, so its SHARED REGEXP rule must not leak into arr's element type.
SELECT
    'array predicate-lambda identifier provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='array_predicate_provenance_04839' AND column != 'id' AND active;

DROP TABLE array_predicate_provenance_04839;
