-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS map_predicate_provenance_04839;

-- mapFilter/mapSort/... are thin Map adapters over the same predicate-only array Impls; an
-- identifier used only inside their lambda must not donate its SHARED REGEXP policy either.
CREATE TABLE map_predicate_provenance_04839
(
    id UInt64,
    m Map(String, UInt64),
    meta JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_')
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO map_predicate_provenance_04839 VALUES (1, map('a', 1), '{"tag_a":1}');

ALTER TABLE map_predicate_provenance_04839
    ADD PROJECTION p (SELECT id, mapFilter((k, v) -> JSONHas(meta, 'flag'), m) WHERE id > 0 ORDER BY id);
ALTER TABLE map_predicate_provenance_04839 MATERIALIZE PROJECTION p SETTINGS mutations_sync=1;

-- The regression: this must be 0. meta is referenced only inside mapFilter's predicate lambda,
-- never contributing a value to the output, so its SHARED REGEXP rule must not leak into m's type.
SELECT
    'map predicate-lambda identifier provenance',
    countIf(position(type, 'SHARED REGEXP') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='map_predicate_provenance_04839' AND column != 'id' AND active;

DROP TABLE map_predicate_provenance_04839;
