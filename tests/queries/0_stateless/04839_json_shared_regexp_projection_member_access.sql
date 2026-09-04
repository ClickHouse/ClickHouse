-- Tags: no-fasttest, no-random-settings, no-random-merge-tree-settings, no-replicated-database

SET enable_json_type = 1;

DROP TABLE IF EXISTS member_access_04839;

-- Provenance must follow the member the projection actually reads, not the lone JSON-shaped
-- sibling: reading t.1 (the String) and reparsing it as JSON carries no history.
CREATE TABLE member_access_04839
(
    id UInt64,
    t Tuple(s String, doc JSON(max_dynamic_paths=5, SHARED REGEXP '^tag_'))
)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part=0, min_rows_for_wide_part=0;

INSERT INTO member_access_04839 VALUES (1, ('{"tag_a":1}', '{"tag_a":1}'));

ALTER TABLE member_access_04839 MODIFY COLUMN t Tuple(s String, doc JSON(max_dynamic_paths=5));

ALTER TABLE member_access_04839
    ADD PROJECTION p_sibling (SELECT id, CAST(tupleElement(t, 1) AS JSON) AS reparsed WHERE id > 0 ORDER BY id);
ALTER TABLE member_access_04839 MATERIALIZE PROJECTION p_sibling SETTINGS mutations_sync=1;

ALTER TABLE member_access_04839
    ADD PROJECTION p_member (SELECT id, tupleElement(t, 2) AS doc WHERE id > 0 ORDER BY id);
ALTER TABLE member_access_04839 MATERIALIZE PROJECTION p_member SETTINGS mutations_sync=1;

SELECT 'sibling reparse carries no provenance', countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='member_access_04839' AND name = 'p_sibling' AND column != 'id' AND active;

SELECT 'member read keeps provenance', countIf(position(type, '^tag_') > 0)
FROM system.projection_parts_columns
WHERE database=currentDatabase() AND table='member_access_04839' AND name = 'p_member' AND column != 'id' AND active;

DROP TABLE member_access_04839;
