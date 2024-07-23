DROP TABLE IF EXISTS tp;

CREATE TABLE tp (
    type Int32,
    eventcnt UInt64,
    PROJECTION p (select sum(eventcnt), type group by type)
) engine = ReplacingMergeTree order by type;  -- { serverError NOT_IMPLEMENTED }

CREATE TABLE tp (
    type Int32,
    eventcnt UInt64,
    PROJECTION p (select sum(eventcnt), type group by type)
) engine = ReplacingMergeTree order by type
SETTINGS deduplicate_merge_projection_mode = 'throw';  -- { serverError NOT_IMPLEMENTED }

CREATE TABLE tp (
    type Int32,
    eventcnt UInt64,
    PROJECTION p (select sum(eventcnt), type group by type)
) engine = ReplacingMergeTree order by type
SETTINGS deduplicate_merge_projection_mode = 'drop';

INSERT INTO tp SELECT number%3, 1 FROM numbers(3);

OPTIMIZE TABLE tp FINAL;

-- expecting no projection
SYSTEM FLUSH LOGS;
SELECT
    name,
    part_name
FROM system.projection_parts
WHERE (database = currentDatabase()) AND (`table` = 'tp') AND (active = 1);

ALTER TABLE tp MODIFY SETTING deduplicate_merge_projection_mode = 'throw';

OPTIMIZE TABLE tp DEDUPLICATE;  -- { serverError NOT_IMPLEMENTED }

DROP TABLE tp;

CREATE TABLE tp (
    type Int32,
    eventcnt UInt64,
    PROJECTION p (select sum(eventcnt), type group by type)
) engine = ReplacingMergeTree order by type
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

ALTER TABLE tp MODIFY SETTING deduplicate_merge_projection_mode = 'throw';

OPTIMIZE TABLE tp DEDUPLICATE;  -- { serverError NOT_IMPLEMENTED }

DROP TABLE tp;