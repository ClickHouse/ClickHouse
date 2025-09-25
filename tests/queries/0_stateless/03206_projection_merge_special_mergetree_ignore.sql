DROP TABLE IF EXISTS tp;

CREATE TABLE tp (
    type Int32,
    eventcnt UInt64,
    PROJECTION p (select sum(eventcnt), type group by type)
) engine = ReplacingMergeTree order by type
SETTINGS deduplicate_merge_projection_mode = 'ignore';

INSERT INTO tp SELECT number%3, 1 FROM numbers(3);
INSERT INTO tp SELECT number%3, 2 FROM numbers(3);

OPTIMIZE TABLE tp DEDUPLICATE;  -- { serverError SUPPORT_IS_DISABLED }

OPTIMIZE TABLE tp FINAL;

SET optimize_use_projections = false, force_optimize_projection = false;

SELECT sum(eventcnt) eventcnt, type
FROM tp
GROUP BY type
ORDER BY eventcnt, type;

SET optimize_use_projections = true, force_optimize_projection = true;

SELECT sum(eventcnt) eventcnt, type
FROM tp
GROUP BY type
ORDER By eventcnt, type;

DROP TABLE tp;
