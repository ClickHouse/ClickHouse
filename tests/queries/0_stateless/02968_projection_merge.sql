SELECT 'ReplacingMergeTree';
DROP TABLE IF EXISTS tp;
CREATE TABLE tp
(
    `type` Int32,
    `eventcnt` UInt64,
    PROJECTION p
    (
        SELECT type,sum(eventcnt)
        GROUP BY type
    )
)
ENGINE = ReplacingMergeTree
ORDER BY type
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO tp SELECT number%3, 1 FROM numbers(3);
INSERT INTO tp SELECT number%3, 2 FROM numbers(3);

OPTIMIZE TABLE tp FINAL;

set parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1, optimize_aggregation_in_order = 0;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 0, force_optimize_projection = 0;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;


SELECT 'CollapsingMergeTree';
DROP TABLE IF EXISTS tp;
CREATE TABLE tp
(
    `type` Int32,
    `eventcnt` UInt64,
    `sign` Int8,
    PROJECTION p
    (
        SELECT type,sum(eventcnt)
        GROUP BY type
    )
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY type
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO tp SELECT number % 3, 1, 1 FROM numbers(3);
INSERT INTO tp SELECT number % 3, 1, -1 FROM numbers(3);
INSERT INTO tp SELECT number % 3, 2, 1 FROM numbers(3);

OPTIMIZE TABLE tp FINAL;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 0, force_optimize_projection = 0;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

-- Actually we don't need to test all 3 engines Replacing/Collapsing/VersionedCollapsing,
-- Because they share the same logic of 'reduce number of rows during merges'
SELECT 'VersionedCollapsingMergeTree';
DROP TABLE IF EXISTS tp;
CREATE TABLE tp
(
    `type` Int32,
    `eventcnt` UInt64,
    `sign` Int8,
    `version` UInt8,
    PROJECTION p
    (
        SELECT type,sum(eventcnt)
        GROUP BY type
    )
)
ENGINE = VersionedCollapsingMergeTree(sign,version)
ORDER BY type
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO tp SELECT number % 3, 1, -1, 0 FROM numbers(3);
INSERT INTO tp SELECT number % 3, 2, 1, 1 FROM numbers(3);
INSERT INTO tp SELECT number % 3, 1, 1, 0 FROM numbers(3);

OPTIMIZE TABLE tp FINAL;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 0, force_optimize_projection = 0;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

SELECT 'DEDUPLICATE ON MergeTree';
DROP TABLE IF EXISTS tp;
CREATE TABLE tp
(
    `type` Int32,
    `eventcnt` UInt64,
    PROJECTION p
    (
        SELECT type,sum(eventcnt)
        GROUP BY type
    )
)
ENGINE = MergeTree
ORDER BY type
SETTINGS deduplicate_merge_projection_mode = 'rebuild';

INSERT INTO tp SELECT number % 3, 1 FROM numbers(3);
INSERT INTO tp SELECT number % 3, 2 FROM numbers(3);

OPTIMIZE TABLE tp FINAL DEDUPLICATE BY type;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 0, force_optimize_projection = 0;

SELECT type,sum(eventcnt) FROM tp GROUP BY type ORDER BY type
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

