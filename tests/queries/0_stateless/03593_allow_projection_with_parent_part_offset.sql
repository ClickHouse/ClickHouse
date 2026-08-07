DROP TABLE IF EXISTS 03593_t;

CREATE TABLE 03593_t (
    s String,
    n UInt64,
    PROJECTION prj_s_pos (SELECT _part_offset ORDER BY s))
ENGINE = MergeTree
ORDER BY n
SETTINGS allow_part_offset_column_in_projections=0; -- {serverError BAD_ARGUMENTS}

CREATE OR REPLACE TABLE 03593_t (
    s String,
    n UInt64)
ENGINE = MergeTree
ORDER BY n
SETTINGS allow_part_offset_column_in_projections=0;

ALTER TABLE 03593_t ADD projection prj_s_pos (SELECT _part_offset ORDER BY s); -- {serverError BAD_ARGUMENTS}

SHOW CREATE TABLE 03593_t;

DROP TABLE IF EXISTS 03593_t;
