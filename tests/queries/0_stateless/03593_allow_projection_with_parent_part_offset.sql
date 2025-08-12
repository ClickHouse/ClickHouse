DROP TABLE IF EXISTS 03593_t;

SET allow_projection_with_parent_part_offset=0;

CREATE TABLE 03593_t (
    s String,
    n UInt64,
    PROJECTION prj_s_pos (SELECT _part_offset ORDER BY s))
ENGINE = MergeTree ORDER BY n; -- {serverError ILLEGAL_PROJECTION}

DROP TABLE IF EXISTS 03593_t;
