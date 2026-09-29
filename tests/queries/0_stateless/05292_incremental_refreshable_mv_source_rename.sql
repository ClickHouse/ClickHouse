-- Tags: atomic-database
-- The source of an incremental refreshable materialized view cannot be renamed or exchanged:
-- the view's cursor would be applied to a different table and silently skip its rows.

DROP TABLE IF EXISTS src_rename_mv;
DROP TABLE IF EXISTS src_rename_append_mv;
DROP TABLE IF EXISTS src_rename_tgt;
DROP TABLE IF EXISTS src_rename_src;
DROP TABLE IF EXISTS src_rename_staged;
DROP TABLE IF EXISTS src_rename_moved;

CREATE TABLE src_rename_src (k UInt64) ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

CREATE TABLE src_rename_staged AS src_rename_src;
CREATE TABLE src_rename_tgt (k UInt64) ENGINE = MergeTree ORDER BY k;

CREATE MATERIALIZED VIEW src_rename_mv
    REFRESH EVERY 10 YEAR APPEND INCREMENTAL
    TO src_rename_tgt EMPTY
    AS SELECT k FROM src_rename_src;

INSERT INTO src_rename_src VALUES (1);
SYSTEM REFRESH VIEW src_rename_mv;
SYSTEM WAIT VIEW src_rename_mv;

RENAME TABLE src_rename_src TO src_rename_moved; -- { serverError HAVE_DEPENDENT_OBJECTS }
EXCHANGE TABLES src_rename_src AND src_rename_staged; -- { serverError HAVE_DEPENDENT_OBJECTS }
EXCHANGE TABLES src_rename_staged AND src_rename_src; -- { serverError HAVE_DEPENDENT_OBJECTS }
CREATE OR REPLACE TABLE src_rename_src (k UInt64) ENGINE = MergeTree ORDER BY k; -- { serverError HAVE_DEPENDENT_OBJECTS }

-- The source is intact and the view keeps reading it.
INSERT INTO src_rename_src VALUES (2);
SYSTEM REFRESH VIEW src_rename_mv;
SYSTEM WAIT VIEW src_rename_mv;
SELECT 'after_rejected_renames', groupArray(k) FROM (SELECT k FROM src_rename_tgt ORDER BY k);

-- Renaming the view itself and other tables is allowed.
RENAME TABLE src_rename_staged TO src_rename_moved;
RENAME TABLE src_rename_moved TO src_rename_staged;
RENAME TABLE src_rename_mv TO src_rename_mv_renamed;
RENAME TABLE src_rename_mv_renamed TO src_rename_mv;

-- A non-incremental refreshable view does not restrict its source.
DROP TABLE src_rename_mv;
CREATE MATERIALIZED VIEW src_rename_append_mv
    REFRESH EVERY 10 YEAR APPEND
    TO src_rename_tgt EMPTY
    AS SELECT k FROM src_rename_src;
EXCHANGE TABLES src_rename_src AND src_rename_staged;
SELECT 'exchanged_without_incremental_view', count() FROM src_rename_src;

DROP TABLE src_rename_append_mv;
DROP TABLE src_rename_tgt;
DROP TABLE src_rename_src;
DROP TABLE src_rename_staged;
