DROP TABLE IF EXISTS s_05138;

CREATE TABLE s_05138
(
    id UInt32,
    s String,
    INDEX ti(s) TYPE text(tokenizer = 'splitByNonAlpha'),
    PROJECTION pj (SELECT s, count() GROUP BY s)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO s_05138 VALUES (1, 'a');

-- The transient form keeps working.
SELECT count() > 0 FROM mergeTreeIndex(currentDatabase(), 's_05138');

-- A persistent table over one of these functions would hold the source table's storage object
-- forever, so DROP TABLE s_05138 SYNC would never return. Every registered name is refused.
CREATE TABLE p_index_05138 AS mergeTreeIndex(currentDatabase(), 's_05138'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_projection_05138 AS mergeTreeProjection(currentDatabase(), 's_05138', 'pj'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_analyze_05138 AS mergeTreeAnalyzeIndexes(currentDatabase(), 's_05138', id = 1); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_text_05138 AS mergeTreeTextIndex(currentDatabase(), 's_05138', 'ti'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_codec_05138 AS mergeTreeCodecBlockCounts(currentDatabase(), 's_05138'); -- { serverError BAD_ARGUMENTS }
CREATE TABLE p_analyze_uuid_05138 AS mergeTreeAnalyzeIndexesUUID('00000000-0000-0000-0000-000000000001', id = 1); -- { serverError BAD_ARGUMENTS }

DROP TABLE s_05138;
