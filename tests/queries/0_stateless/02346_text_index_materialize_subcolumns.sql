-- Tests that text indexes can be created and used on subcolumns

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (data JSON(a String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab (data) VALUES ('{"a": "aaa", "b": "bbb"}');

SET mutations_sync = 2;

ALTER TABLE tab ADD INDEX a_idx(data.a) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab MATERIALIZE INDEX a_idx;

ALTER TABLE tab ADD INDEX b_idx(data.b::String) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab MATERIALIZE INDEX b_idx;

SELECT sum(secondary_indices_compressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;

SELECT count() FROM tab WHERE data.a = 'aaa' SETTINGS force_data_skipping_indices = 'a_idx';
SELECT count() FROM tab WHERE data.b::String = 'bbb' SETTINGS force_data_skipping_indices = 'b_idx';

DROP TABLE tab;

-- Test the same, but for compact parts

CREATE TABLE tab (id UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS min_bytes_for_wide_part = 100000000;

INSERT INTO tab (id) VALUES (1);

ALTER TABLE tab ADD COLUMN data JSON(a String);

SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active;

ALTER TABLE tab ADD INDEX a_idx(data.a) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab MATERIALIZE INDEX a_idx;

ALTER TABLE tab ADD INDEX b_idx(data.b::String) TYPE text(tokenizer = splitByNonAlpha);
ALTER TABLE tab MATERIALIZE INDEX b_idx;

-- Check that column 'data' was materialized on MATERIALIZE INDEX query.
SELECT column FROM system.parts_columns WHERE database = currentDatabase() AND table = 'tab' AND active ORDER BY column;
SELECT sum(secondary_indices_compressed_bytes) > 0 FROM system.parts WHERE database = currentDatabase() AND table = 'tab' AND active;

SELECT count() FROM tab WHERE data.a = 'aaa' SETTINGS force_data_skipping_indices = 'a_idx';
SELECT count() FROM tab WHERE data.b::String = 'bbb' SETTINGS force_data_skipping_indices = 'b_idx';

DROP TABLE tab;
