-- Checks that no bad things happen when the table optimizes the row order to improve compressability during just simple insert.

DROP TABLE IF EXISTS tab;

CREATE TABLE tab (name String, event Int8) ENGINE = MergeTree ORDER BY name SETTINGS allow_experimental_optimized_row_order = true;
INSERT INTO tab VALUES ('Igor', 3), ('Egor', 1), ('Egor', 2), ('Igor', 2), ('Igor', 1);

SELECT * FROM tab SETTINGS max_threads=1;

DROP TABLE tab;
