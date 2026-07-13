DROP TABLE IF EXISTS 03402_data;

CREATE TABLE 03402_data (id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO 03402_data SELECT * FROM numbers(100);

SELECT avg(id) FROM 03402_data SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 0;
SELECT avg(id) FROM 03402_data SETTINGS max_threads = 0, max_streams_to_max_threads_ratio = 0;
SELECT avg(id) FROM 03402_data SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 0.2;

SELECT '';

SELECT id FROM 03402_data ORDER BY id LIMIT 1 SETTINGS max_threads = 4, max_streams_to_max_threads_ratio = 0;
SELECT id FROM 03402_data ORDER BY id LIMIT 1 SETTINGS max_threads = 0, max_streams_to_max_threads_ratio = 0;
SELECT id FROM 03402_data ORDER BY id LIMIT 1 SETTINGS max_threads = 2, max_streams_to_max_threads_ratio = 0.2;

DROP TABLE 03402_data;
