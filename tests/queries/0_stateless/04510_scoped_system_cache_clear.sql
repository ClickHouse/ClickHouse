SET allow_experimental_analyzer = 1;

CREATE TABLE first (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
CREATE TABLE second (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO first SELECT number, number FROM numbers(1000000);
INSERT INTO second SELECT number, number FROM numbers(1000000);

SELECT count() FROM first WHERE b = 10000 SETTINGS use_query_condition_cache = 1 FORMAT Null;
SELECT count() FROM second WHERE b = 10000 SETTINGS use_query_condition_cache = 1 FORMAT Null;

SELECT count()
FROM system.query_condition_cache
WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'first');
SELECT count()
FROM system.query_condition_cache
WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'second');

SYSTEM CLEAR QUERY CONDITION CACHE FOR TABLE first;

SELECT count()
FROM system.query_condition_cache
WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'first');
SELECT count()
FROM system.query_condition_cache
WHERE table_uuid = (SELECT uuid FROM system.tables WHERE database = currentDatabase() AND name = 'second');

SYSTEM CLEAR MARK CACHE FOR TABLE first;
SYSTEM CLEAR PRIMARY INDEX CACHE FOR TABLE first;
SYSTEM CLEAR UNCOMPRESSED CACHE FOR TABLE first;
SYSTEM CLEAR INDEX MARK CACHE FOR TABLE first;
SYSTEM CLEAR INDEX UNCOMPRESSED CACHE FOR TABLE first;
SYSTEM CLEAR VECTOR SIMILARITY INDEX CACHE FOR TABLE first;
SYSTEM CLEAR TEXT INDEX TOKENS CACHE FOR TABLE first;
SYSTEM CLEAR TEXT INDEX HEADER CACHE FOR TABLE first;
SYSTEM CLEAR TEXT INDEX POSTINGS CACHE FOR TABLE first;
SYSTEM CLEAR TEXT INDEX CACHES FOR TABLE first;

-- Verify the scoped mark cache clear takes effect: marks must be re-loaded from disk afterwards.
SELECT sum(b) FROM first WHERE a < 100000 SETTINGS use_query_condition_cache = 0, load_marks_asynchronously = 0 FORMAT Null;
SYSTEM CLEAR MARK CACHE FOR TABLE first;
SELECT sum(b) FROM first WHERE a < 100000 SETTINGS use_query_condition_cache = 0, load_marks_asynchronously = 0, log_comment = '04510-marks-after-clear' FORMAT Null;

-- Verify the scoped uncompressed cache clear takes effect: a repeated small-range read must miss again.
SELECT sum(b) FROM first WHERE a < 1000 SETTINGS use_uncompressed_cache = 1, use_query_condition_cache = 0, merge_tree_max_rows_to_use_cache = 1000000, merge_tree_max_bytes_to_use_cache = 100000000 FORMAT Null;
SYSTEM CLEAR UNCOMPRESSED CACHE FOR TABLE first;
SELECT sum(b) FROM first WHERE a < 1000 SETTINGS use_uncompressed_cache = 1, use_query_condition_cache = 0, merge_tree_max_rows_to_use_cache = 1000000, merge_tree_max_bytes_to_use_cache = 100000000, log_comment = '04510-uncompressed-after-clear' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT ProfileEvents['LoadedMarksCount'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04510-marks-after-clear';
SELECT ProfileEvents['UncompressedCacheMisses'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '04510-uncompressed-after-clear';

DROP TABLE first;
DROP TABLE second;
