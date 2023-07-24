DROP TABLE IF EXISTS date_t;
CREATE TABLE date_t (id UInt32, value1 String, date1 Date) ENGINE ReplacingMergeTree() ORDER BY id;

EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) <> 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) <> 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) < 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) < 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) > 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) > 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) <= 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) <= 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) >= 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) >= 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYear(date1) BETWEEN 1993 AND 1997 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYear(date1) BETWEEN 1993 AND 1997 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE (toYear(date1) = 1993 OR toYear(date1) = 1994) AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE (toYear(date1) = 1993 OR toYear(date1) = 1994) AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1, toYear(date1) as year1 FROM date_t WHERE year1 = 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1, toYear(date1) as year1 FROM date_t WHERE year1 = 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE 1993 > toYear(date1) AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE 1993 > toYear(date1) AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t PREWHERE toYear(date1) = 1993 WHERE id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t PREWHERE toYear(date1) = 1993 WHERE id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE id BETWEEN 1 AND 3 HAVING toYear(date1) = 1993;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE id BETWEEN 1 AND 3 HAVING toYear(date1) = 1993 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199300 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199300 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199313 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199313 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) = 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) <> 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) <> 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) < 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) < 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) > 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) > 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) <= 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) <= 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE toYYYYMM(date1) >= 199203 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE toYYYYMM(date1) >= 199203 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date_t WHERE (toYYYYMM(date1) >= 199203 OR toYear(date1) = 1993) AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date_t WHERE (toYYYYMM(date1) >= 199203 OR toYear(date1) = 1993) AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
DROP TABLE date_t;

DROP TABLE IF EXISTS datetime_t;
CREATE TABLE datetime_t (id UInt32, value1 String, date1 Datetime) ENGINE ReplacingMergeTree() ORDER BY id;

EXPLAIN SYNTAX SELECT value1 FROM datetime_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM datetime_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM datetime_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM datetime_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
DROP TABLE datetime_t;

DROP TABLE IF EXISTS date32_t;
CREATE TABLE date32_t (id UInt32, value1 String, date1 Date32) ENGINE ReplacingMergeTree() ORDER BY id;

EXPLAIN SYNTAX SELECT value1 FROM date32_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date32_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM date32_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM date32_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
DROP TABLE date32_t;

DROP TABLE IF EXISTS datetime64_t;
CREATE TABLE datetime64_t (id UInt32, value1 String, date1 Datetime64) ENGINE ReplacingMergeTree() ORDER BY id;

EXPLAIN SYNTAX SELECT value1 FROM datetime64_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM datetime64_t WHERE toYear(date1) = 1993 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
EXPLAIN SYNTAX SELECT value1 FROM datetime64_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3;
EXPLAIN QUERY TREE run_passes=1 SELECT value1 FROM datetime64_t WHERE toYYYYMM(date1) = 199312 AND id BETWEEN 1 AND 3 SETTINGS allow_experimental_analyzer=1;
DROP TABLE datetime64_t;
