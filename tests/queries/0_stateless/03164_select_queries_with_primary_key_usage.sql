-- Tests for ProfileEvents "SelectedMarksByPrimaryKeyUsage"
set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

DROP TABLE IF EXISTS 03164_select_queries_with_primary_key_usage;
CREATE TABLE 03164_select_queries_with_primary_key_usage
(
    a Int64, b Int64, c Int64
) ENGINE = MergeTree ORDER BY a;

ALTER TABLE 03164_select_queries_with_primary_key_usage ADD PROJECTION b_projection (
    SELECT * ORDER BY b
);

INSERT INTO 03164_select_queries_with_primary_key_usage SELECT number, number + 100, number + 1000 FROM numbers(100);

SELECT count(*) FROM 03164_select_queries_with_primary_key_usage WHERE a >= 0 and b <= 100;
SELECT count(*) FROM 03164_select_queries_with_primary_key_usage WHERE a >= 0;
SELECT count(*) FROM 03164_select_queries_with_primary_key_usage WHERE b >= 100;
SELECT count(*) FROM 03164_select_queries_with_primary_key_usage WHERE c >= 1000;
SELECT count(*) FROM 03164_select_queries_with_primary_key_usage;

SYSTEM FLUSH LOGS;

SELECT
    IF (ProfileEvents['SelectQueriesWithPrimaryKeyUsage'] > 0, 1, 0) AS queries_with_primary_key_usage
FROM
    system.query_log
WHERE
    current_database = currentDatabase()
    AND query LIKE 'SELECT count(*) FROM 03164_select_queries_with_primary_key_usage%'
ORDER BY query
FORMAT Vertical;

DROP TABLE IF EXISTS 03164_select_queries_with_primary_key_usage;
