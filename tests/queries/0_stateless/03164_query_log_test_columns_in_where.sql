set log_queries=1;
set log_queries_min_type='QUERY_FINISH';

DROP TABLE IF EXISTS 03164_query_log_test_columns_in_where;
CREATE TABLE 03164_query_log_test_columns_in_where
(
    a Int64, b Int64, c Int64,
    INDEX idx_c(c) TYPE minmax
) ENGINE = MergeTree PARTITION BY a ORDER BY b SETTINGS index_granularity=10;

INSERT INTO 03164_query_log_test_columns_in_where SELECT number, number+1000, number+10000 FROM numbers(100);

select count(*) from 03164_query_log_test_columns_in_where where a between 1 and 30 and b between 1001 and 1020 and c between 10001 and 10010;

SYSTEM FLUSH LOGS;

SELECT
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'TotalParts')] as tp,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'PartitionParts')] as pp,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'SelectedParts')] as sp,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'TotalMarks')] as tm,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'PrimaryKeyMarks')] as pm,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'SelectedMarks')] as sm,
    ProfileEvents.Values[indexOf(ProfileEvents.Names, 'PrimaryKeyNotUsed')] as pnu,
    arrayStringConcat(columns_in_where, '.')
FROM
    system.query_log
WHERE
    current_database=currentDatabase() and
    query = 'select count(*) from 03164_query_log_test_columns_in_where where a between 1 and 30 and b between 1001 and 1020 and c between 10001 and 10010;';

