-- Tags: stateful
SET max_bytes_before_external_group_by = '1G';
SET max_bytes_ratio_before_external_group_by = 0;
SELECT k, any(u) AS u, uniqMerge(us) AS us FROM (SELECT domain(URL) AS k, uniq(UserID) AS u, uniqState(UserID) AS us FROM test.hits GROUP BY k) GROUP BY k ORDER BY u DESC, k ASC LIMIT 100
