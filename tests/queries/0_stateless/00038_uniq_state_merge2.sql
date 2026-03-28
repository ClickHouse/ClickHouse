-- Tags: stateful
SET max_bytes_before_external_group_by = '1G';
SET max_bytes_ratio_before_external_group_by = 0;
SELECT topLevelDomain(concat('http://', k)) AS tld, sum(u) AS u, uniqMerge(us) AS us FROM (SELECT domain(URL) AS k, uniq(UserID) AS u, uniqState(UserID) AS us FROM test.hits GROUP BY k) GROUP BY tld ORDER BY u DESC, tld ASC LIMIT 100
