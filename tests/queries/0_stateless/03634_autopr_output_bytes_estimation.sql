-- Tags: stateful, no-random-settings

SET optimize_read_in_order=0, query_plan_read_in_order=0, local_filesystem_read_prefetch=0, merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability=0, local_filesystem_read_method='pread_threadpool', use_uncompressed_cache=0;

SET enable_parallel_replicas=0, enable_automatic_parallel_replicas=0, parallel_replicas_local_plan=1, parallel_replicas_index_analysis_only_on_coordinator=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';

SET enable_parallel_replicas=0, enable_automatic_parallel_replicas=2;


SELECT COUNT(*) FROM test.hits WHERE AdvEngineID <> 0 FORMAT Null SETTINGS log_comment='query_1';

SELECT COUNT(DISTINCT SearchPhrase) FROM test.hits FORMAT Null SETTINGS log_comment='query_5';

SELECT MobilePhoneModel, COUNT(DISTINCT UserID) AS u FROM test.hits WHERE MobilePhoneModel <> '' GROUP BY MobilePhoneModel ORDER BY u DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_10';

SELECT SearchPhrase, COUNT(*) AS c FROM test.hits WHERE SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_12';

SELECT UserID, COUNT(*) FROM test.hits GROUP BY UserID ORDER BY COUNT(*) DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_15';

SELECT COUNT(*) FROM test.hits WHERE URL LIKE '%google%' FORMAT Null SETTINGS log_comment='query_20';

SELECT SearchPhrase, MIN(URL), COUNT(*) AS c FROM test.hits WHERE URL LIKE '%google%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_21';

SELECT SearchPhrase, MIN(URL), MIN(Title), COUNT(*) AS c, COUNT(DISTINCT UserID) FROM test.hits WHERE Title LIKE '%Google%' AND URL NOT LIKE '%.google.%' AND SearchPhrase <> '' GROUP BY SearchPhrase ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_22';

SELECT * FROM test.hits WHERE URL LIKE '%google%' ORDER BY EventTime LIMIT 10 FORMAT Null SETTINGS log_comment='query_23';

SELECT REGEXP_REPLACE(Referer, '^https?://(?:www\.)?([^/]+)/.*$', '\1') AS k, AVG(length(Referer)) AS l, COUNT(*) AS c, MIN(Referer) FROM test.hits WHERE Referer <> '' GROUP BY k HAVING COUNT(*) > 100000 ORDER BY l DESC LIMIT 25 FORMAT Null SETTINGS log_comment='query_28';

SELECT 1, URL, COUNT(*) AS c FROM test.hits GROUP BY 1, URL ORDER BY c DESC LIMIT 10 FORMAT Null SETTINGS log_comment='query_34';

SET enable_parallel_replicas=0, enable_automatic_parallel_replicas=0;

SYSTEM FLUSH LOGS query_log;

-- Just checking that the estimation is not too far off (within 75% error)
WITH
    [32+32+32, 2097152+1048576+1048576, 252512+133120+133120, 6389760+4069632+2129920, 1180160+589824+589824, 32+32, 22560+4224+3136, 41536+37088+8832, 30000, 19931136+5726464+5406720, 135266304+72351744+67633152] AS expected_bytes,
    --[32, 2097152, 252512, 6389760, 1180160, 32, 22560, 41536, 16192, 19931136, 135266304] AS expected_bytes,
    arrayJoin(arrayMap(x -> (untuple(x.1), x.2), arrayZip(res, expected_bytes))) AS res
SELECT format('{} {} {}', res.1, res.2, res.3)
FROM
(
    SELECT groupArray((log_comment, output_bytes)) AS res
    FROM (
      SELECT log_comment, ProfileEvents['RuntimeDataflowStatisticsOutputBytes'] output_bytes
      FROM system.query_log
      WHERE (event_date >= yesterday()) AND (event_time >= (NOW() - toIntervalMinute(15))) AND (current_database = currentDatabase()) AND (log_comment LIKE 'query_%') AND (type = 'QueryFinish')
      ORDER BY event_time_microseconds
    )
)
WHERE (greatest(res.2, res.3) / least(res.2, res.3)) > 3 AND NOT (res.2 < 100 AND res.3 < 100);

