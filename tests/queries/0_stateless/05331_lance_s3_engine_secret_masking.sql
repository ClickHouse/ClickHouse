-- The credentials of the `LanceS3` table engine and the `lanceS3` table function must be masked in
-- the logged query text, like for the other S3-based engines. The queries fail before reaching
-- the storage (`Lance` is experimental, or not included in the build), but they are still logged.

SET allow_experimental_lance = 0;

CREATE TABLE t_05331 (id UInt64) ENGINE = LanceS3('http://localhost:11111/test/05331.lance', 'ak', 'SEKRIT_ENGINE_05331'); -- { serverError SUPPORT_IS_DISABLED, UNKNOWN_STORAGE }
SELECT * FROM lanceS3('http://localhost:11111/test/05331.lance', 'ak', 'SEKRIT_FUNCTION_05331'); -- { serverError SUPPORT_IS_DISABLED, UNKNOWN_FUNCTION }

SYSTEM FLUSH LOGS query_log;

SELECT
    countIf(query LIKE '%SEKRIT%'),
    countIf(query LIKE '%LanceS3(%[HIDDEN]%') > 0,
    countIf(query LIKE '%lanceS3(%[HIDDEN]%') > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND event_date >= yesterday() AND query LIKE '%05331.lance%' AND query NOT LIKE '%system.query_log%';
