-- Real-time previews of the query result (the `query_result_previews` setting) are initiator-only:
-- the initiator drops every `PreviewData` packet of a remote query, so a shard that still had
-- previews enabled would snapshot and serialize its intermediate state for nothing (and an older
-- shard in a rolling upgrade would reject the settings as `UNKNOWN_SETTING`). Neither the per-shard
-- `Settings` packet nor the forwarded query *text* may carry the preview settings.

SET query_result_previews = 1;
SET query_result_previews_min_interval_ms = 0;
SET query_result_previews_min_rows = 1;

SELECT '-- previews enabled by the query context';
SELECT intDiv(number, 100000) AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(200000)) GROUP BY k ORDER BY k
SETTINGS log_comment = '05054_query_result_previews_context';

SELECT '-- previews enabled by an inline SETTINGS clause';
SELECT intDiv(number, 100000) AS k, count() AS c FROM remote('127.0.0.{1,2}', numbers(200000)) GROUP BY k ORDER BY k
SETTINGS query_result_previews = 1, query_result_previews_min_interval_ms = 0, query_result_previews_max_result_rows = 7,
    log_comment = '05054_query_result_previews_inline';

SYSTEM FLUSH LOGS query_log;

SELECT '-- the shards received neither the settings nor a query text enabling previews';
SELECT
    log_comment,
    countIf(Settings['query_result_previews'] = '1') AS previews_enabled_on_shard,
    countIf(arrayExists(name -> name LIKE 'query_result_previews%', mapKeys(Settings))) AS preview_settings_on_shard,
    countIf(query LIKE '%query_result_previews%') AS preview_settings_in_query_text
FROM system.query_log
WHERE log_comment IN ('05054_query_result_previews_context', '05054_query_result_previews_inline')
    AND NOT is_initial_query AND type = 'QueryFinish' AND event_date >= yesterday()
GROUP BY log_comment
ORDER BY log_comment;
