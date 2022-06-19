-- Tags: no-parallel

DROP DICTIONARY IF EXISTS system.dict1;

CREATE DICTIONARY IF NOT EXISTS system.dict1
(
    bytes_allocated UInt64,
    element_count Int32,
    loading_start_time DateTime
)
PRIMARY KEY bytes_allocated
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' PASSWORD '' TABLE 'dictionaries' DB 'system'))
LIFETIME(0)
LAYOUT(hashed());

SELECT dictGetInt32('system.dict1', 'element_count', toUInt64(dict_key)) AS join_key,
       toTimeZone(dictGetDateTime('system.dict1', 'loading_start_time', toUInt64(dict_key)), 'UTC') AS datetime
FROM (select 1 AS dict_key) js1
LEFT JOIN (SELECT toInt32(2) AS join_key) js2
USING (join_key)
WHERE now() >= datetime;

DROP DICTIONARY IF EXISTS system.dict1;
