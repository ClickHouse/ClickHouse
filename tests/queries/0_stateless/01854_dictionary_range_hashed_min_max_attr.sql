-- Tags: no-parallel

DROP DICTIONARY IF EXISTS dict_01864;
CREATE DICTIONARY dict_01864
(
    `id` UInt64,
    `value` String
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() TABLE 'does_not_exists'))
LIFETIME(MIN 0 MAX 1000)
LAYOUT(RANGE_HASHED())
RANGE(MIN first MAX last) -- { serverError 489 }
