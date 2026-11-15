CREATE TABLE t (click_city_id UInt32, click_country_id UInt32) Engine = Memory;
CREATE TABLE d_src (id UInt64, country_id UInt8, name String) Engine = Memory;

INSERT INTO t VALUES (0, 0);
INSERT INTO d_src VALUES (0, 0, 'n');

CREATE DICTIONARY d (id UInt32, country_id UInt8, name String)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' DB currentDatabase() table 'd_src'))
LIFETIME(MIN 1 MAX 1)
LAYOUT(HASHED());

SELECT click_country_id FROM t AS cc LEFT JOIN d ON toUInt32(d.id) = cc.click_city_id;
SELECT click_country_id FROM t AS cc LEFT JOIN d ON d.country_id < 99 AND d.id = cc.click_city_id;
