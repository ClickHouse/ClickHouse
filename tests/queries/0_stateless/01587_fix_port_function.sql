-- 1088
SELECT port('localhost:1088');
SELECT port('http://localhost:1088');
SELECT port('https://localhost:1088');
-- 0
SELECT port('localhost.:1088');
SELECT port('.:1088');
--1088
SELECT port('http://1.2.3.4:1088');
SELECT port('1.2.3.4:1088');
SELECT port('yandex:1088');
SELECT port('http://clickhouse:1088');
SELECT port('clickhouse:1088');
SELECT port('clickhouse.tech:1088');
SELECT port('http://clickhouse.tech:1088');
