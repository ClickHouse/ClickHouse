DROP TABLE IF EXISTS table_for_ttl;

CREATE TABLE table_for_ttl(
  d DateTime,
  key UInt64,
  value String)
ENGINE = MergeTree()
ORDER BY tuple()
PARTITION BY key;

INSERT INTO table_for_ttl SELECT now() - INTERVAL 2 YEAR, 1, toString(number) from numbers(1000);

INSERT INTO table_for_ttl SELECT now() - INTERVAL 2 DAY, 3, toString(number) from numbers(2000, 1000);

INSERT INTO table_for_ttl SELECT now(), 4, toString(number) from numbers(3000, 1000);

SELECT count() FROM table_for_ttl;

ALTER TABLE table_for_ttl MODIFY TTL d + INTERVAL 1 YEAR SETTINGS materialize_ttl_after_modify = 0;

SELECT count() FROM table_for_ttl;

OPTIMIZE TABLE table_for_ttl FINAL;

SELECT count() FROM table_for_ttl;

ALTER TABLE table_for_ttl MODIFY COLUMN value String TTL d + INTERVAL 1 DAY SETTINGS materialize_ttl_after_modify = 0;

SELECT count(distinct value) FROM table_for_ttl;

OPTIMIZE TABLE table_for_ttl FINAL;

SELECT count(distinct value) FROM table_for_ttl;

OPTIMIZE TABLE table_for_ttl FINAL; -- Just check in logs, that it doesn't run with force again

DROP TABLE IF EXISTS table_for_ttl;
