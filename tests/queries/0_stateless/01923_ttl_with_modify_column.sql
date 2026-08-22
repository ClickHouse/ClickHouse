DROP TABLE IF EXISTS t_ttl_modify_column;

CREATE TABLE t_ttl_modify_column
(
    InsertionDateTime DateTime,
    TTLDays Int32 DEFAULT CAST(365, 'Int32')
)
ENGINE = MergeTree
ORDER BY tuple()
TTL InsertionDateTime + toIntervalDay(TTLDays)
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_modify_column VALUES (now(), 23);

SET mutations_sync = 2;

ALTER TABLE t_ttl_modify_column modify column TTLDays Int16 DEFAULT CAST(365, 'Int16');

INSERT INTO t_ttl_modify_column VALUES (now(), 23);

SELECT sum(rows), groupUniqArray(type) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_modify_column' AND column = 'TTLDays' AND active;

DROP TABLE IF EXISTS t_ttl_modify_column;

CREATE TABLE t_ttl_modify_column (InsertionDateTime DateTime)
ENGINE = MergeTree
ORDER BY tuple()
TTL InsertionDateTime + INTERVAL 3 DAY
SETTINGS min_bytes_for_wide_part = 0;

INSERT INTO t_ttl_modify_column VALUES (now());

ALTER TABLE t_ttl_modify_column MODIFY COLUMN InsertionDateTime Date;

INSERT INTO t_ttl_modify_column VALUES (now());

SELECT sum(rows), groupUniqArray(type) FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ttl_modify_column' AND column = 'InsertionDateTime' AND active;

ALTER TABLE t_ttl_modify_column MODIFY COLUMN InsertionDateTime Float32; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

DROP TABLE IF EXISTS t_ttl_modify_column;
