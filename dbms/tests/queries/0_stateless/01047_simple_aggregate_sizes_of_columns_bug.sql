DROP TABLE IF EXISTS column_size_bug;

CREATE TABLE column_size_bug (date_time DateTime, value SimpleAggregateFunction(sum,UInt64)) ENGINE = AggregatingMergeTree PARTITION BY toStartOfInterval(date_time, INTERVAL 1 DAY) ORDER BY (date_time);

INSERT INTO column_size_bug VALUES(now(),1);
INSERT INTO column_size_bug VALUES(now(),1);

ALTER TABLE column_size_bug DELETE WHERE value=1;

-- wait for DELETE
SELECT sleep(1);

OPTIMIZE TABLE column_size_bug;

DROP TABLE column_size_bug;
