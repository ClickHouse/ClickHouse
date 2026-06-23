DROP TABLE IF EXISTS repro;

CREATE TABLE repro (Id UInt64, EventId UInt64) ENGINE = MergeTree ORDER BY Id;
INSERT INTO repro SELECT number, number FROM numbers(1000);

SELECT EventId
FROM repro
WHERE _part_starting_offset + _part_offset < 100
ORDER BY Id DESC
LIMIT 10
;
