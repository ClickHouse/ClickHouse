DROP TABLE IF EXISTS table2;
CREATE TABLE table2
(
        EventDate Date,
        Id Int32,
        Value Int32
)
Engine = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY Id;

ALTER TABLE table2 MODIFY COLUMN `Value` DEFAULT 'some_string'; --{serverError 6}

ALTER TABLE table2 ADD COLUMN `Value2` DEFAULT 'some_string'; --{serverError 36}

DROP TABLE IF EXISTS table2;
