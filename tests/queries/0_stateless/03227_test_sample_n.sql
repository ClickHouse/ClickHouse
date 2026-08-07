CREATE TABLE IF NOT EXISTS table_name
(
id UInt64
)
ENGINE = MergeTree()
ORDER BY cityHash64(id)
SAMPLE BY cityHash64(id);

INSERT INTO table_name SELECT rand() from system.numbers limit 10000;
INSERT INTO table_name SELECT rand() from system.numbers limit 10000;
INSERT INTO table_name SELECT rand() from system.numbers limit 10000;
INSERT INTO table_name SELECT rand() from system.numbers limit 10000;
INSERT INTO table_name SELECT rand() from system.numbers limit 10000;

select count() from table_name;
SELECT count() < 50 * 5 FROM (
	SELECT * FROM table_name SAMPLE 50
);

DROP TABLE table_name;
