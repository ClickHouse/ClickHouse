CREATE TABLE raw_data
(
	`id` UInt8,
	`data` String
)
ENGINE = MergeTree
ORDER BY id;


INSERT INTO raw_data SELECT number, number
FROM numbers(10);

CREATE VIEW raw_data_parameterized AS
SELECT *
FROM raw_data
WHERE (id >= {id_from:UInt8}) AND (id <= {id_to:UInt8});

SELECT t1.id
FROM raw_data_parameterized(id_from = 0, id_to = 50000) t1
ORDER BY t1.id;
