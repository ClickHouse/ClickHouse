CREATE TABLE test
(
	`id` UInt32, 
	`age` UInt32, 
	`name` String
) 
ENGINE = MergeTree()
ORDER BY id

INSERT INTO test (*) VALUES (1, 20, 'a'), (2, 10, 'b')

SELECT * FROM groupArrayOrderBy('age asc, name desc')(id, age, name)

SELECT * FROM groupArrayOrderBy('id')(id, id)

DROP TABLE test
