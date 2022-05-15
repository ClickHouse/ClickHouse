CREATE TABLE test
(
	`id` UInt32, 
	`age` UInt32, 
	`name` String
) 
ENGINE = MergeTree()
ORDER BY id

INSERT INTO test (*) VALUES (1, 20, 'a'), (2, 10, 'b')

SELECT * FROM countOrderBy('age asc, name desc')(id, age, name)

SELECT * FROM sumOrderBy('age')(id, age)

DROP TABLE test