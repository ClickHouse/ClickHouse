DROP TABLE IF EXISTS table1;
DROP TABLE IF EXISTS table2;

CREATE TABLE table1
(
	`id` UInt32
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'table2', rand());

CREATE TABLE table2
(
	`id` UInt32
)
ENGINE = Distributed('test_shard_localhost', currentDatabase(), 'table1', rand());

INSERT INTO table1 VALUES (1); -- { serverError 581 }

SELECT * FROM table1; -- { serverError 581 }

SET max_distributed_depth = 0;

-- stack overflow
INSERT INTO table1 VALUES (1); -- { serverError 306 }

-- stack overflow
SELECT * FROM table1; -- { serverError 306 }

DROP TABLE table1;
DROP TABLE table2;
