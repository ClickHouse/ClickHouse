-- Tags: distributed

SET prefer_localhost_replica = 1;

DROP TABLE IF EXISTS tt6;

CREATE TABLE tt6
(
	`id` UInt32,
	`first_column` UInt32,
	`second_column` UInt32,
	`third_column` UInt32,
	`status` String

)
ENGINE = Distributed('test_shard_localhost', '', 'tt7', rand());

DROP TABLE IF EXISTS tt7;

CREATE TABLE tt7 as tt6 ENGINE = Distributed('test_shard_localhost', '', 'tt6', rand());

INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 581 }

SELECT * FROM tt6; -- { serverError 581 }

SET max_distributed_depth = 0;

-- stack overflow
INSERT INTO tt6 VALUES (1, 1, 1, 1, 'ok'); -- { serverError 306}

-- stack overflow
SELECT * FROM tt6; -- { serverError 306 }

DROP TABLE tt6;
DROP TABLE tt7;
