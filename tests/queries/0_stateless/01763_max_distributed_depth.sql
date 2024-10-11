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

CREATE TABLE tt7 as tt6 ENGINE = Distributed('test_shard_localhost', '', 'tt6', rand()); -- {serverError INFINITE_LOOP}

DROP TABLE tt6;
