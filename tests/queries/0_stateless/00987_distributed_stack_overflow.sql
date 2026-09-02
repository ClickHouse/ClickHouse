-- Tags: distributed

DROP TABLE IF EXISTS distr0;
DROP TABLE IF EXISTS distr1;
DROP TABLE IF EXISTS distr2;

CREATE TABLE distr (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr); -- { serverError INFINITE_LOOP }

CREATE TABLE distr0 (x UInt8) ENGINE = Distributed(test_shard_localhost, '', distr0); -- { serverError INFINITE_LOOP }

CREATE TABLE distr1 (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr2);
CREATE TABLE distr2 (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr1); -- { serverError INFINITE_LOOP }

DROP TABLE distr1;
