DROP TABLE IF EXISTS distr0;
DROP TABLE IF EXISTS distr1;
DROP TABLE IF EXISTS distr2;

CREATE TABLE distr (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr); -- { serverError 269 }

CREATE TABLE distr0 (x UInt8) ENGINE = Distributed(test_shard_localhost, '', distr0); -- { serverError 269 }

CREATE TABLE distr1 (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr2);
CREATE TABLE distr2 (x UInt8) ENGINE = Distributed(test_shard_localhost, currentDatabase(), distr1);

SELECT * FROM distr1; -- { serverError 581 }
SELECT * FROM distr2; -- { serverError 581 }

DROP TABLE distr1;
DROP TABLE distr2;
