drop table IF EXISTS local;
drop table IF EXISTS dist;
CREATE TABLE local(`dummy` UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dist AS default.local ENGINE = Distributed(localhost_cluster, currentDatabase(), local) where current_database = currentDatabase();
SET prefer_localhost_replica = 1;
WITH 'Hello' AS `alias` SELECT `alias` FROM default.dist GROUP BY `alias`;
