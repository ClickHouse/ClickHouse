-- Tags: no-parallel, zookeeper

DROP ROW POLICY IF EXISTS 02911_rowpolicy ON default.* ON CLUSTER test_shard_localhost;
DROP USER IF EXISTS 02911_user ON CLUSTER test_shard_localhost;

CREATE USER 02911_user ON CLUSTER test_shard_localhost;
CREATE ROW POLICY 02911_rowpolicy ON CLUSTER test_shard_localhost ON default.* USING 1 TO 02911_user;

DROP ROW POLICY 02911_rowpolicy ON default.* ON CLUSTER test_shard_localhost;
DROP USER 02911_user ON CLUSTER test_shard_localhost;
