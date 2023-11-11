-- Tags: no-parallel, zookeeper

DROP ROW POLICY IF EXISTS 02911_rowpolicy ON default.* ON CLUSTER default;
DROP USER IF EXISTS 02911_user ON CLUSTER default;

CREATE USER 02911_user ON CLUSTER default;
CREATE ROW POLICY 02911_rowpolicy ON CLUSTER default ON default.* USING 1 TO 02911_user;

DROP ROW POLICY 02911_rowpolicy ON default.* ON CLUSTER default;
DROP USER 02911_user ON CLUSTER default;
