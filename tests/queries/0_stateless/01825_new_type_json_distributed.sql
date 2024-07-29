-- Tags: no-fasttest

SET allow_experimental_json_type = 1;

DROP TABLE IF EXISTS t_json_local;
DROP TABLE IF EXISTS t_json_dist;

CREATE TABLE t_json_local(data JSON) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_json_dist AS t_json_local ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), t_json_local);

INSERT INTO t_json_local FORMAT JSONAsObject {"k1": 2, "k2": {"k3": "qqq", "k4": [44, 55]}}
;

SELECT data, JSONAllPathsWithTypes(data) FROM t_json_dist;
SELECT data.k1, data.k2.k3, data.k2.k4 FROM t_json_dist;

DROP TABLE IF EXISTS t_json_local;
DROP TABLE IF EXISTS t_json_dist;
