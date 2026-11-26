-- Star topology: hub connected to 5 spokes, each through its own hub key column.
-- Exercises getNeighborhood on a high-degree center node.
-- Each join predicate references a distinct hub column, so every equality forms its own
-- equivalence class and no transitive spoke-to-spoke predicates can be derived: the join
-- graph remains a true star even with enable_join_transitive_predicates (default on).
-- {S1, S2} is not a valid complement because there is no direct predicate
-- between S1 and S2; DPhyp never generates it since single-node emission
-- only extends through actual neighborhood edges.
-- DPhyp and DPsize must return the same result hash.

SET allow_experimental_analyzer = 1;
SET query_plan_optimize_join_order_limit = 10;
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET query_plan_join_swap_table = 'auto';
SET enable_join_runtime_filters = 0;

CREATE TABLE st5_hub (k1 UInt32, k2 UInt32, k3 UInt32, k4 UInt32, k5 UInt32) ENGINE = MergeTree() PRIMARY KEY k1 SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s1  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s2  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s3  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s4  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';
CREATE TABLE st5_s5  (id UInt32, hub_id UInt32) ENGINE = MergeTree() PRIMARY KEY id SETTINGS auto_statistics_types = 'uniq';

INSERT INTO st5_hub SELECT number, number, number, number, number FROM numbers(100);
INSERT INTO st5_s1  SELECT number, number % 100 FROM numbers(10);
INSERT INTO st5_s2  SELECT number, number % 100 FROM numbers(30);
INSERT INTO st5_s3  SELECT number, number % 100 FROM numbers(60);
INSERT INTO st5_s4  SELECT number, number % 100 FROM numbers(80);
INSERT INTO st5_s5  SELECT number, number % 100 FROM numbers(100);

SELECT sum(sipHash64(s1.id, s2.id, s3.id, s4.id, s5.id))
FROM st5_hub hub
JOIN st5_s1 s1 ON hub.k1 = s1.hub_id
JOIN st5_s2 s2 ON hub.k2 = s2.hub_id
JOIN st5_s3 s3 ON hub.k3 = s3.hub_id
JOIN st5_s4 s4 ON hub.k4 = s4.hub_id
JOIN st5_s5 s5 ON hub.k5 = s5.hub_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dphyp', enable_parallel_replicas = 0;

-- DPsize must produce the same result.
SELECT sum(sipHash64(s1.id, s2.id, s3.id, s4.id, s5.id))
FROM st5_hub hub
JOIN st5_s1 s1 ON hub.k1 = s1.hub_id
JOIN st5_s2 s2 ON hub.k2 = s2.hub_id
JOIN st5_s3 s3 ON hub.k3 = s3.hub_id
JOIN st5_s4 s4 ON hub.k4 = s4.hub_id
JOIN st5_s5 s5 ON hub.k5 = s5.hub_id
SETTINGS query_plan_optimize_join_order_algorithm = 'dpsize', enable_parallel_replicas = 0;

DROP TABLE st5_hub;
DROP TABLE st5_s1;
DROP TABLE st5_s2;
DROP TABLE st5_s3;
DROP TABLE st5_s4;
DROP TABLE st5_s5;
