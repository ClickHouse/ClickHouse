SET allow_experimental_live_view = 1;
SET insert_distributed_sync = 1;

DROP TABLE IF EXISTS lv;
DROP TABLE IF EXISTS visits;
DROP TABLE IF EXISTS visits_layer;

CREATE TABLE visits(StartDate Date) ENGINE MergeTree ORDER BY(StartDate);
CREATE TABLE visits_layer(StartDate Date) ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

CREATE LIVE VIEW lv AS SELECT foo.x FROM (SELECT StartDate AS x FROM visits_layer) AS foo ORDER BY foo.x;

INSERT INTO visits_layer (StartDate) VALUES ('2020-01-01');
INSERT INTO visits_layer (StartDate) VALUES ('2020-01-02');

SELECT * FROM lv;

DROP TABLE visits;
DROP TABLE visits_layer;

DROP TABLE lv;
