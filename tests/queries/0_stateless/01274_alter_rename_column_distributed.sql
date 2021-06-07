set insert_distributed_sync = 1;

DROP TABLE IF EXISTS visits;
DROP TABLE IF EXISTS visits_dist;

CREATE TABLE visits(StartDate Date, Name String) ENGINE MergeTree ORDER BY(StartDate);
CREATE TABLE visits_dist AS visits ENGINE Distributed(test_cluster_two_shards_localhost,  currentDatabase(), 'visits', rand());

INSERT INTO visits_dist (StartDate, Name) VALUES ('2020-01-01', 'hello');
INSERT INTO visits_dist (StartDate, Name) VALUES ('2020-01-02', 'hello2');

ALTER TABLE visits RENAME COLUMN Name TO Name2;
ALTER TABLE visits_dist RENAME COLUMN Name TO Name2;

SELECT * FROM visits_dist ORDER BY StartDate, Name2;

DROP TABLE visits;
DROP TABLE visits_dist;

