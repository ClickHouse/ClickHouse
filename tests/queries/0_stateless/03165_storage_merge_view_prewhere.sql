-- Tags: distributed

DROP TABLE IF EXISTS ids;
DROP TABLE IF EXISTS data;
DROP TABLE IF EXISTS data2;

CREATE TABLE ids (id UUID, whatever String) Engine=MergeTree ORDER BY tuple();
INSERT INTO ids VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', 'whatever');

CREATE TABLE data (id UUID, event_time DateTime, status String) Engine=MergeTree ORDER BY tuple();
INSERT INTO data VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', '2000-01-01', 'CREATED');

CREATE TABLE data2 (id UUID, event_time DateTime, status String) Engine=MergeTree ORDER BY tuple();
INSERT INTO data2 VALUES ('a1451105-722e-4fe7-bfaa-65ad2ae249c2', '2000-01-02', 'CREATED');

SELECT
    id,
    whatever
FROM ids AS l
INNER JOIN merge(currentDatabase(), 'data*') AS s ON l.id = s.id
WHERE (status IN ['CREATED', 'CREATING'])
ORDER BY event_time DESC
;

SELECT
    id,
    whatever
FROM ids AS l
INNER JOIN clusterAllReplicas(test_cluster_two_shards, merge(currentDatabase(), 'data*')) AS s ON l.id = s.id
WHERE (status IN ['CREATED', 'CREATING'])
ORDER BY event_time DESC
;

SELECT
    id,
    whatever
FROM ids AS l
INNER JOIN view(SELECT * FROM merge(currentDatabase(), 'data*')) AS s ON l.id = s.id
WHERE (status IN ['CREATED', 'CREATING'])
ORDER BY event_time DESC
;
