DROP TABLE IF EXISTS 03400_users;
DROP TABLE IF EXISTS 03400_dist_users;

CREATE TABLE 03400_users
(
    `uid` Int16,
    `name` String,
    `age` Int16,
    `version` UInt8
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (uid, name);

INSERT INTO 03400_users VALUES (111, 'John', 33, 1);
INSERT INTO 03400_users VALUES (111, 'John', 33, 2);
INSERT INTO 03400_users VALUES (8888, 'Alice', 50, 1);

CREATE TABLE 03400_dist_users AS 03400_users
ENGINE = Distributed(test_cluster_two_shards, currentDatabase(), 03400_users);

SET max_threads=1;

SELECT *
FROM 03400_dist_users AS l
FINAL
LEFT JOIN
(
    SELECT *
    FROM 03400_dist_users AS d
    FINAL
) AS r ON l.uid = r.uid
ORDER BY l.version
SETTINGS distributed_product_mode = 'local';
