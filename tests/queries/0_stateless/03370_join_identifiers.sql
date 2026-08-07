DROP TABLE IF EXISTS t0;
DROP TABLE IF EXISTS t1;

CREATE TABLE t0
(
    `id` INT UNSIGNED NOT NULL,
    `rev` INT UNSIGNED NOT NULL,
    `content` varchar(200) NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (id, rev);

CREATE TABLE t1
(
    `id` INT UNSIGNED NOT NULL,
    `rev` INT UNSIGNED NOT NULL,
    `content` varchar(200) NOT NULL
)
ENGINE = MergeTree
PRIMARY KEY (id, rev);

INSERT INTO TABLE t0 VALUES (1,1,'1');
INSERT INTO TABLE t1 VALUES (1,1,'1');

SELECT SUM(t1.rev) AS aggr
FROM t1
INNER JOIN t0 AS right_0 ON t1.id = right_0.id
INNER JOIN t1 AS right_1 ON t1.id = right_1.id;

DROP TABLE t0;
DROP TABLE t1;
