set enable_analyzer=1;

CREATE TABLE bug_table
(
    `date_column` Date,
    `c1` String,
    `c2` String
)
ENGINE =  MergeTree
PARTITION BY toYYYYMM(date_column)
ORDER BY (c1, c2);

INSERT INTO bug_table values
    (toDate(now()),hex(rand()),hex(now())),
    (toDate(now()),hex(rand()),hex(now())),
    (toDate(now()),hex(rand()),hex(now())),
    (toDate(now()),hex(rand()),hex(now())),
    (toDate(now()),hex(rand()),hex(now()));

CREATE TABLE distributed_bug_table
(
date_column Date,
c1 String,
c2 String
)
ENGINE = Distributed('test_cluster_two_shards', currentDatabase(), 'bug_table', cityHash64(c1));

set distributed_product_mode = 'allow';

set prefer_localhost_replica=1;

WITH alias_1 AS
   (SELECT c1,c2 FROM distributed_bug_table)
SELECT c1 from alias_1 where c2 IN (SELECT DISTINCT c2 from alias_1)
FORMAT Null;

set prefer_localhost_replica=0;

WITH alias_1 AS
   (SELECT c1,c2 FROM distributed_bug_table)
SELECT c1 from alias_1 where c2 IN (SELECT DISTINCT c2 from alias_1)
FORMAT Null;
