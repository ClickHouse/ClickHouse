# Tags: no-fasttest
# no-fasttest: requires object storage

CREATE TABLE small(sid UInt64, s Array(Int64)) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64, b Array(Int64)) ENGINE = MergeTree ORDER BY bid;

insert into small select number, [number] from numbers(0, 1000);
insert into big select number, [number] from numbers(0, 100000);

SET
    make_distributed_plan=1,
    enable_parallel_replicas=0,
    distributed_plan_optimize_exchanges=1;

EXPLAIN SELECT count()
FROM big, small
WHERE (small.sid = (big.bid + 1) % 5000);

SELECT count()
FROM big, small
WHERE (small.sid = (big.bid + 1) % 5000);

SELECT '------------';

EXPLAIN SELECT count()
FROM big, small
WHERE (small.sid = (big.bid + 1) % 5000)
SETTINGS distributed_plan_optimize_exchanges=0;

SELECT '------------';

EXPLAIN SELECT count()
FROM small, big
WHERE (small.sid = (big.bid + 1) % 5000);

SELECT count()
FROM small, big
WHERE (small.sid = (big.bid + 1) % 5000);
