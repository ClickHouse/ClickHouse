DROP TABLE IF EXISTS ttl_repl1;
DROP TABLE IF EXISTS ttl_repl2;

CREATE TABLE ttl_repl1(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00933/ttl_repl', '1')
    PARTITION BY toDayOfMonth(d) ORDER BY x TTL d + INTERVAL 1 DAY;
CREATE TABLE ttl_repl2(d Date, x UInt32) ENGINE ReplicatedMergeTree('/clickhouse/tables/test_00933/ttl_repl', '2')
    PARTITION BY toDayOfMonth(d) ORDER BY x TTL d + INTERVAL 1 DAY;

INSERT INTO TABLE ttl_repl1 VALUES (toDate('2000-10-10 00:00:00'), 100);
INSERT INTO TABLE ttl_repl1 VALUES (toDate('2100-10-10 00:00:00'), 200);

ALTER TABLE ttl_repl1 MODIFY TTL d + INTERVAL 1 DAY;
SYSTEM SYNC REPLICA ttl_repl2;

INSERT INTO TABLE ttl_repl1 VALUES (toDate('2000-10-10 00:00:00'), 300);
INSERT INTO TABLE ttl_repl1 VALUES (toDate('2100-10-10 00:00:00'), 400);

SYSTEM SYNC REPLICA ttl_repl2;

SELECT sleep(1) format Null; -- wait for probable merges after inserts

OPTIMIZE TABLE ttl_repl2 FINAL;
SELECT x FROM ttl_repl2 ORDER BY x;

SHOW CREATE TABLE ttl_repl2;

DROP TABLE ttl_repl1;
DROP TABLE ttl_repl2;
