-- https://github.com/ClickHouse/ClickHouse/issues/117183
-- https://github.com/ClickHouse/ClickHouse/issues/117184
-- https://github.com/ClickHouse/ClickHouse/issues/117185
-- `MACNumToString` ignores the two high bytes of its argument, `unhex`/`unbin` decode
-- case-insensitively and pad an incomplete leading group, and `UUIDStringToNum` parses
-- case-insensitively. None of them is injective, so the passes that eliminate an injective
-- function under `GROUP BY` or inside `uniq` must not fire for them. Every pair below prints the
-- result at default settings and then with the optimization disabled; the two must agree.

-- The test harness randomizes both settings, so pin them to the values under test.
SET optimize_injective_functions_in_group_by = 1, optimize_injective_functions_inside_uniq = 1;

SELECT 'MACNumToString';
SELECT MACNumToString(toUInt64(1)) = MACNumToString(toUInt64(1) + 281474976710656);
SELECT count() FROM (SELECT MACNumToString(x) AS k FROM (SELECT arrayJoin([toUInt64(1), toUInt64(1) + 281474976710656]) AS x) GROUP BY k);
SELECT count() FROM (SELECT MACNumToString(x) AS k FROM (SELECT arrayJoin([toUInt64(1), toUInt64(1) + 281474976710656]) AS x) GROUP BY k) SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT uniqExact(MACNumToString(x)) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(1) + 281474976710656]) AS x);
SELECT uniqExact(MACNumToString(x)) FROM (SELECT arrayJoin([toUInt64(1), toUInt64(1) + 281474976710656]) AS x) SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'unhex';
SELECT unhex('0a') = unhex('0A'), unhex('0a') = unhex('a');
SELECT count() FROM (SELECT unhex(s) AS k FROM (SELECT arrayJoin(['0a', '0A', 'a']) AS s) GROUP BY k);
SELECT count() FROM (SELECT unhex(s) AS k FROM (SELECT arrayJoin(['0a', '0A', 'a']) AS s) GROUP BY k) SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT uniqExact(unhex(s)) FROM (SELECT arrayJoin(['0a', '0A', 'a']) AS s);
SELECT uniqExact(unhex(s)) FROM (SELECT arrayJoin(['0a', '0A', 'a']) AS s) SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'unbin';
SELECT unbin('1010') = unbin('01010');
SELECT uniqExact(unbin(s)) FROM (SELECT arrayJoin(['1010', '01010']) AS s);
SELECT uniqExact(unbin(s)) FROM (SELECT arrayJoin(['1010', '01010']) AS s) SETTINGS optimize_injective_functions_inside_uniq = 0;

SELECT 'UUIDStringToNum';
SELECT UUIDStringToNum('61f0c404-5cb3-11e7-907b-a6006ad3dba0') = UUIDStringToNum('61F0C404-5CB3-11E7-907B-A6006AD3DBA0');
SELECT count() FROM (SELECT UUIDStringToNum(s) AS k FROM (SELECT arrayJoin(['61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61F0C404-5CB3-11E7-907B-A6006AD3DBA0']) AS s) GROUP BY k);
SELECT count() FROM (SELECT UUIDStringToNum(s) AS k FROM (SELECT arrayJoin(['61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61F0C404-5CB3-11E7-907B-A6006AD3DBA0']) AS s) GROUP BY k) SETTINGS optimize_injective_functions_in_group_by = 0;
SELECT uniqExact(UUIDStringToNum(s)) FROM (SELECT arrayJoin(['61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61F0C404-5CB3-11E7-907B-A6006AD3DBA0']) AS s);
SELECT uniqExact(UUIDStringToNum(s)) FROM (SELECT arrayJoin(['61f0c404-5cb3-11e7-907b-a6006ad3dba0', '61F0C404-5CB3-11E7-907B-A6006AD3DBA0']) AS s) SETTINGS optimize_injective_functions_inside_uniq = 0;

-- The same false claim also fed partition-wise independent aggregation.
SELECT 'independent aggregation of partitions';
DROP TABLE IF EXISTS t_injective_mac;
CREATE TABLE t_injective_mac (x UInt64) ENGINE = MergeTree ORDER BY x PARTITION BY x;
INSERT INTO t_injective_mac VALUES (1), (281474976710657);
SELECT count() FROM (SELECT MACNumToString(x) AS k FROM t_injective_mac GROUP BY k) SETTINGS force_aggregate_partitions_independently = 1, allow_aggregate_partitions_independently = 1;
SELECT count() FROM (SELECT MACNumToString(x) AS k FROM t_injective_mac GROUP BY k) SETTINGS allow_aggregate_partitions_independently = 0;
DROP TABLE t_injective_mac;

-- The same claim is consumed by `tryConvertAnyOuterJoinToInnerJoin`, which peels an injective
-- function off the join key of the aggregated side to decide that every row of the other side has
-- at most one match, and then rewrites `ANY OUTER` into `ALL INNER`. With a false claim two
-- aggregation groups collapse onto one join key, so the rewritten join duplicates rows instead of
-- keeping the single row `ANY` returns. Both prints must give one row.
SELECT 'ANY OUTER JOIN to INNER JOIN';
DROP TABLE IF EXISTS t_injective_join;
CREATE TABLE t_injective_join (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_injective_join VALUES (1), (281474976710657);

SELECT count()
FROM
(
    SELECT l.s
    FROM (SELECT MACNumToString(toUInt64(1)) AS s) AS l
    ANY LEFT JOIN (SELECT x, count() AS c FROM t_injective_join GROUP BY x) AS agg ON l.s = MACNumToString(agg.x)
    WHERE agg.c > 0
);

SELECT count()
FROM
(
    SELECT r.s
    FROM (SELECT x, count() AS c FROM t_injective_join GROUP BY x) AS agg
    ANY RIGHT JOIN (SELECT MACNumToString(toUInt64(1)) AS s) AS r ON MACNumToString(agg.x) = r.s
    WHERE agg.c > 0
);

DROP TABLE t_injective_join;
