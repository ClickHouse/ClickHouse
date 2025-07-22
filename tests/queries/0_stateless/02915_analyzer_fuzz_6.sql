set allow_suspicious_low_cardinality_types=1;
set enable_analyzer=1;

create table tab (x LowCardinality(Nullable(Float64))) engine = MergeTree order by x settings allow_nullable_key=1;
insert into tab select number from numbers(2);
SELECT [(arrayJoin([x]), x)] AS row FROM tab;


CREATE TABLE t__fuzz_307 (`k1` DateTime, `k2` LowCardinality(Nullable(Float64)), `v` Nullable(UInt32)) ENGINE =
 ReplacingMergeTree ORDER BY (k1, k2) settings allow_nullable_key=1;
 insert into t__fuzz_307 select * from generateRandom() limit 10;
 SELECT arrayJoin([tuple([(toNullable(NULL), -9223372036854775808, toNullable(3.4028234663852886e38), arrayJoin(
[tuple([(toNullable(NULL), 2147483647, toNullable(0.5), k2)])]), k2)])]) AS row, arrayJoin([(1024, k2)]), -9223372036854775807, 256, tupleElement(row, 1048576, 1024) AS k FROM t__fuzz_307 FINAL ORDER BY (toNullable('655.36'), 2, toNullable
('0.2147483648'), k2) ASC, toNullable('102.3') DESC NULLS FIRST, '10.25' DESC, k ASC NULLS FIRST format Null;

CREATE TABLE t__fuzz_282 (`k1` DateTime, `k2` LowCardinality(Nullable(Float64)), `v` Nullable(UInt32)) ENGINE = ReplacingMergeTree ORDER BY (k1, k2) SETTINGS allow_nullable_key = 1;
INSERT INTO t__fuzz_282 VALUES (1, 2, 3) (1, 2, 4) (2, 3, 4), (2, 3, 5);

SELECT arrayJoin([tuple([(toNullable(NULL), -9223372036854775808, toNullable(3.4028234663852886e38), arrayJoin([tuple([(toNullable(NULL), 2147483647, toNullable(0.5), k2)])]), k2)])]) AS row, arrayJoin([(1024, k2)]), -9223372036854775807, 256, tupleElement(row, 1048576, 1024) AS k FROM t__fuzz_282 FINAL ORDER BY (toNullable('655.36'), 2, toNullable('0.2147483648'), k2) ASC, toNullable('102.3') DESC NULLS FIRST, '10.25' DESC, k ASC NULLS FIRST format Null;
