DROP TABLE IF EXISTS t64m;

CREATE TABLE t64m
(
    u32 UInt32,
    i32 Int32,
    u64 UInt64,
    i64 Int64,
    t_u32 UInt32 Codec(T64),
    t_i32 Int32 Codec(T64),
    t_u64 UInt64 Codec(T64),
    t_i64 Int64 Codec(T64),
    tm_u64 UInt64 Codec(T64('byte_mask')),
    tm_i64 Int64 Codec(T64('byte_mask')),
    tm_u32 UInt32 Codec(T64('byte_mask')),
    tm_i32 Int32 Codec(T64('byte_mask'))
) ENGINE MergeTree() ORDER BY tuple();

SELECT * FROM t64m;
INSERT INTO t64m SELECT 42 AS x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1);
SELECT * FROM t64m;
TRUNCATE TABLE t64m;

--

INSERT INTO t64m SELECT 42 AS x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(10);
SELECT DISTINCT (u32, i32, u64, i64, t_u32, t_i32, t_u64, t_i64, tm_u32, tm_i32, tm_u64, tm_i64) FROM t64m;
TRUNCATE TABLE t64m;

--

INSERT INTO t64m SELECT number AS x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

INSERT INTO t64m SELECT number AS x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp2(10), 'UInt64') AS x INSERT INTO t64m SELECT x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp2(10), 'UInt64') AS x INSERT INTO t64m SELECT x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp10(5), 'UInt64') AS x INSERT INTO t64m SELECT x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp10(5), 'Int64') AS x INSERT INTO t64m SELECT x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

OPTIMIZE TABLE t64m FINAL;
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

--

INSERT INTO t64m SELECT number AS x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

INSERT INTO t64m SELECT number AS x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp2(10), 'UInt64') AS x INSERT INTO t64m SELECT x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp2(10), 'UInt64') AS x INSERT INTO t64m SELECT x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp10(5), 'UInt64') AS x INSERT INTO t64m SELECT x, x, x, x, x, x, x, x, x, x, x, x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

WITH CAST(number * exp10(5), 'Int64') AS x INSERT INTO t64m SELECT x, -x, x, -x, x, -x, x, -x, x, -x, x, -x FROM numbers(1000000);
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

OPTIMIZE TABLE t64m FINAL;
SELECT DISTINCT (t_u64 = tm_u64, t_i64 = tm_i64, u64 = tm_u64, i64 = tm_i64, u32 = tm_u32, i32 = tm_i32) FROM t64m;

DROP TABLE t64m;
