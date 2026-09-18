-- Fixture for json_ast_sql_execution_fuzzer: executed once by the in-process `clickhouse local` before the
-- first input. The table and column names match the vocabulary in json_ast.proto (`KnownString`) and the
-- seed queries of json_ast_sql_parser_fuzzer, so that mutated queries reference real tables and columns and
-- reach execution instead of failing on name resolution. Keep the data small: every input runs one query.

CREATE TABLE t
(
    a UInt64,
    b Int32,
    c String,
    d Date,
    e Float64,
    f Nullable(String),
    s String,
    x UInt8,
    y UInt16,
    z Int64,
    id UInt64,
    key String,
    value Float64,
    n Nullable(UInt32),
    arr Array(UInt32),
    m Map(String, UInt64),
    tup Tuple(a UInt8, b String),
    dt DateTime,
    dt64 DateTime64(3),
    lc LowCardinality(String),
    u UUID,
    dec Decimal(18, 4),
    ip IPv4,
    en Enum8('a' = 1, 'b' = 2, 'c' = 3),
    fs FixedString(4),
    bl Bool,
    d32 Date32,
    nested Nested(k UInt32, v String),
    j JSON,
    dyn Dynamic,
    var Variant(UInt64, String, Array(UInt8)),
    i128 Int128,
    u256 UInt256,
    fl32 Float32,
    ipv6 IPv6,
    dec2 Decimal(38, 10),
    t_arr Array(Tuple(UInt8, String)),
    arr_null Array(Nullable(Int32)),
    m2 Map(UInt64, Array(String)),
    lc_null LowCardinality(Nullable(String)),
    INDEX idx_c c TYPE bloom_filter GRANULARITY 2,
    INDEX idx_e e TYPE minmax GRANULARITY 1,
    PROJECTION p_a (SELECT a, count() GROUP BY a)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY (a, id)
SETTINGS index_granularity = 64;

INSERT INTO t SELECT
    number,
    toInt32(number % 7) - 3,
    toString(number % 13),
    toDate('2024-01-01') + (number % 100),
    number / 3,
    if(number % 5 = 0, NULL, toString(number)),
    concat('s', toString(number % 17)),
    number % 256,
    number % 65536,
    toInt64(number) - 500,
    number,
    concat('k', toString(number % 11)),
    number * 1.5,
    if(number % 3 = 0, NULL, toUInt32(number)),
    arrayMap(i -> toUInt32(i * number), range(number % 5)),
    map('a', number, 'b', number * 2),
    (toUInt8(number % 256), toString(number)),
    toDateTime('2024-01-01 00:00:00', 'UTC') + number * 60,
    toDateTime64('2024-01-01 00:00:00', 3, 'UTC') + number,
    toString(number % 4),
    generateUUIDv4(),
    toDecimal64(number / 7, 4),
    toIPv4(number * 65537),
    CAST(number % 3 + 1, 'Enum8(\'a\' = 1, \'b\' = 2, \'c\' = 3)'),
    toFixedString(leftPad(toString(number % 1000), 4, '0'), 4),
    number % 2 = 0,
    toDate32('2024-01-01') + number,
    range(number % 4),
    arrayMap(i -> toString(i), range(number % 4)),
    CAST(concat('{"a":', toString(number), ',"b":"', toString(number % 3), '","c":[', toString(number), ']}'), 'JSON'),
    multiIf(number % 3 = 0, number::Dynamic, number % 3 = 1, toString(number)::Dynamic, [1, 2]::Dynamic),
    multiIf(number % 3 = 0, number::Variant(UInt64, String, Array(UInt8)), number % 3 = 1, toString(number)::Variant(UInt64, String, Array(UInt8)), [toUInt8(number % 256)]::Variant(UInt64, String, Array(UInt8))),
    toInt128(number) * 1000000000000,
    toUInt256(number) * 1000000000000000000,
    toFloat32(number) / 7,
    toIPv6(concat('2001:db8::', hex(number))),
    toDecimal128(number, 10) / 3,
    arrayMap(i -> (toUInt8(i), toString(i)), range(number % 3)),
    arrayMap(i -> if(i % 2 = 0, NULL, toInt32(i)), range(number % 4)),
    map(number, arrayMap(i -> toString(i), range(number % 3))),
    if(number % 4 = 0, NULL, toString(number % 5))
FROM numbers(1000);

CREATE TABLE t1 (k UInt64, ts DateTime, a UInt64, b String, v Float64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number * 3600, number % 10, toString(number % 7), number / 2 FROM numbers(200);

CREATE TABLE t2 (k UInt64, ts DateTime, a UInt64, b String, v Float64) ENGINE = MergeTree ORDER BY (k, ts);
INSERT INTO t2 SELECT number * 2, toDateTime('2024-01-01 00:00:00', 'UTC') + number * 1800, number % 5, toString(number % 3), number FROM numbers(150);

CREATE TABLE t3 (id UInt64, name String, value Nullable(Float64), tags Array(String)) ENGINE = Memory;
INSERT INTO t3 SELECT number, concat('name', toString(number)), if(number % 4 = 0, NULL, number * 0.5), arrayMap(i -> concat('tag', toString(i)), range(number % 3)) FROM numbers(50);

CREATE TABLE src (a UInt64, b String, c Float64) ENGINE = Memory;
INSERT INTO src SELECT number, toString(number % 5), number / 10 FROM numbers(100);

CREATE TABLE dst (a UInt64, b String, c Float64) ENGINE = MergeTree ORDER BY a;

CREATE TABLE tbl (t Tuple(a UInt8, b String), name String, arr Array(UInt64)) ENGINE = Memory;
INSERT INTO tbl SELECT (toUInt8(number), toString(number)), concat('n', toString(number)), range(number % 6) FROM numbers(30);

CREATE TABLE empsalary (depname String, empno UInt64, salary Int64, enroll_date Date) ENGINE = MergeTree ORDER BY (depname, empno);
INSERT INTO empsalary SELECT ['develop', 'sales', 'personnel'][number % 3 + 1], number, 3000 + number * 100, toDate('2006-01-01') + number * 30 FROM numbers(30);

CREATE TABLE lg (a UInt64, s String) ENGINE = Log;
INSERT INTO lg SELECT number, toString(number) FROM numbers(20);

CREATE TABLE nul (a UInt64, s String) ENGINE = Null;

CREATE TABLE jt (k UInt64, jv String) ENGINE = Join(ANY, LEFT, k);
INSERT INTO jt SELECT number * 3, concat('j', toString(number)) FROM numbers(40);

CREATE TABLE st (k UInt64) ENGINE = Set;
INSERT INTO st SELECT number * 5 FROM numbers(40);

CREATE VIEW v AS SELECT a, count() AS c, sum(e) AS total FROM t GROUP BY a;

CREATE MATERIALIZED VIEW mv TO dst AS SELECT a, b, c FROM src;

CREATE DICTIONARY d (k UInt64, dv String DEFAULT 'none') PRIMARY KEY k
SOURCE(CLICKHOUSE(QUERY 'SELECT a AS k, b AS dv FROM src')) LAYOUT(FLAT()) LIFETIME(0);

-- Warm up the objects that materialize lazily, so that the first fuzzed query does not pay for it.
SELECT count() FROM t WHERE a IN (SELECT a FROM src) FORMAT Null;
SELECT dictGet('d', 'dv', toUInt64(1)) FORMAT Null;
SELECT * FROM v ORDER BY a LIMIT 1 FORMAT Null;

-- The fuzzed statements run in this session: forbid DDL and data modification so that a mutated query
-- cannot destroy the fixture. `readonly = 2` still allows per-query `SETTINGS`.
SET readonly = 2;
