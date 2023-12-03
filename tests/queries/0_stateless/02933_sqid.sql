-- Tags: no-fasttest
SET allow_experimental_hash_functions = 1;
SET allow_suspicious_low_cardinality_types = 1;

select sqid(1, 2, 3, 4, 5);
select sqid(1, 2, 3, 4, materialize(5));
select number, sqid(number, number+1, number+2) from system.numbers limit 5;

CREATE TABLE t_sqid
(
    id UInt64,
    a LowCardinality(UInt8),
    b LowCardinality(UInt16),
    c LowCardinality(UInt32),
    d LowCardinality(UInt64),
    e Nullable(UInt8),
    f Nullable(UInt16),
    g Nullable(UInt32),
    h Nullable(UInt64)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_sqid select number, number+1, number+2, number+3, number+4, number+1, number+2, number+3, number+4 from system.numbers limit 5;

select sqid(id, a, b, c, d) from t_sqid;
select sqid(id, e, f, g, h) from t_sqid;

select sqid('1'); -- { serverError 43}
select sqid(); -- { serverError 42 }
