SET allow_experimental_dynamic_type=1;
SET allow_suspicious_types_in_order_by=1;

CREATE TABLE t (d Dynamic) ENGINE = Memory;

INSERT INTO t VALUES ([(1, 'aa'), (2, 'bb')]::Nested(x UInt32, y Dynamic)) ;
INSERT INTO t VALUES ([(1, (2, ['aa', 'bb'])), (5, (6, ['ee', 'ff']))]::Nested(x UInt32, y Dynamic));

SELECT dynamicType(d),
       d,
       d.`Nested(x UInt32, y Dynamic)`.x,
       d.`Nested(x UInt32, y Dynamic)`.y,
       dynamicType(d.`Nested(x UInt32, y Dynamic)`.y[1]),
       d.`Nested(x UInt32, y Dynamic)`.y.`String`,
       d.`Nested(x UInt32, y Dynamic)`.y.`Tuple(Int64, Array(String))`
FROM t ORDER BY d
FORMAT PrettyCompactMonoBlock;
