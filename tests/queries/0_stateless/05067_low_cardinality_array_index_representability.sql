-- `has`/`indexOf`/`countEqual` over an `Array(LowCardinality(<numeric>))` looked the constant needle up in
-- the dictionary after a plain wrapping cast, so a needle that the element type cannot represent matched
-- the element its cast image happens to equal.

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_lc_array_index;
CREATE TABLE t_lc_array_index
(
    id UInt8,
    a8 Array(LowCardinality(UInt8)),
    a64 Array(LowCardinality(UInt64)),
    af32 Array(LowCardinality(Float32))
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO t_lc_array_index VALUES (1, [255], [18446744073709551615], [16777216]), (2, [5], [5], [5]);

SELECT groupArray(id) FROM t_lc_array_index WHERE has(a8, -1);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(a64, -1);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(af32, 16777217);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(a8, 256);
SELECT indexOf(a8, -1), countEqual(a8, -1) FROM t_lc_array_index WHERE id = 1;
-- `optimize_rewrite_array_exists_to_has` sends this through the same path.
SELECT groupArray(id) FROM t_lc_array_index WHERE arrayExists(x -> x = -1, a8);

-- Representable needles keep matching.
SELECT groupArray(id) FROM t_lc_array_index WHERE has(a8, 5);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(a8, 255);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(a64, 18446744073709551615);
SELECT groupArray(id) FROM t_lc_array_index WHERE has(af32, 16777216);
SELECT indexOf(a8, 255), countEqual(a8, 255) FROM t_lc_array_index WHERE id = 1;

DROP TABLE t_lc_array_index;
