SET allow_experimental_full_text_index = 1;
SET explain_query_plan_default = 'legacy';

DROP TABLE IF EXISTS t_json_all_values_coercion;

CREATE TABLE t_json_all_values_coercion
(
    data JSON(ip IPv4, x UInt16),
    INDEX idx_values JSONAllValues(data) TYPE text(tokenizer = splitByNonAlpha) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 4;

INSERT INTO t_json_all_values_coercion
SELECT '{"ip":"1.2.3.4","x":256,"tag":"needle"}' FROM numbers(4);
INSERT INTO t_json_all_values_coercion
SELECT '{"ip":"8.8.8.8","x":1,"tag":"other"}' FROM numbers(4);

-- Convert the constant to the typed path before serializing it for index lookup.
SELECT count() FROM t_json_all_values_coercion WHERE data.ip = toUInt32(16909060);

-- Value-changing casts cannot use the representation stored by `JSONAllValues`.
SELECT count() FROM t_json_all_values_coercion WHERE data.x::UInt8 = 0;

-- Matching types and casts to String can still use the index.
SELECT count() FROM t_json_all_values_coercion WHERE data.ip = toIPv4('1.2.3.4');
SELECT count() FROM t_json_all_values_coercion WHERE data.tag::String = 'needle';

SELECT count() FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM t_json_all_values_coercion WHERE data.ip = toIPv4('1.2.3.4')
)
WHERE explain LIKE '%Granules: 1/2%';

DROP TABLE t_json_all_values_coercion;
