SET allow_experimental_dynamic_type=1;

DROP TABLE IF EXISTS test_max_types;
CREATE TABLE test_max_types (d Dynamic(max_types=5)) ENGINE = Memory;
INSERT INTO test_max_types VALUES ('string1'), (42), (3.14), ([1, 2]), (toDate('2021-01-01')), ('string2');
SELECT d, dynamicType(d) FROM test_max_types;

SELECT '';
DROP TABLE IF EXISTS test_nested_dynamic;
CREATE TABLE test_nested_dynamic (d1 Dynamic, d2 Dynamic(max_types=2)) ENGINE = Memory;
INSERT INTO test_nested_dynamic VALUES (NULL, 42), (42, 'string'), ('string', [1, 2]), ([1, 2], NULL);
SELECT d1, dynamicType(d1), d2, dynamicType(d2) FROM test_nested_dynamic;

DROP TABLE IF EXISTS test_rapid_schema;
CREATE TABLE test_rapid_schema (d Dynamic) ENGINE = Memory;
INSERT INTO test_rapid_schema VALUES (42), ('string1'), (toDate('2021-01-01')), ([1, 2, 3]), (3.14), ('string2'), (toDateTime('2021-01-01 12:00:00')), (['array', 'of', 'strings']), (NULL), (toFloat64(42.42));

SELECT d, dynamicType(d), d.Int64, d.String, d.Date, d.Float64, d.DateTime, d.`Array(Int64)`, d.`Array(String)`
FROM test_rapid_schema FORMAT PrettyCompactMonoBlock;


SELECT '';
SELECT finalizeAggregation(CAST(dynamic_state, 'AggregateFunction(sum, UInt64)'))
FROM
(
    SELECT CAST(state, 'Dynamic') AS dynamic_state
    FROM
    (
        SELECT sumState(number) AS state
        FROM numbers(10000)
    )
);

DROP TABLE test_max_types;
DROP TABLE test_nested_dynamic;
DROP TABLE test_rapid_schema;

