-- Tags: no-fasttest
-- no-fasttest: requires dictionaries

SET enable_analyzer = 1; -- DictGetTupleElementPass is an Analyzer pass; EXPLAIN QUERY TREE also requires the analyzer

DROP TABLE IF EXISTS test_keys;
DROP DICTIONARY IF EXISTS test_dict;
DROP TABLE IF EXISTS dict_source;

CREATE TABLE dict_source
(
    id UInt64,
    country String,
    city String,
    population UInt64
) ENGINE = Memory;

INSERT INTO dict_source VALUES (1, 'US', 'New York', 8336000), (2, 'FR', 'Paris', 2161000), (3, 'JP', 'Tokyo', 13960000);

CREATE DICTIONARY test_dict
(
    id UInt64,
    country String,
    city String,
    population UInt64
)
PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'dict_source' DB currentDatabase()))
LAYOUT(FLAT())
LIFETIME(0);

CREATE TABLE test_keys (id UInt64) ENGINE = Memory;
INSERT INTO test_keys VALUES (1), (2), (3);

-- With optimization: the query tree should contain dictGet but not tupleElement
SELECT 'optimization enabled';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 1
) WHERE explain LIKE '%function_name: dictGet,%';
SELECT count() FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 1
) WHERE explain LIKE '%function_name: tupleElement%';

-- Without optimization: tupleElement should be present wrapping dictGet
SELECT 'optimization disabled';
SELECT count() > 0 FROM (
    EXPLAIN QUERY TREE
    SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 1) FROM test_keys
    SETTINGS optimize_dictget_tuple_element = 0
) WHERE explain LIKE '%function_name: tupleElement%';

-- Functional tests: verify correctness
SELECT 'dictGet index access';
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city'), id).1 FROM test_keys ORDER BY id;
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city'), id).2 FROM test_keys ORDER BY id;

SELECT 'all three attributes by index';
SELECT
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).1,
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).2,
    dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).3
FROM test_keys ORDER BY id;

-- Test named access (e.g. .country instead of .1)
SELECT 'named access';
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).country FROM test_keys ORDER BY id;
SELECT dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id).city FROM test_keys ORDER BY id;

-- Test with dictGetOrDefault (existing keys)
SELECT 'dictGetOrDefault';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).1 FROM test_keys ORDER BY id;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), id, ('Unknown', 'Unknown')).2 FROM test_keys ORDER BY id;

-- Test with dictGetOrDefault (missing keys — exercises the default value rewrite path)
SELECT 'dictGetOrDefault with missing keys';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).2;

-- Test dictGetOrDefault with named access on missing keys (exercises the default-value rewrite path with string index)
SELECT 'dictGetOrDefault with missing keys named access';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).country;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), ('DefaultCountry', 'DefaultCity')).city;

-- Test dictGetOrDefault with tuple() function as default
SELECT 'dictGetOrDefault with tuple function default';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).2;

-- Same as above, but with named access — exercises the default-value rewrite path through the named-index code branch.
SELECT 'dictGetOrDefault with tuple function default named access';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).country;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('FuncCountry', 'FuncCity')).city;

-- Test dictGetOrDefault with non-rewritable default (non-constant expression) — the pass must bail out gracefully
SELECT 'dictGetOrDefault with non-rewritable default';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), materialize(('MCountry', 'MCity'))).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), materialize(('MCountry', 'MCity'))).2;

-- Test dictGetOrDefault with `tuple(...)` containing a non-constant element. The pass must bail out so
-- side effects in the dropped elements (e.g. `materialize`) are still evaluated as part of argument
-- evaluation, preserving original semantics.
SELECT 'dictGetOrDefault with tuple containing non-constant';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('OkCountry', materialize('MatCity'))).1;
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), tuple('OkCountry', materialize('MatCity'))).2;

-- Test dictGetOrDefault with CTE alias default (the alias references a constant tuple expression, not a ConstantNode)
SELECT 'dictGetOrDefault with CTE alias default';
WITH ('CTECountry', 'CTECity') AS d
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), d).1;
WITH ('CTECountry', 'CTECity') AS d
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), d).2;

-- Same as above with named access — ensures the named-index code path also bails out on alias defaults.
SELECT 'dictGetOrDefault with CTE alias default named access';
WITH ('CTECountry', 'CTECity') AS d
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), d).country;
WITH ('CTECountry', 'CTECity') AS d
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(999), d).city;

-- Test shared-parent scenario: ORDER BY ALL references the SELECT expression, so the tupleElement
-- (and its inner dictGet) node is shared between SELECT and ORDER BY. The pass must not mutate the
-- shared dictGet in place — that would leave the other parent's tupleElement wrapping a scalar.
SELECT 'shared tupleElement across SELECT and ORDER BY ALL';
SELECT DISTINCT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city', 'population'), id), 'city') FROM test_keys ORDER BY ALL;

-- Test `LowCardinality` key: the tuple-attribute form drops the `LowCardinality` wrapper from
-- the result, but the single-attribute form preserves it. Rewriting would change the result
-- type (`String` vs `LowCardinality(String)`) and break parent functions that expected the
-- original type. The pass must bail out in this case.
SELECT 'LowCardinality key bail out';
SELECT tupleElement(dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(toLowCardinality(1)), ('Default', 'Default')), 'city');
SELECT tupleElement(dictGet(currentDatabase() || '.test_dict', ('country', 'city'), toUInt64(toLowCardinality(2))), 'country');

-- `dictGetOrDefault` casts the entire default tuple to the attribute tuple type at execution.
-- When an un-selected default element is invalid for its attribute type (here `'not_a_number'`
-- for the `UInt64` `population` attribute), the original cast throws `CANNOT_PARSE_TEXT`. The
-- pass must bail out so this exception is preserved instead of being suppressed by extracting
-- only the selected element.
SELECT 'dictGetOrDefault default tuple element type mismatch bail out';
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'population'), toUInt64(999), ('Default', 'not_a_number')).country; -- { serverError CANNOT_PARSE_TEXT }
SELECT dictGetOrDefault(currentDatabase() || '.test_dict', ('country', 'population'), toUInt64(999), tuple('Default', 'not_a_number')).country; -- { serverError CANNOT_PARSE_TEXT }

DROP TABLE test_keys;
DROP DICTIONARY test_dict;
DROP TABLE dict_source;
