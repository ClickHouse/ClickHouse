SELECT CAST(tuple(), 'Tuple()') IN (tuple());

SELECT CAST(tuple(), 'Tuple()') IN [tuple()];

SELECT CAST(tuple(), 'Tuple()') IN [tuple(), tuple()];

SELECT CAST(tuple(), 'Tuple()') NOT IN (tuple());

SELECT CAST(tuple(), 'Tuple()') IN (tuple(1));-- { serverError TYPE_MISMATCH }

SELECT CAST(tuple(), 'Tuple()') IN [()];

SELECT CAST(tuple(), 'Tuple()') IN (());

SELECT tuple() IN (tuple());

SELECT [tuple()] IN [[tuple()], [tuple()]];

SELECT [tuple()] IN [()];

SELECT tuple() IN (((tuple())));

SELECT tuple() IN [(((tuple())))];

DROP TABLE IF EXISTS test_empty_tuple;
CREATE TABLE test_empty_tuple (t Tuple()) ENGINE = Memory;
INSERT INTO test_empty_tuple VALUES (tuple()), (tuple()), (tuple());

SELECT t FROM test_empty_tuple WHERE t IN (tuple());

SELECT t FROM test_empty_tuple WHERE t IN [tuple()];

SELECT t FROM test_empty_tuple WHERE t IN [()];

SELECT t FROM test_empty_tuple WHERE t IN [tuple(), tuple()];

SELECT t FROM test_empty_tuple WHERE t IN tuple();

SELECT t FROM test_empty_tuple WHERE [t] IN [tuple()];

SELECT count() FROM test_empty_tuple WHERE t IN [tuple()];

SELECT arrayJoin([tuple(), tuple()]) IN (tuple());


SET enable_analyzer = 0;

SELECT CAST(tuple(), 'Tuple()') IN (tuple());

SELECT CAST(tuple(), 'Tuple()') IN [tuple()];

SELECT CAST(tuple(), 'Tuple()') IN [tuple(), tuple()];

SELECT CAST(tuple(), 'Tuple()') NOT IN (tuple());

SELECT CAST(tuple(), 'Tuple()') IN (tuple(1));-- { serverError TYPE_MISMATCH }

SELECT CAST(tuple(), 'Tuple()') IN [()];

SELECT CAST(tuple(), 'Tuple()') IN (());

SELECT tuple() IN (tuple());

SELECT [tuple()] IN [[tuple()], [tuple()]];

SELECT [tuple()] IN [()];

SELECT tuple() IN (((tuple())));

SELECT tuple() IN [(((tuple())))];

DROP TABLE IF EXISTS test_empty_tuple;
CREATE TABLE test_empty_tuple (t Tuple()) ENGINE = Memory;
INSERT INTO test_empty_tuple VALUES (tuple()), (tuple()), (tuple());

SELECT t FROM test_empty_tuple WHERE t IN (tuple());

SELECT t FROM test_empty_tuple WHERE t IN [tuple()];

SELECT t FROM test_empty_tuple WHERE t IN [()];

SELECT t FROM test_empty_tuple WHERE t IN [tuple(), tuple()];

SELECT t FROM test_empty_tuple WHERE t IN tuple();

SELECT t FROM test_empty_tuple WHERE [t] IN [tuple()];

SELECT count() FROM test_empty_tuple WHERE t IN [tuple()];

SELECT arrayJoin([tuple(), tuple()]) IN (tuple());
