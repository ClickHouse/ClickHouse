-- `EXPLAIN AST json = 1` serializes the query AST to JSON.
-- Currently only `ALTER NAMED COLLECTION` is fully supported: its AST is
-- self-contained, so serialization does not recurse into child AST nodes
-- (which do not implement JSON serialization yet).
-- The test checks that the produced output is well-formed JSON, without
-- pinning the exact formatting, which is expected to evolve.

SET max_threads = 1;

SELECT isValidJSON(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((num, line)))), '\n'))
FROM (SELECT rowNumberInAllBlocks() AS num, explain AS line FROM (EXPLAIN AST json = 1 ALTER NAMED COLLECTION foo SET a = 'b'));

SELECT isValidJSON(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((num, line)))), '\n'))
FROM (SELECT rowNumberInAllBlocks() AS num, explain AS line FROM (EXPLAIN AST json = 1 ALTER NAMED COLLECTION foo SET a = 'b', c = 'd' OVERRIDABLE, e = 'f' NOT OVERRIDABLE));

SELECT isValidJSON(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((num, line)))), '\n'))
FROM (SELECT rowNumberInAllBlocks() AS num, explain AS line FROM (EXPLAIN AST json = 1 ALTER NAMED COLLECTION foo DELETE a, b));

SELECT isValidJSON(arrayStringConcat(arrayMap(x -> x.2, arraySort(groupArray((num, line)))), '\n'))
FROM (SELECT rowNumberInAllBlocks() AS num, explain AS line FROM (EXPLAIN AST json = 1 ALTER NAMED COLLECTION IF EXISTS foo SET a = '1'));

-- Node types without JSON serialization report NOT_IMPLEMENTED.
EXPLAIN AST json = 1 SELECT 1; -- { serverError NOT_IMPLEMENTED }
