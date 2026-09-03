SELECT * FROM globalIn('a', 1); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM plus(1, 2); -- { serverError UNKNOWN_FUNCTION }
SELECT * FROM negate(x); -- { serverError UNKNOWN_FUNCTION }

SELECT not((SELECT * AND(16)) AND 1);

SELECT -[(1)]; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT NOT ((1, 1, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ProfileEvents['LoadedMarksCount'], 1 OR toLowCardinality(1) FROM system.nonexistent PREWHERE tupleElement(*, 1) AND match(query, 'SELECT * FROM t_prewarm_add_column%') AND (currentDatabase() = current_database) WHERE ('SELECT * FROM t_prewarm_add_column%' NOT LIKE query) AND (type = 'QueryFinish') AND (current_database = currentDatabase()) ORDER BY ALL DESC NULLS FIRST; -- { serverError UNKNOWN_TABLE }

select (((1), (2)));

SELECT (1 AS c0).1, (1 AS c0).1;  -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT (1 AS c0,).1, (1 AS c0,).1;
