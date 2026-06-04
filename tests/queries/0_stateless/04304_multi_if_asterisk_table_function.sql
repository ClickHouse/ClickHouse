-- Tags: no-random-settings

-- The fix lives in the new analyzer's `resolveFunction`; the old analyzer takes a
-- different error path here, so pin the analyzer.
SET enable_analyzer = 1;

SELECT if(number % 2, materialize(toLowCardinality('a')), materialize(toLowCardinality('a'))) FROM numbers(multiIf(*, moduloOrZero(number, NULL), 'a'), 2); -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM numbers(multiIf(*, number, 1), 2); -- { serverError UNSUPPORTED_METHOD }
SELECT * FROM numbers(multiIf(*, 1, 2), 2); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

-- A multiIf whose condition is a real expression must keep working.
SELECT count() FROM numbers(multiIf(1, 5, 10), 1);
SELECT count() FROM numbers(multiIf(0, 5, 10), 1);
