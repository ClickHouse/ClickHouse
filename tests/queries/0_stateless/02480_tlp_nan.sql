-- {echo}
SELECT sqrt(-1) as x, not(x), not(not(x)), (not(x)) IS NULL;

SELECT -inf as x, not(x), not(not(x)), (not(x)) IS NULL;

SELECT NULL as x, not(x), not(not(x)), (not(x)) IS NULL;

SELECT inf as x, not(x), not(not(x)), (not(x)) IS NULL;

SELECT nan as x, not(x), not(not(x)), (not(x)) IS NULL;
