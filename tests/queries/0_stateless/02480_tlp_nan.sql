-- {echo}
SELECT sqrt(-1) as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=1;
SELECT sqrt(-1) as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=0;

SELECT -inf as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=1;
SELECT -inf as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=0;

SELECT NULL as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=1;
SELECT NULL as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=0;

SELECT inf as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=1;
SELECT inf as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=0;

SELECT nan as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=1;
SELECT nan as x, not(x), not(not(x)), (not(x)) IS NULL SETTINGS enable_analyzer=0;
