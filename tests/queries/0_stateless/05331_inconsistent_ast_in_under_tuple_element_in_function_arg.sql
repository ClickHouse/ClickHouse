-- An IN operand of a tuple element inside a multi-argument function call must format idempotently.

SELECT f((40 IN (1)).1[4], 38); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT notHas(1 != SOME((SELECT 1)).2[3][2], 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 1 FROM (VALUES ((40 IN (1)).1[4], 38), (1, 1)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT q, formatQuerySingleLine(q) = formatQuerySingleLine(formatQuerySingleLine(q))
FROM VALUES('q String',
    'SELECT f((40 IN (1)).1[4], 38)',
    'SELECT f(38, (40 IN (1)).1[4])',
    'SELECT notHas((1 IN (1)).2[3][2], \'x\')',
    'SELECT notHas(1 != SOME((SELECT 1)).2[3][2], \'x\')',
    'SELECT 1 FROM (VALUES ((40 IN (1)).1[4], 38), (1, 1))',
    'SELECT f(((40 IN (1)).1).3[4], 38)',
    'SELECT f((40 IN (1)).1 + 1, 2)',
    'SELECT f(y[(40 IN (1)).1][2], 3)',
    'SELECT f((40 NOT IN (1)).1[4], 38)',
    'SELECT f((40 GLOBAL IN (1)).1[4], 38)',
    'SELECT f((40 IN (1) AS a).1[4], 38)');

SELECT formatQuerySingleLine('SELECT f((40 IN (1)).1[4], 38)');
SELECT formatQuerySingleLine('SELECT f((40 IN (1)).1, 38)');
