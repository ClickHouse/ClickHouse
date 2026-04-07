SELECT formatQuerySingleLine('SELECT position(1 = ANY(SELECT 1), 1)');
SELECT formatQuerySingleLine('SELECT position((1 IN (SELECT 1)), 1)');
SELECT formatQuerySingleLine('SELECT position(1, (1 IN (SELECT 1)))');
SELECT formatQuerySingleLine('SELECT position((1 NOT IN (SELECT 1)), 1)');
