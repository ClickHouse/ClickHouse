SELECT count() FROM (SELECT bayesAB('beta', 1, ['Control', 'A', 'B'], [3000.0, 3000.0, 2000.0], [1000.0, 1100.0, 800.0]));
SELECT count() FROM (SELECT bayesAB('gamma', 1, ['Control', 'A', 'B'], [3000.0, 3000.0, 2000.0], [1000.0, 1100.0, 800.0]));
SELECT count() FROM (SELECT bayesAB('beta', 0, ['Control', 'A', 'B'], [3000.0, 3000.0, 2000.0], [1000.0, 1100.0, 800.0]));
SELECT count() FROM (SELECT bayesAB('gamma', 0, ['Control', 'A', 'B'], [3000.0, 3000.0, 2000.0], [1000.0, 1100.0, 800.0]));
