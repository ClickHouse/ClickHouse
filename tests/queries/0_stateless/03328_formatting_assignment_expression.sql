SELECT formatQuerySingleLine('ALTER TABLE t (UPDATE c = (1 AS a) WHERE true)');
SELECT formatQuerySingleLine('ALTER TABLE t (UPDATE c = a > 1 WHERE true)');
SELECT formatQuerySingleLine('ALTER TABLE t (UPDATE c = a IS NULL WHERE true)');
