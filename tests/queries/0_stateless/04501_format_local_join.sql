-- The `LOCAL` and `GLOBAL` keywords of a join must survive formatting. Dropping them changes
-- the query and fails the format-reparse consistency check of debug builds.
SELECT formatQuerySingleLine('SELECT count() FROM ta AS a LOCAL INNER JOIN tb AS b ON a.k = b.k');
SELECT formatQuerySingleLine('SELECT count() FROM ta AS a GLOBAL ANY LEFT JOIN tb AS b ON a.k = b.k');
