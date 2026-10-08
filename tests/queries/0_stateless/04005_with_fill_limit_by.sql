-- ORDER BY ... WITH FILL combined with LIMIT BY used to throw
-- "Filling column ... is not present in the block" because the ActionsChain
-- optimization removed ORDER BY columns from the LIMIT BY expression step.
SELECT 1 ORDER BY toDateTime(0) WITH FILL LIMIT 1 BY 1;
SELECT 1 ORDER BY rank() OVER () WITH FILL LIMIT 1 BY 1;
SELECT number FROM numbers(5) ORDER BY number WITH FILL FROM 0 TO 7 LIMIT 10 BY number;
