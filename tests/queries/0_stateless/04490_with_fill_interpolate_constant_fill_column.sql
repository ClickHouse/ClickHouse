-- A column participating in ORDER BY ... WITH FILL must not also be an INTERPOLATE output.
-- When the fill column is a constant, folding it and the INTERPOLATE target to the same source
-- column used to skip the validation, and the query aborted with a chunk row-count logical error
-- instead of a clean exception. Found by the AST fuzzer.

SET enable_analyzer = 1;
SELECT 1 AS a, 1 AS x ORDER BY a WITH FILL FROM 1 TO 5 INTERPOLATE (`x` AS x);
