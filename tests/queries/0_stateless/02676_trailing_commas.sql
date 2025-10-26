SELECT 1,;
SELECT 1, FROM numbers(1);
WITH 1 as a SELECT a, FROM numbers(1);
WITH 1 as from SELECT from, from + from, from in [0], FROM numbers(1);
SELECT n, FROM (SELECT 1 AS n);
SELECT (1, 'foo')::Tuple(a Int, b String,);
SELECT (1, 'foo')::Tuple(a Int, b String,,); -- { clientError SYNTAX_ERROR }
SELECT (1, 'foo')::Tuple(Int, String,);
SELECT (1, (2,'foo'))::Tuple(Int, Tuple(Int, String,),);
