-- PostgreSQL-style regular expression match operators: ~, ~*, !~, !~*

SELECT 'Hello' ~ 'ell', 'Hello' ~ '^ell', 'Hello' ~ 'ELL', 'Hello' ~* 'ELL';
SELECT 'Hello' !~ 'ell', 'Hello' !~ '^ell', 'Hello' !~* 'ELL', 'Hello' !~* 'xyz';

-- The operators are sugar for the match family of functions.
SELECT notMatch('Hello', 'e.*o'), matchCaseInsensitive('HELLO', 'he..o'), notMatchCaseInsensitive('HELLO', 'he..o');

-- Non-constant haystack and needle.
SELECT materialize('Hello') ~ materialize('l+'), materialize('Hello') !~ materialize('l+');

-- Interplay with other operators.
SELECT 'abc' ~ 'a' AND 'def' !~ 'x';
SELECT NOT ('abc' ~ 'z');
SELECT number FROM numbers(3) WHERE toString(number) !~ '[02]';

-- The query psql sends for the \d command uses !~ like this.
SELECT name !~ '^pg_toast' FROM (SELECT 'pg_toast_temp_1' AS name UNION ALL SELECT 'public' AS name) ORDER BY name;

SELECT formatQuerySingleLine('SELECT \'abc\' ~ \'a\', \'abc\' ~* \'A\', \'abc\' !~ \'z\', \'abc\' !~* \'Z\'');

-- A single exclamation mark is still an error.
SELECT 'abc' ! 'a'; -- { clientError SYNTAX_ERROR }

-- All PostgreSQL regexp operators support array quantifiers.
SELECT 'abc' ~* SOME(['A']);
SELECT 'abc' !~ SOME(['A', 'z']);
SELECT 'abc' !~* ALL(['Z', 'X']);
