-- Tags: no-parallel
-- ^^ required because the test runs SYSTEM DROP COMPILED EXPRESSION CACHE, which affects global state.

-- The regexp JIT keeps a process-wide seen-count map (how many times each pattern has been used before
-- it is compiled). SYSTEM DROP COMPILED EXPRESSION CACHE must clear that map together with the compiled
-- cache and the JIT instances; matching must stay correct after the drop (the pattern is simply seen for
-- the first time again and recompiled).

SET compile_regular_expressions = 1;
SET min_count_to_compile_regular_expression = 0;

SELECT match('id42/x', '^id[0-9]+/.*$');
SELECT extractAll('a1b22c333', '[0-9]+');

SYSTEM DROP COMPILED EXPRESSION CACHE;

SELECT match('id42/x', '^id[0-9]+/.*$');
SELECT extractAll('a1b22c333', '[0-9]+');
