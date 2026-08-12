-- Tags: no-parallel, no-fasttest, no-msan
-- no-parallel: the test runs SYSTEM DROP COMPILED EXPRESSION CACHE, which affects global state.
-- no-fasttest: the Fast test build has no embedded compiler, so SYSTEM DROP COMPILED EXPRESSION CACHE
--              raises SUPPORT_IS_DISABLED there (and the regexp JIT path is a no-op).
-- no-msan: the MSan build also disables the embedded compiler (see contrib/llvm-project-cmake/CMakeLists.txt),
--          so SYSTEM DROP COMPILED EXPRESSION CACHE raises SUPPORT_IS_DISABLED there as well.

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
