-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103235:
-- on macOS, the JIT symbol resolver received mangled names (with a leading
-- '_') but the symbol table was populated with unmangled names, so any
-- ORDER BY that JIT-compiles a String comparator failed with:
--   CANNOT_COMPILE_CODE: Could not find symbol _memcmpSmallCharsAllowOverflow15
-- The fix retries the lookup after stripping a leading '_'. Linux is
-- unaffected because LLVM does not add that prefix there.

SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

SELECT count() FROM (SELECT number, toString(number) AS s FROM numbers(100) ORDER BY s, number);
