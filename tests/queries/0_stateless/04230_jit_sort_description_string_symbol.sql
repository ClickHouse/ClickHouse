-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/103235:
-- on macOS, JIT-compiling a `String` sort comparator failed with
--   CANNOT_COMPILE_CODE: Could not find symbol _memcmpSmallCharsAllowOverflow15
-- because `llvm::Mangler::getNameWithPrefix` prepends `_` to C symbol names
-- under the Mach-O target, but `registerSymbol` stored unmangled names.
-- The fix mangles names at registration time so `findSymbol`'s lookup matches.
-- Linux is unaffected because the ELF mangler does not add a prefix.
--
-- The query uses a top-level ORDER BY on a `String` column so the optimizer
-- cannot drop the sort step (an inner ORDER BY under `count()` would be
-- eliminated, which silently bypasses the JIT path entirely).

SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

SELECT toString(number) AS s
FROM numbers(100)
ORDER BY s, number
FORMAT Null;

-- Assert JIT actually compiled the comparator. Without this, the test would
-- pass whenever JIT silently doesn't trigger (e.g., optimizer drops ORDER BY,
-- or a future setting change disables compilation), giving a false negative.
SYSTEM FLUSH LOGS;

SELECT sum(ProfileEvents['CompileFunction']) > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND type = 'QueryFinish';
