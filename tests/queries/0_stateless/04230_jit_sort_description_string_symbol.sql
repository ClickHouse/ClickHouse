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

-- The compiled-expression cache is server-wide. If an earlier test with the
-- same sort-description hash already populated it, our SELECT below would hit
-- the cache, `compileSortDescription` would not run, and
-- `ProfileEvents['CompileFunction']` would stay at zero - turning the
-- assertion into a false negative. Drop the cache so compilation is forced.
SYSTEM DROP COMPILED EXPRESSION CACHE;

SET compile_sort_description = 1;
SET min_count_to_compile_sort_description = 0;

SELECT toString(number) AS s
FROM numbers(100)
ORDER BY s, number
SETTINGS log_comment = '04230_jit_sort_description_check'
FORMAT Null;

-- Assert JIT actually compiled the comparator. Without this, the test would
-- pass whenever JIT silently doesn't trigger (e.g., optimizer drops ORDER BY,
-- or a future setting change disables compilation), giving a false negative.
-- Filtering by the unique `log_comment` above isolates the assertion to
-- exactly the JIT query; `current_database = currentDatabase()` is required
-- by the repository style check for any read of `system.query_log`.
SYSTEM FLUSH LOGS query_log;

SELECT ProfileEvents['CompileFunction'] > 0
FROM system.query_log
WHERE current_database = currentDatabase()
  AND log_comment = '04230_jit_sort_description_check'
  AND type = 'QueryFinish'
ORDER BY event_time_microseconds DESC
LIMIT 1;
