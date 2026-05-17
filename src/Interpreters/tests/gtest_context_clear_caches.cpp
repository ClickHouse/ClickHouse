#include <Interpreters/Context.h>
#include <Common/tests/gtest_global_context.h>
#include <gtest/gtest.h>

using namespace DB;

/// Regression test: `Context::clearCaches` used to throw a `LOGICAL_ERROR`
/// for every uninitialized cache, which made the function unusable from
/// `Context` instances that do not initialize the full set of caches
/// (`execute_query_fuzzer` libFuzzer harness, certain unit-test harnesses).
/// The fuzzer reached `clearCaches` via the `validateStorage` cleanup path
/// (`MergeTreeData::dropAllData` calls `Context::clearCaches`) and tripped
/// the assertion on the very first null cache, aborting the fuzzer.
///
/// The fix replaces every "`null` cache `→` throw" guard with a defensive
/// "`if (cache) cache->clear()`" check, matching the pattern already used
/// by every single-cache `clear<X>Cache` method on `Context`.
///
/// The unit-test harness `ContextHolder` (`gtest_global_context.cpp`) does not
/// call any `set*Cache` initializer, so the global test `Context` has all of
/// the caches null -- the exact precondition that the original code rejected.
TEST(Context, ClearCachesDoesNotThrowOnUninitializedCaches)
{
    auto context = Context::createCopy(getContext().context);
    context->makeQueryContext();

    /// Must not throw `Exception(LOGICAL_ERROR, "<X> cache was not created yet.")`.
    EXPECT_NO_THROW(context->clearCaches());

    /// Second call must also be a no-op (idempotent when there is nothing to clear).
    EXPECT_NO_THROW(context->clearCaches());
}
