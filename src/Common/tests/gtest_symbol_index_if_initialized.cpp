#include <gtest/gtest.h>

#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>

/// `SymbolIndex::instanceIfInitialized` and `StackTrace::tryResolveAddress` exist for the fatal signal
/// handler: its first, bare dump of addresses must never build the symbol index, because building it
/// blocks on the function-local static's guard, which a thread that faulted inside the constructor will
/// never release.
///
/// Whether the index has already been built depends on what ran before in this binary, so the test
/// checks the invariants that hold either way.
TEST(SymbolIndex, InstanceIfInitializedNeverBuildsTheIndex)
{
    const void * const address = reinterpret_cast<const void *>(&::testing::Test::SetUpTestSuite);

    if (!DB::SymbolIndex::instanceIfInitialized())
    {
        /// Asking again must not have built it as a side effect, and the fallback for the bare dump
        /// must report that nothing can be resolved instead of building the index to find out.
        EXPECT_EQ(DB::SymbolIndex::instanceIfInitialized(), nullptr);
        EXPECT_FALSE(StackTrace::tryResolveAddress(address).has_value());
        EXPECT_EQ(DB::SymbolIndex::instanceIfInitialized(), nullptr);
    }

    /// Once built, the instance is published, so the handler prints the same addresses as the
    /// symbolized trace below it.
    const DB::SymbolIndex & index = DB::SymbolIndex::instance();
    EXPECT_EQ(DB::SymbolIndex::instanceIfInitialized(), &index);

    const auto resolved = StackTrace::tryResolveAddress(address);
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->address, StackTrace::resolveAddress(address).address);
}
