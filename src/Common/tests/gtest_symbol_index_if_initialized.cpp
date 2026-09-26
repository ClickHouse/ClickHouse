#include <gtest/gtest.h>

#include <Common/StackTrace.h>
#include <Common/SymbolIndex.h>

#include <iostream>
#include <string>

#include <cstdlib>

/// `SymbolIndex::instanceIfInitialized` and `StackTrace::tryResolveAddress` exist for the fatal signal
/// handler: its first, bare dump of addresses must never build the symbol index, because building it
/// blocks on the function-local static's guard, which a thread that faulted inside the constructor will
/// never release.

namespace
{

const void * testAddress()
{
    return reinterpret_cast<const void *>(&::testing::Test::SetUpTestSuite);
}

/// A failed gtest assertion in the child would not reach the parent, which sees only the exit code.
void require(bool condition, const std::string & what)
{
    if (!condition)
    {
        std::cerr << "failed: " << what << '\n';
        std::_Exit(1);
    }
}

[[noreturn]] void checkNothingBuildsTheIndex()
{
    require(DB::SymbolIndex::instanceIfInitialized() == nullptr, "the index is not built at startup");
    /// The fallback for the bare dump must report that nothing can be resolved instead of building the
    /// index to find out. Where the index is not used for resolution at all, the answer is the same
    /// `Unsupported` one as from `resolveAddress`, and the point of the check is only that asking did
    /// not build anything.
#if defined(__ELF__) && !defined(OS_FREEBSD)
    require(!StackTrace::tryResolveAddress(testAddress()).has_value(), "nothing is resolved before the index is built");
#else
    const auto resolved = StackTrace::tryResolveAddress(testAddress());
    require(resolved.has_value() && resolved->kind == StackTrace::AddressKind::Unsupported,
        "resolution is unsupported on this platform");
#endif
    require(DB::SymbolIndex::instanceIfInitialized() == nullptr, "asking did not build the index as a side effect");
    std::_Exit(0);
}

}

/// Runs in a fresh process: whether the process-global index has been built depends on what ran before
/// in this binary, and the state this PR is about - not built yet - is only reachable before any other
/// test touches it. The `threadsafe` style re-executes the test binary, so the child runs this test
/// alone and the check below is deterministic rather than dependent on the test order.
TEST(SymbolIndexDeathTest, NothingBuildsTheIndexBeforeTheFirstInstanceCall)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    EXPECT_EXIT(checkNothingBuildsTheIndex(), ::testing::ExitedWithCode(0), ".*");
}

/// Once built, the instance is published, so the handler prints the same addresses as the symbolized
/// trace below it.
TEST(SymbolIndex, InstanceIsPublishedOnceBuilt)
{
    const DB::SymbolIndex & index = DB::SymbolIndex::instance();
    EXPECT_EQ(DB::SymbolIndex::instanceIfInitialized(), &index);

    const auto resolved = StackTrace::tryResolveAddress(testAddress());
    ASSERT_TRUE(resolved.has_value());
    EXPECT_EQ(resolved->address, StackTrace::resolveAddress(testAddress()).address);
}
