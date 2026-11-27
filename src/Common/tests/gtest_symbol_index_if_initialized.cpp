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
    /// Without the index, an address in the main executable must still resolve, to the same offset the
    /// index gives once it is built, so the bare dump never prints a runtime address.
    const auto before = StackTrace::tryResolveAddress(testAddress());
    require(DB::SymbolIndex::instanceIfInitialized() == nullptr, "asking did not build the index as a side effect");

    const auto after = StackTrace::resolveAddress(testAddress());
    require(before.kind == after.kind && before.address == after.address, "the fallback resolves like the index");
#if defined(__ELF__) && !defined(OS_FREEBSD)
    require(before.kind == StackTrace::AddressKind::MainObject, "the address is in the main executable");
#endif
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

    EXPECT_EQ(StackTrace::tryResolveAddress(testAddress()).address, StackTrace::resolveAddress(testAddress()).address);
}
