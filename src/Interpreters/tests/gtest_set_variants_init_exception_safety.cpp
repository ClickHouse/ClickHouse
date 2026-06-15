#include <gtest/gtest.h>

#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <Interpreters/SetVariants.h>

using namespace DB;

/// init() must be exception-safe: if the variant allocation throws (e.g. under a memory
/// limit), the object must stay EMPTY rather than ending up with `type != EMPTY` and a null
/// variant pointer. A half-initialized object crashes the next reader, e.g.
/// getTotalRowCount() -> (NAME)->data.size() dereferences null.
TEST(SetVariants, InitIsExceptionSafeUnderMemoryLimit)
{
    MainThreadStatus::getInstance();

    auto & thread_tracker = CurrentThread::get().memory_tracker;
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();

    /// Force every allocation to reach the tracker (default 4 MiB per-thread buffer would
    /// otherwise hide the small variant allocation).
    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    CurrentThread::get().untracked_memory_limit = 0;

    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    SCOPE_EXIT_SAFE({
        total_memory_tracker.setHardLimit(0);
        thread_tracker.setHardLimit(0);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
    });

    SetVariants variants;

    bool threw = false;
    try
    {
        variants.init(SetVariants::Type::key64);
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }

    /// Lift the limit before touching the object so the assertions themselves can allocate.
    total_memory_tracker.setHardLimit(0);
    thread_tracker.setHardLimit(0);
    CurrentThread::get().untracked_memory_limit = saved_untracked_limit;

    ASSERT_TRUE(threw) << "init() was expected to throw under the memory limit";

    /// The allocation failed, so the object must be back to a consistent EMPTY state.
    /// Without the fix, type == key64 while the key64 pointer is null, so empty() is false
    /// and getTotalRowCount() dereferences a null pointer.
    EXPECT_TRUE(variants.empty());
    EXPECT_EQ(variants.getTotalRowCount(), 0u);
}
