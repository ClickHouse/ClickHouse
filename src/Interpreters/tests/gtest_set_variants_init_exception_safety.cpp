#include <gtest/gtest.h>

#include <Common/CurrentMemoryTracker.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/scope_guard_safe.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Set.h>
#include <Interpreters/SetKeys.h>
#include <Interpreters/SetVariants.h>
#include <QueryPipeline/SizeLimits.h>

#include <limits>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

/// init() must be exception-safe: if the variant allocation throws (e.g. under a memory
/// limit), the object must stay EMPTY rather than ending up with `type != EMPTY` and a null
/// variant pointer. A half-initialized object crashes the next reader, e.g.
/// getTotalRowCount() -> (NAME)->data.size() dereferences null.
TEST(SetVariants, InitIsExceptionSafeUnderMemoryLimit)
{
    MainThreadStatus::getInstance();

    auto & thread_tracker = CurrentThread::get().memory_tracker;

    /// Construct the object before arming the limit so only init()'s allocation can throw.
    SetVariants variants;

    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_thread_hard_limit = thread_tracker.getHardLimit();
    const Int64 saved_total_hard_limit = total_memory_tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();

    SCOPE_EXIT_SAFE({
        total_memory_tracker.setHardLimit(saved_total_hard_limit);
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    });

    /// The throwing operator new routes through CurrentMemoryTracker::allocThrow, which only
    /// enforces the hard limit for allocations of at least this many bytes. The default is
    /// disabled (UINT64_MAX), so without lowering it the 1-byte limit below is ignored and
    /// init()'s make_unique succeeds, making this test pass even without the fix.
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);

    /// Make sure the small variant allocation reaches the tracker: drop the per-thread
    /// untracked-memory buffer (default 4 MiB) and flush whatever is already buffered before
    /// resetting counters, otherwise the buffered bytes hide the allocation.
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();

    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    bool threw = false;
    try
    {
        variants.init(SetVariants::Type::key64);
    }
    catch (const DB::Exception &)
    {
        threw = true;
    }

    /// Lift the limits before touching the object so the assertions themselves can allocate.
    total_memory_tracker.setHardLimit(saved_total_hard_limit);
    thread_tracker.setHardLimit(saved_thread_hard_limit);
    CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);

    ASSERT_TRUE(threw) << "init() was expected to throw under the memory limit";

    /// The allocation failed, so the object must be back to a consistent EMPTY state.
    /// Without the fix, type == key64 while the key64 pointer is null, so empty() is false
    /// and getTotalRowCount() dereferences a null pointer.
    EXPECT_TRUE(variants.empty());
    EXPECT_EQ(variants.getTotalRowCount(), 0u);
}

TEST(Set, NullMarkerFollowsSuccessfulKeyInsertion)
{
    MainThreadStatus::getInstance();

    auto type = DataTypeFactory::instance().get("LowCardinality(Nullable(String))");
    Set set(SizeLimits{}, /*max_elements_to_fill_=*/ 0, /*transform_null_in_=*/ true);
    set.setHeader({ColumnWithTypeAndName(type->createColumn(), type, "x")});

    auto present = type->createColumn();
    present->insert(Field(String{"a"}));
    set.insertFromColumns({std::move(present)});

    auto incoming = type->createColumn();
    incoming->insert(Field{});
    Columns incoming_columns{std::move(incoming)};
    SetKeyColumns holder;
    holder.key_columns.reserve(1);
    holder.materialized_columns.reserve(1);

    auto & thread_tracker = CurrentThread::get().memory_tracker;
    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_thread_hard_limit = thread_tracker.getHardLimit();
    const Int64 saved_total_hard_limit = total_memory_tracker.getHardLimit();
    const UInt64 saved_min_alloc_to_throw = CurrentMemoryTracker::getMinAllocationSizeBytesToThrow();

    SCOPE_EXIT_SAFE({
        total_memory_tracker.setHardLimit(saved_total_hard_limit);
        thread_tracker.setHardLimit(saved_thread_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
        CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);
    });

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    bool memory_limit_exceeded = false;
    try
    {
        set.insertFromColumns(incoming_columns, holder);
    }
    catch (const DB::Exception & e)
    {
        memory_limit_exceeded = e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED;
    }

    total_memory_tracker.setHardLimit(saved_total_hard_limit);
    thread_tracker.setHardLimit(saved_thread_hard_limit);
    CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);

    ASSERT_TRUE(memory_limit_exceeded);
    EXPECT_FALSE(set.hasNull());
    set.finishInsert();

    auto missing = type->createColumn();
    missing->insert(Field(String{"missing"}));
    auto membership = set.execute({ColumnWithTypeAndName(std::move(missing), type, "x")}, false);
    ASSERT_EQ(membership->size(), 1);
    EXPECT_FALSE(membership->getBool(0));

    Set empty_set(SizeLimits{}, /*max_elements_to_fill_=*/ 0, /*transform_null_in_=*/ true);
    empty_set.setHeader({ColumnWithTypeAndName(type->createColumn(), type, "x")});
    SetKeyColumns empty_holder;
    empty_holder.key_columns.reserve(1);
    empty_holder.materialized_columns.reserve(1);

    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(1);
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();
    total_memory_tracker.resetCounters();
    thread_tracker.resetCounters();
    total_memory_tracker.setHardLimit(1);
    thread_tracker.setHardLimit(1);

    bool empty_insert_failed = false;
    try
    {
        empty_set.insertFromColumns(incoming_columns, empty_holder);
    }
    catch (const DB::Exception & e)
    {
        empty_insert_failed = e.code() == ErrorCodes::MEMORY_LIMIT_EXCEEDED;
    }

    total_memory_tracker.setHardLimit(saved_total_hard_limit);
    thread_tracker.setHardLimit(saved_thread_hard_limit);
    CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
    CurrentMemoryTracker::setMinAllocationSizeBytesToThrow(saved_min_alloc_to_throw);

    ASSERT_TRUE(empty_insert_failed);
    EXPECT_TRUE(empty_set.sourceIsEmpty());
    EXPECT_FALSE(empty_set.hasNull());
    empty_set.finishInsert();

    auto null_probe = type->createColumn();
    null_probe->insert(Field{});
    auto empty_membership = empty_set.execute({ColumnWithTypeAndName(std::move(null_probe), type, "x")}, false);
    ASSERT_EQ(empty_membership->size(), 1);
    EXPECT_FALSE(empty_membership->getBool(0));
}
