#include <gtest/gtest.h>

#include <Columns/ColumnArray.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/assert_cast.h>
#include <Common/scope_guard_safe.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <Interpreters/Set.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

namespace
{

/// Few rows, each holding one large value, so that the collected values end up in a buffer that
/// is full: appending anything to them then has to grow that buffer, and growing it doubles it.
constexpr size_t rows_per_batch = 32;
constexpr size_t bytes_per_row = 1024 * 1024 - 64;

/// The batch that gets refused only has to reach past the end of that buffer, so it is tiny:
/// everything it allocates stays orders of magnitude below the limit, which leaves growing the
/// collected values as the only allocation the limit can refuse.
constexpr size_t refused_rows = 4;
constexpr size_t refused_bytes_per_row = 16 * 1024;

/// Above everything the refused batch allocates and below the doubled buffer.
constexpr Int64 memory_headroom = 32 * 1024 * 1024;

Columns makeBatch(const DataTypePtr & type, UInt64 first_id, size_t rows, size_t value_bytes)
{
    auto column = type->createColumn();
    for (size_t i = 0; i < rows; ++i)
    {
        /// Two values of different kinds per row. With values of a single kind the copy below
        /// takes a shortcut that appends in a different order, where the defect cannot appear.
        Array value;
        value.push_back(Field(String(value_bytes, 'x')));
        value.push_back(Field(first_id + i));
        column->insert(Field(std::move(value)));
    }
    return Columns{std::move(column)};
}

}

/// A set that fails to collect its values because of a memory limit must leave the values it has
/// already collected unchanged and internally consistent, rather than half appended. The set
/// outlives the failure (a join keeps it and reads the values back when it merges the results of
/// its build side), so half appended values make the next reader of them raise a logical error,
/// which aborts a build that treats logical errors as assertion failures.
TEST(SetElements, CollectingValuesIsExceptionSafeUnderMemoryLimit)
{
    MainThreadStatus::getInstance();

    const auto type = DataTypeFactory::instance().get("Array(Variant(String, UInt64))");

    Set set(SizeLimits{}, /*max_elements_to_fill_=*/ 0, /*transform_null_in_=*/ false);
    set.setHeader({ColumnWithTypeAndName(type->createColumn(), type, "k")});
    set.fillSetElements();

    /// Two batches with no limit in force, which is what fills the buffer holding the collected
    /// values right up to its end.
    for (UInt64 batch = 0; batch < 2; ++batch)
        set.insertFromColumns(makeBatch(type, batch * rows_per_batch, rows_per_batch, bytes_per_row));

    /// Only so that the collected values can be read below; it does not close the set to inserts.
    set.finishInsert();

    const size_t size_before = set.getSetElements().front()->size();
    ASSERT_EQ(size_before, 2 * rows_per_batch);

    /// Built before the limit is armed, so only collecting it can fail.
    const Columns refused_batch = makeBatch(type, 2 * rows_per_batch, refused_rows, refused_bytes_per_row);

    auto & thread_tracker = CurrentThread::get().memory_tracker;
    const Int64 saved_untracked_limit = CurrentThread::get().untracked_memory_limit;
    const Int64 saved_hard_limit = thread_tracker.getHardLimit();
    SCOPE_EXIT_SAFE({
        thread_tracker.setHardLimit(saved_hard_limit);
        CurrentThread::get().untracked_memory_limit = saved_untracked_limit;
    });

    /// Account every allocation immediately, so the limit takes effect where it is expected to
    /// and not a buffer's worth of allocations later.
    CurrentThread::get().untracked_memory_limit = 0;
    CurrentThread::flushUntrackedMemory();

    /// This thread's own limit, not the process wide one: every thread of the test binary reports
    /// into the process wide tracker, so their allocations would decide where a limit on it trips.
    thread_tracker.setHardLimit(thread_tracker.get() + memory_headroom);

    bool threw = false;
    int thrown_code = 0;
    try
    {
        set.insertFromColumns(refused_batch);
    }
    catch (const Exception & e)
    {
        threw = true;
        thrown_code = e.code();
    }

    /// Lift the limit before touching the set, so the assertions themselves can allocate.
    thread_tracker.setHardLimit(saved_hard_limit);
    CurrentThread::get().untracked_memory_limit = saved_untracked_limit;

    ASSERT_TRUE(threw) << "collecting the refused batch was expected to exceed the memory limit";

    /// The rollback must rethrow the original exception, not replace it with one of its own.
    EXPECT_EQ(thrown_code, ErrorCodes::MEMORY_LIMIT_EXCEEDED);

    /// The hash table is filled before the values are collected, so a full count proves the refusal
    /// happened while collecting them and not in a step before that.
    EXPECT_EQ(set.getTotalRowCount(), 2 * rows_per_batch + refused_rows);

    const Columns elements = set.getSetElements();
    ASSERT_EQ(elements.size(), 1u);
    const ColumnPtr & values = elements.front();

    /// The refused batch must be rolled back whole, not in part.
    EXPECT_EQ(values->size(), size_before);

    /// The values and their offsets must agree, which is what the reader below checks.
    const auto & array = assert_cast<const ColumnArray &>(*values);
    EXPECT_EQ(array.getData().size(), array.getOffsets().empty() ? 0 : array.getOffsets().back());

    /// The expression a join runs when it reads the collected values back.
    ColumnPtr read_back;
    EXPECT_NO_THROW(read_back = values->convertToFullIfWrapped());
}
