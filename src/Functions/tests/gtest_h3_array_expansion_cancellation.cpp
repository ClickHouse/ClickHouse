#include <gtest/gtest.h>

#include "config.h"

#if USE_H3

#include <Columns/ColumnsNumber.h>
#include <Common/CurrentThread.h>
#include <Common/Stopwatch.h>
#include <Common/ThreadStatus.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
}

/// Two properties of the checkpoint counter, neither of which admits a latency oracle.
///
/// The per-row floor: a row that produces nothing still counts as one work unit. Removing it leaves a
/// 120 million row block running 1.86 seconds, inside the smallest bound 04818 allows. `h3Line` has two
/// loops and this pins their floors jointly, since its sizing pass reaches the threshold one pass
/// before the fill pass and so removing either alone still stops the block.
///
/// The item weighting: a row is worth its output size, because one row can hold 300030001 items for
/// `h3kRing` and 1073741824 for `h3ToChildren`. Removing it leaves a counter that advances once per
/// row, which every 04818 shape still reaches - they have millions of rows - so those pass on a binary
/// with no weighting at all. These cases use fewer rows than the threshold and more items than it.
namespace
{

struct ExpiredDeadlineQuery
{
    std::optional<ThreadStatus> thread_status;
    ContextMutablePtr context;
    ThreadGroupPtr thread_group;
    ProcessList::EntryPtr entry;

    ExpiredDeadlineQuery()
    {
        thread_status.emplace();
        context = Context::createCopy(getContext().context);
        context->setSetting("max_execution_time", 1);
        context->setSetting("functions_h3_default_if_invalid", 1);
        thread_group = std::make_shared<ThreadGroup>(context, 0);
        CurrentThread::attachToGroup(thread_group);
        context->setCurrentQueryId("");  /// generates a fresh random id; an empty one is rejected
        /// A past watch start expires the deadline at the first check rather than after a wait, and
        /// must come from `CLOCK_MONOTONIC`, the clock the status's watch runs on: the default clock
        /// is `CLOCK_MONOTONIC_RAW`, which drifts from it by seconds and would expire it by itself.
        entry = context->getProcessList().insert(
            /*query_=*/"", /*normalized_query_hash=*/0, /*ast=*/nullptr, context,
            /*watch_start_nanoseconds=*/clock_gettime_ns(CLOCK_MONOTONIC) - 10'000'000'000,
            /*is_internal=*/true);
        context->setProcessListElement(entry->getQueryStatus());
    }

    ~ExpiredDeadlineQuery()
    {
        context->setProcessListElement(nullptr);
        entry.reset();
        CurrentThread::detachFromGroupIfNotDetached();
    }
};

/// One block of `rows` rows, both arguments constant, run against an already expired deadline.
void assertBlockIsCancelled(
    const String & function_name, UInt64 cell, const DataTypePtr & second_type, UInt64 second_value, size_t rows)
{
    ExpiredDeadlineQuery query;

    auto cells = ColumnUInt64::create(rows, cell);
    auto second = second_type->createColumn();
    for (size_t i = 0; i < rows; ++i)
        second->insert(second_value);

    ColumnsWithTypeAndName arguments{
        {std::move(cells), std::make_shared<DataTypeUInt64>(), "cell"},
        {std::move(second), second_type, "arg"}};

    auto function = FunctionFactory::instance().get(function_name, query.context)->build(arguments);

    try
    {
        function->execute(arguments, function->getResultType(), rows, /*dry_run=*/false);
        FAIL() << function_name << " returned instead of reporting the expired deadline";
    }
    catch (const Exception & e)
    {
        ASSERT_EQ(e.code(), ErrorCodes::TIMEOUT_EXCEEDED) << function_name << ": " << e.message();
    }
}

/// Every row's cell index is invalid, so every row produces an empty array and only the per-row floor
/// can advance the counter. One throttle unit of rows, so the floor reaches it on the last row.
void assertDegenerateBlockIsCancelled(const String & function_name, const DataTypePtr & second_type, UInt64 second_value)
{
    assertBlockIsCancelled(function_name, /*cell=*/1, second_type, second_value, /*rows=*/10'000);
}

}

class H3ArrayExpansionCancellation : public ::testing::Test
{
public:
    H3ArrayExpansionCancellation()
    {
        /// Another test in this binary may have initialized the process-lifetime `MainThreadStatus`,
        /// which leaves `current_thread` set on this thread; the `ThreadStatus` these cases create
        /// asserts that it is null.
        previous_thread_status = current_thread;
        current_thread = nullptr;
    }

    ~H3ArrayExpansionCancellation() override { current_thread = previous_thread_status; }

private:
    ThreadStatus * previous_thread_status = nullptr;
};

TEST_F(H3ArrayExpansionCancellation, KRingHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3kRing", std::make_shared<DataTypeUInt16>(), 100);
}

TEST_F(H3ArrayExpansionCancellation, HexRingHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3HexRing", std::make_shared<DataTypeUInt16>(), 100);
}

TEST_F(H3ArrayExpansionCancellation, LineHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3Line", std::make_shared<DataTypeUInt64>(), 1);
}

TEST_F(H3ArrayExpansionCancellation, ToChildrenHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3ToChildren", std::make_shared<DataTypeUInt8>(), 9);
}

/// Valid cells, fewer rows than the 10000 threshold but more items than it, so the counter can only
/// reach the threshold by weighting each row with its output size. Row counts are a third or more
/// clear of where the weighted counter fires, and every block stays under 2 MB.
TEST_F(H3ArrayExpansionCancellation, KRingWeightsRowsByItemCount)
{
    tryRegisterFunctions();
    /// k = 20 is 3k^2+3k+1 = 1261 cells per row; 100 rows fire at row 9.
    assertBlockIsCancelled("h3kRing", 644325529233966508, std::make_shared<DataTypeUInt16>(), 20, /*rows=*/100);
}

TEST_F(H3ArrayExpansionCancellation, HexRingWeightsRowsByItemCount)
{
    tryRegisterFunctions();
    /// k = 20 is 6k = 120 cells per row; 1000 rows fire at row 84.
    assertBlockIsCancelled("h3HexRing", 644325529233966508, std::make_shared<DataTypeUInt16>(), 20, /*rows=*/1000);
}

TEST_F(H3ArrayExpansionCancellation, LineWeightsRowsByItemCountInTheSizingPass)
{
    tryRegisterFunctions();
    /// A cell to itself is one item per row, so the weighted sizing counter advances by two a row and
    /// fires at row 5001 while the fill pass, whose weighting this case does not test, stays at 6667 of
    /// the threshold's 10000. A longer line would reach the threshold in the fill pass instead and the
    /// case would pass whether or not the sizing pass weights its rows.
    assertBlockIsCancelled("h3Line", 621807531097128959, std::make_shared<DataTypeUInt64>(), 621807531097128959, /*rows=*/6'667);
}

TEST_F(H3ArrayExpansionCancellation, LineWeightsRowsByItemCountInTheFillPass)
{
    tryRegisterFunctions();
    /// The sizing pass tests before adding the row's own size, so with two rows of a measured 6641 items
    /// its checkpoints see 1 and 6643, both short of the threshold's 10000, and the block ends before a
    /// third one could see the 13284 the counter finishes on. The fill pass tests after adding, so it
    /// reaches 13282 on row 2. Only the fill pass can fire, and only if it weights its rows.
    assertBlockIsCancelled("h3Line", 646078419604526808, std::make_shared<DataTypeUInt64>(), 646078420123713713, /*rows=*/2);
}

TEST_F(H3ArrayExpansionCancellation, ToChildrenWeightsRowsByItemCount)
{
    tryRegisterFunctions();
    /// A resolution 9 parent has 7^4 = 2401 children at resolution 13; 100 rows fire at row 6.
    assertBlockIsCancelled("h3ToChildren", 617303931469955071, std::make_shared<DataTypeUInt8>(), 13, /*rows=*/100);
}

#endif
