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
#include <base/scope_guard.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
extern const int TIMEOUT_EXCEEDED;
}

/// Every row counts as at least one work unit, so a block of rows that produce nothing still reaches a
/// cancellation checkpoint. Removing that floor leaves a 120 million row block running 1.86 seconds,
/// inside the smallest bound 04818 allows, hence a throw on an expired deadline rather than a latency.
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

/// One block of `rows` rows whose first argument is an invalid cell index, so every row produces an
/// empty array and only the per-row floor can advance the checkpoint counter.
void assertDegenerateBlockIsCancelled(const String & function_name, const DataTypePtr & second_type, UInt64 second_value)
{
    static constexpr size_t rows = 100'000;  /// exactly one throttle unit

    ExpiredDeadlineQuery query;

    auto invalid_cells = ColumnUInt64::create(rows, 1);
    auto second = second_type->createColumn();
    for (size_t i = 0; i < rows; ++i)
        second->insert(second_value);

    ColumnsWithTypeAndName arguments{
        {std::move(invalid_cells), std::make_shared<DataTypeUInt64>(), "cell"},
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

}

TEST(H3ArrayExpansionCancellation, KRingHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3kRing", std::make_shared<DataTypeUInt16>(), 100);
}

TEST(H3ArrayExpansionCancellation, HexRingHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3HexRing", std::make_shared<DataTypeUInt16>(), 100);
}

TEST(H3ArrayExpansionCancellation, LineHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3Line", std::make_shared<DataTypeUInt64>(), 1);
}

TEST(H3ArrayExpansionCancellation, ToChildrenHonorsDeadlineOnZeroOutputRows)
{
    tryRegisterFunctions();
    assertDegenerateBlockIsCancelled("h3ToChildren", std::make_shared<DataTypeUInt8>(), 9);
}

#endif
