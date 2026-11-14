#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/Common/WindowArguments.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>

#include <Columns/IColumn.h>

#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int TIMEOUT_EXCEEDED;
}

namespace
{

constexpr size_t arena_replace_bytes = 16 << 20;

/// The most units a single finalization is charged: at least 64 finalizations per cancellation check.
constexpr Int64 max_finalization_units = CancellationBudget::units_per_check / 64;

std::function<void()> makeTimeLimitCheck(const String & function_name)
{
    QueryStatusPtr query_status;
    if (auto query_context = CurrentThread::tryGetQueryContext())
        query_status = query_context->getProcessListElementSafe();

    if (!query_status)
        return {};

    return [query_status, function_name]
    {
        if (!query_status->checkTimeLimit())
            throw Exception(
                ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: elapsed time limit reached in window function {}", function_name);
    };
}

}

AggregateState::AggregateState(AggregateFunctionPtr function_)
    : function(std::move(function_))
    , place(function->sizeOfData(), function->alignOfData())
    , is_state(function->isState())
    , check_cancellation(makeTimeLimitCheck(function->getName()))
    , budget(check_cancellation)
{
    if (function->allocatesMemoryInArena())
        arena = std::make_unique<Arena>();

    function->create(place.data());
}

AggregateState::~AggregateState()
{
    function->destroy(place.data());
}

void AggregateState::recreate()
{
    function->destroy(place.data());
    if (arena && arena->allocatedBytes() > arena_replace_bytes)
        arena = std::make_unique<Arena>();

    function->create(place.data());
}

void AggregateState::add(const WindowArguments & arguments, Int64 begin, Int64 end)
{
    while (begin < end)
    {
        const ArgumentsSegment segment = arguments.segment(begin, end);
        budget.chargeUnits(segment.row_end - segment.row_begin);
        function->addBatchSinglePlace(segment.row_begin, segment.row_end, place.data(), const_cast<const IColumn **>(segment.columns.data()), arena.get());
        begin += segment.row_end - segment.row_begin;
    }
}

void AggregateState::insertResultInto(IColumn & result, Int64 frame_rows)
{
    /// Without this, a run of finalizations that add few rows each, such as the growing prefix of a moving frame
    /// or an unbounded preceding frame, would not reach a cancellation check for a very long time. The charge is
    /// capped, so that cheap finalizations of large frames (e.g. `sum`) do not check the time limit on every row.
    budget.chargeUnits(1 + static_cast<size_t>(std::clamp<Int64>(frame_rows, 0, max_finalization_units)));

    if (is_state)
        function->insertMergeResultInto(place.data(), result, arena.get());
    else
        function->insertResultInto(place.data(), result, arena.get());
}

}
