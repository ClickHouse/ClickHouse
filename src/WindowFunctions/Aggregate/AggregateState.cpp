#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/Common/WindowArguments.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/IColumn.h>

#include <Common/Arena.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr size_t arena_replace_bytes = 16 << 20;

}

AggregateState::AggregateState(AggregateFunctionPtr function_)
    : function(std::move(function_))
    , place(function->sizeOfData(), function->alignOfData())
    , is_state(function->isState())
    , check_cancellation(makeCancellationCheck(function->getName()))
    , budget(check_cancellation)
{
    if (function->allocatesMemoryInArena())
        arena = std::make_unique<Arena>();

    function->create(place.data());
    created = true;
}

AggregateState::~AggregateState()
{
    if (created)
        function->destroy(place.data());
}

void AggregateState::recreate()
{
    if (created)
    {
        function->destroy(place.data());
        created = false;
    }

    if (arena && arena->allocatedBytes() > arena_replace_bytes)
        arena = std::make_unique<Arena>();

    function->create(place.data());
    created = true;
}

void AggregateState::throwNotCreated() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregate state of window function {} is used after a failed creation", function->getName());
}

void AggregateState::add(const WindowArguments & arguments, Int64 begin, Int64 end)
{
    checkCreated();
    while (begin < end)
    {
        const ArgumentsSegment segment = arguments.segment(begin, end);
        budget.chargeUnits(segment.row_end - segment.row_begin);
        function->addBatchSinglePlace(segment.row_begin, segment.row_end, place.data(), const_cast<const IColumn **>(segment.columns.data()), arena.get());
        begin += segment.row_end - segment.row_begin;
    }
}

void AggregateState::insertResultInto(IColumn & result)
{
    checkCreated();
    if (is_state)
        function->insertMergeResultInto(place.data(), result, arena.get());
    else
        function->insertResultInto(place.data(), result, arena.get());
}

}
