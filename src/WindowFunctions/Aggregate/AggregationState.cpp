#include <WindowFunctions/Aggregate/AggregationState.h>
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

[[noreturn]] NO_INLINE void throwNotCreated()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregate state of window function is used after a failed creation");
}

ALWAYS_INLINE void throwIfNot(bool condition)
{
    if (unlikely(!condition))
        throwNotCreated();
}

}

AggregationState::AggregationState(AggregateFunctionPtr function_)
    : function(std::move(function_))
    , is_state(function->isState())
    , place(function->sizeOfData(), function->alignOfData())
    , check_cancellation(makeCancellationCheck(function->getName()))
    , budget(check_cancellation)
{
    if (function->allocatesMemoryInArena())
        arena = std::make_unique<Arena>();

    function->create(place.data());
    created = true;
}

AggregationState::~AggregationState()
{
    if (created)
        function->destroy(place.data());
}

void AggregationState::recreate()
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

void AggregationState::add(const WindowArguments & arguments, Int64 begin, Int64 end)
{
    throwIfNot(created);
    while (begin < end)
    {
        const ArgumentsSegment segment = arguments.segment(begin, end);
        budget.chargeUnits(segment.row_end - segment.row_begin);
        function->addBatchSinglePlace(segment.row_begin, segment.row_end, place.data(), const_cast<const IColumn **>(segment.columns.data()), arena.get());
        begin += segment.row_end - segment.row_begin;
    }
}

void AggregationState::merge(ConstAggregateDataPtr rhs)
{
    throwIfNot(created);
    function->merge(place.data(), rhs, arena.get());
}

void AggregationState::insertResultInto(IColumn & result)
{
    throwIfNot(created);
    if (is_state)
        function->insertMergeResultInto(place.data(), result, arena.get());
    else
        function->insertResultInto(place.data(), result, arena.get());
}

ConstAggregateDataPtr AggregationState::getData() const
{
    throwIfNot(created);
    return place.data();
}

}
