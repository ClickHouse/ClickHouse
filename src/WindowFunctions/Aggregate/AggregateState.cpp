#include <WindowFunctions/Aggregate/AggregateState.h>
#include <WindowFunctions/Common/WindowArguments.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Columns/IColumn.h>

#include <Common/Arena.h>

namespace DB
{

namespace
{

constexpr size_t arena_replace_bytes = 16 << 20;

}

AggregateState::AggregateState(AggregateFunctionPtr function_)
    : function(std::move(function_))
    , place(function->sizeOfData(), function->alignOfData())
    , is_state(function->isState())
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
        function->addBatchSinglePlace(segment.row_begin, segment.row_end, place.data(), const_cast<const IColumn **>(segment.columns.data()), arena.get());
        begin += segment.row_end - segment.row_begin;
    }
}

void AggregateState::insertResultInto(IColumn & result)
{
    if (is_state)
        function->insertMergeResultInto(place.data(), result, arena.get());
    else
        function->insertResultInto(place.data(), result, arena.get());
}

}
