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
    for (const ArgumentsSegment & segment : arguments.segments(begin, end))
        function->addBatchSinglePlace(segment.row_begin, segment.row_end, place.data(), const_cast<const IColumn **>(segment.columns.data()), arena.get());
}

void AggregateState::insertResultInto(IColumn & result)
{
    function->insertMergeResultInto(place.data(), result, arena.get());
}

}
