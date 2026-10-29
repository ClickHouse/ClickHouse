#include <WindowFunctions/WindowFunction.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

[[noreturn]] void failAsAggregate(const String & name)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "The function '{}' can only be used as a window function, not as an aggregate function", name);
}

}

WindowFunction::WindowFunction(GeneralWindowFunctionPtr definition_)
    : IAggregateFunctionHelper<WindowFunction>(definition_->getArgumentTypes(), definition_->getParameters(), definition_->getResultType())
    , definition(std::move(definition_))
{
}

const GeneralWindowFunctionPtr & WindowFunction::getDefinition() const
{
    return definition;
}

String WindowFunction::getName() const
{
    return definition->getName();
}

bool WindowFunction::isOnlyWindowFunction() const
{
    return true;
}

bool WindowFunction::allocatesMemoryInArena() const
{
    return false;
}

size_t WindowFunction::sizeOfData() const
{
    return 0;
}

size_t WindowFunction::alignOfData() const
{
    return 1;
}

bool WindowFunction::hasTrivialDestructor() const
{
    return true;
}

void WindowFunction::create(AggregateDataPtr __restrict) const
{
}

void WindowFunction::destroy(AggregateDataPtr __restrict) const noexcept
{
}

void WindowFunction::add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const
{
    failAsAggregate(getName());
}

void WindowFunction::mergeImpl(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const
{
    failAsAggregate(getName());
}

void WindowFunction::serialize(ConstAggregateDataPtr __restrict, WriteBuffer &, std::optional<size_t>) const
{
    failAsAggregate(getName());
}

void WindowFunction::deserialize(AggregateDataPtr __restrict, ReadBuffer &, std::optional<size_t>, Arena *) const
{
    failAsAggregate(getName());
}

void WindowFunction::insertResultInto(AggregateDataPtr __restrict, IColumn &, Arena *) const
{
    failAsAggregate(getName());
}

}
