#pragma once

#include <AggregateFunctions/IAggregateFunction.h>
#include <WindowFunctions/IGeneralWindowFunction.h>

namespace DB
{

/// A fake aggregate function that carries a general window function through the aggregate function factory.
class WindowFunction final : public IAggregateFunctionHelper<WindowFunction>
{
public:
    explicit WindowFunction(GeneralWindowFunctionPtr definition_);

    const GeneralWindowFunctionPtr & getDefinition() const;

    String getName() const override;
    bool isOnlyWindowFunction() const override;
    bool allocatesMemoryInArena() const override;
    size_t sizeOfData() const override;
    size_t alignOfData() const override;
    bool hasTrivialDestructor() const override;
    void create(AggregateDataPtr __restrict place) const override;
    void destroy(AggregateDataPtr __restrict place) const noexcept override;
    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override;
    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override;
    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> version) const override;
    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override;
    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override;

private:
    const GeneralWindowFunctionPtr definition;
};

}
