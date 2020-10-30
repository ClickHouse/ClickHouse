#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<Float64, true, AggregateFunctionAvgWeighted>
{
public:
    using AggregateFunctionAvgBase<Float64, true, AggregateFunctionAvgWeighted>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto value = columns[0]->getFloat64(row_num);
        const auto weight = columns[1]->getFloat64(row_num);

        this->data(place).numerator += value * weight;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};
}
