#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<Float64, AggregateFunctionAvgWeighted>
{
public:
    using AggregateFunctionAvgBase<Float64, AggregateFunctionAvgWeighted>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values  = static_cast<const ColumnVector<Float64> &>(*columns[0]);
        const auto & weights = static_cast<const ColumnVector<Float64> &>(*columns[1]);

        const auto value = values.getData()[row_num];
        const auto weight = weights.getData()[row_num];

        this->data(place).numerator += value * weight;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};
}
