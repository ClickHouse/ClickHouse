#pragma once

#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{

template <class Large, class Numerator, class Denominator>
class AggregateFunctionAvgWeighted final :
    public AggregateFunctionAvgBase<Large, Numerator, Denominator,
        AggregateFunctionAvgWeighted<Large, Numerator, Denominator>>
{
public:
    using AggregateFunctionAvgBase<Large, Numerator, Denominator,
        AggregateFunctionAvgWeighted<Large, Numerator, Denominator>>::AggregateFunctionAvgBase;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const DecimalOrVectorCol<Numerator> &>(*columns[0]);
        const auto & weights = static_cast<const DecimalOrVectorCol<Denominator> &>(*columns[1]);

        const Numerator value = static_cast<Numerator>(values.getData()[row_num]);
        const auto weight = weights.getData()[row_num];

        this->data(place).numerator += value * weight;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};
}
