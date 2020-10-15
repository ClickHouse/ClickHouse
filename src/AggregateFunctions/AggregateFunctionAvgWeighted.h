#pragma once

#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{

template <typename Value, typename Weight, typename Largest, typename Data>
class AggregateFunctionAvgWeighted final :
    public AggregateFunctionAvgBase<Largest, Data, AggregateFunctionAvgWeighted<Value, Weight, Largest, Data>>
{
public:
    using AggregateFunctionAvgBase<Largest, Data,
        AggregateFunctionAvgWeighted<Value, Weight, Largest, Data>>::AggregateFunctionAvgBase;

    template <class T>
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const ColVecType<Value> &>(*columns[0]);
        const auto & weights = static_cast<const ColVecType<Weight> &>(*columns[1]);

        const auto value = values.getData()[row_num];
        const auto weight = weights.getData()[row_num];

        this->data(place).numerator += static_cast<typename Data::NumeratorType>(value) * weight;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};

}
