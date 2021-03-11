#pragma once

#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
template <typename T, typename Data>
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<T, Data, AggregateFunctionAvgWeighted<T, Data>>
{
public:
    using AggregateFunctionAvgBase<T, Data, AggregateFunctionAvgWeighted<T, Data>>::AggregateFunctionAvgBase;

    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const ColVecType &>(*columns[0]);
        const auto & weights = static_cast<const ColVecType &>(*columns[1]);

        this->data(place).numerator += static_cast<typename Data::NumeratorType>(values.getData()[row_num]) * weights.getData()[row_num];
        this->data(place).denominator += weights.getData()[row_num];
    }

    String getName() const override { return "avgWeighted"; }
};

}
