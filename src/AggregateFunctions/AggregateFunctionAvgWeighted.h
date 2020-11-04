#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
template <class T>
using FieldType = std::conditional_t<IsDecimalNumber<T>,
    std::conditional_t<std::is_same_v<T, Decimal256>,
        Decimal256, Decimal128>,
        NearestFieldType<T>>;

template <class Value, class Weight>
class AggregateFunctionAvgWeighted final :
    public AggregateFunctionAvgBase<FieldType<Value>, FieldType<Weight>, AggregateFunctionAvgWeighted<Value, Weight>>
{
public:
    using Base = AggregateFunctionAvgBase<
        FieldType<Value>, FieldType<Weight>, AggregateFunctionAvgWeighted<Value, Weight>>;
    using Base::Base;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const Value value = static_cast<const DecimalOrVectorCol<Value> &>(*columns[0]).getData()[row_num];
        const Weight weight = static_cast<const DecimalOrVectorCol<Weight> &>(*columns[1]).getData()[row_num];

        this->data(place).numerator += static_cast<FieldType<Value>>(value) * weight;
        this->data(place).denominator += static_cast<FieldType<Weight>>(weight);
    }

    String getName() const override { return "avgWeighted"; }
};
}
