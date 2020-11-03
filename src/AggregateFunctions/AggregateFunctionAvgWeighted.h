#pragma once

#include <type_traits>
#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{
template <class Value, class Weight>
class AggregateFunctionAvgWeighted final :
    public AggregateFunctionAvgBase<Float64, AggregateFunctionAvgWeighted<Value, Weight>>
{
public:
    using Base = AggregateFunctionAvgBase<Float64, AggregateFunctionAvgWeighted<Value, Weight>>;
    using Base::Base;

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const Float64 value = [&columns, row_num] {
            if constexpr(IsDecimalNumber<Value>)
                return columns[0]->getFloat64(row_num);
            else
                return static_cast<Float64>(static_cast<const ColumnVector<Value>&>(*columns[0]).getData()[row_num]);
        }();

        using WeightRet = std::conditional_t<DecimalOrExtendedInt<Weight>, Float64, Weight>;
        const WeightRet weight = [&columns, row_num]() -> WeightRet {
            if constexpr(IsDecimalNumber<Weight>) /// Unable to cast to double -> use the virtual method
                return columns[1]->getFloat64(row_num);
            else if constexpr(DecimalOrExtendedInt<Weight>) /// Casting to double, otherwise += would be ambitious.
                return static_cast<Float64>(static_cast<const ColumnVector<Weight>&>(*columns[1]).getData()[row_num]);
            else
                return static_cast<const ColumnVector<Weight>&>(*columns[1]).getData()[row_num];
        }();

        this->data(place).numerator += weight * value;
        this->data(place).denominator += weight;
    }

    String getName() const override { return "avgWeighted"; }
};
}
