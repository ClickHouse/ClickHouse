#pragma once

#include <AggregateFunctions/AggregateFunctionAvg.h>

namespace DB
{


template <typename T>
struct AggregateFunctionAvgWeightedData
{
    T numerator = 0;
    T denominator = 0;

    template <typename ResultT>
    ResultT NO_SANITIZE_UNDEFINED result() const
    {
        if constexpr (std::is_floating_point_v<ResultT>)
            if constexpr (std::numeric_limits<ResultT>::is_iec559)
                return static_cast<ResultT>(numerator) / denominator; /// allow division by zero

        if (denominator == 0)
            return static_cast<ResultT>(0);
        return static_cast<ResultT>(numerator / denominator);
    }
};

template <typename T, typename Data>
class AggregateFunctionAvgWeighted final : public AggregateFunctionAvgBase<Data, T, AggregateFunctionAvgWeighted<T, Data>>
{
public:

	AggregateFunctionAvgWeighted(const DataTypes & argument_types_)
	: AggregateFunctionAvgBase<Data, T, AggregateFunctionAvgWeighted<T, Data>>(argument_types_) {}
	
	AggregateFunctionAvgWeighted(const IDataType & data_type, const DataTypes & argument_types_)
	: AggregateFunctionAvgBase<Data, T, AggregateFunctionAvgWeighted<T, Data>>(data_type, argument_types_)
	{}

	using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & values = static_cast<const ColVecType &>(*columns[0]);
        const auto & weights = static_cast<const ColVecType &>(*columns[1]);

        this->data(place).numerator += values.getData()[row_num] * weights.getData()[row_num];
        this->data(place).denominator += weights.getData()[row_num];
    }

    String getName() const override { return "avgWeighted"; }

};

}
