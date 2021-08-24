#pragma once

#include <type_traits>
#include <DataTypes/DataTypeTuple.h>
#include <AggregateFunctions/AggregateFunctionAvg.h>


namespace DB
{
template <typename T>
using DecimalOrNumberDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<AvgFieldType<T>>, DataTypeNumber<AvgFieldType<T>>>;
template <typename T>
class AggregateFunctionSumCount final : public AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionSumCount<T>>
{
public:
    using Base = AggregateFunctionAvgBase<AvgFieldType<T>, UInt64, AggregateFunctionSumCount<T>>;

    AggregateFunctionSumCount(const DataTypes & argument_types_, UInt32 num_scale_ = 0)
         : Base(argument_types_, num_scale_), scale(num_scale_) {}

    DataTypePtr getReturnType() const override
    {
        DataTypes types;
        if constexpr (IsDecimalNumber<T>)
            types.emplace_back(std::make_shared<DecimalOrNumberDataType<T>>(DecimalOrNumberDataType<T>::maxPrecision(), scale));
        else
            types.emplace_back(std::make_shared<DecimalOrNumberDataType<T>>());

        types.emplace_back(std::make_shared<DataTypeUInt64>());

        return std::make_shared<DataTypeTuple>(types);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const final
    {
        assert_cast<DecimalOrVectorCol<AvgFieldType<T>> &>((assert_cast<ColumnTuple &>(to)).getColumn(0)).getData().push_back(
            this->data(place).numerator);

        assert_cast<ColumnUInt64 &>((assert_cast<ColumnTuple &>(to)).getColumn(1)).getData().push_back(
            this->data(place).denominator);
    }

    void NO_SANITIZE_UNDEFINED add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const final
    {
        this->data(place).numerator += static_cast<const DecimalOrVectorCol<T> &>(*columns[0]).getData()[row_num];
        ++this->data(place).denominator;
    }

    String getName() const final { return "sumCount"; }

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override
    {
        return false;
    }

#endif

private:
    UInt32 scale;
};

}
