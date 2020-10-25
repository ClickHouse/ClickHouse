#pragma once

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypesNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
namespace ErrorCodes
{
}

template <typename T, typename Denominator>
struct AggregateFunctionAvgData
{
    using NumeratorType = T;
    using DenominatorType = Denominator;

    T numerator = 0;
    Denominator denominator = 0;

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

/// Calculates arithmetic mean of numbers.
template <typename T, typename Data, typename Derived>
class AggregateFunctionAvgBase : public IAggregateFunctionDataHelper<Data, Derived>
{
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, T, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<T>, DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<Float64>>;

    /// ctor for native types
    AggregateFunctionAvgBase(const DataTypes & argument_types_) : IAggregateFunctionDataHelper<Data, Derived>(argument_types_, {}), scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvgBase(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, Derived>(argument_types_, {}), scale(getDecimalScale(data_type))
    {
    }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<T>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).numerator += this->data(rhs).numerator;
        this->data(place).denominator += this->data(rhs).denominator;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<typename Data::DenominatorType>)
            writeVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            writeBinary(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);

        if constexpr (std::is_unsigned_v<typename Data::DenominatorType>)
            readVarUInt(this->data(place).denominator, buf);
        else /// Floating point denominator type can be used
            readBinary(this->data(place).denominator, buf);
    }

    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena *) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(this->data(place).template result<ResultType>());
    }

protected:
    UInt32 scale;
};

template <typename T, typename Data>
class AggregateFunctionAvg final : public AggregateFunctionAvgBase<T, Data, AggregateFunctionAvg<T, Data>>
{
public:
    using AggregateFunctionAvgBase<T, Data, AggregateFunctionAvg<T, Data>>::AggregateFunctionAvgBase;

    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).numerator += column.getData()[row_num];
        this->data(place).denominator += 1;
    }

    String getName() const override { return "avg"; }
};

}
