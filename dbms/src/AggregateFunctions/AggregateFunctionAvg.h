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
    extern const int LOGICAL_ERROR;
}

template <typename T>
struct AggregateFunctionAvgData
{
    T numerator = 0;
    UInt64 denominator = 0;

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
template <typename Data, typename T, typename F>
class AggregateFunctionAvgBase : public IAggregateFunctionDataHelper<Data, F>
{
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, T, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<T>, DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<Float64>>;

    /// ctor for native types
    AggregateFunctionAvgBase(const DataTypes & argument_types_) : IAggregateFunctionDataHelper<Data, F>(argument_types_, {}), scale(0) {}

    /// ctor for Decimals
    AggregateFunctionAvgBase(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, F>(argument_types_, {}), scale(getDecimalScale(data_type))
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
        writeVarUInt(this->data(place).denominator, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).numerator, buf);
        readVarUInt(this->data(place).denominator, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(this->data(place).template result<ResultType>());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

protected:
    UInt32 scale;
};

template <typename T, typename Data>
class AggregateFunctionAvg final : public AggregateFunctionAvgBase<Data, T, AggregateFunctionAvg<T, Data>>
{
public:
    AggregateFunctionAvg(const DataTypes & argument_types_)
        : AggregateFunctionAvgBase<Data, T, AggregateFunctionAvg<T, Data>>(argument_types_)
    {
    }

    AggregateFunctionAvg(const IDataType & data_type, const DataTypes & argument_types_)
        : AggregateFunctionAvgBase<Data, T, AggregateFunctionAvg<T, Data>>(data_type, argument_types_)
    {
    }

    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).numerator += column.getData()[row_num];
        ++this->data(place).denominator;
    }

    String getName() const override { return "avg"; }
};

}
