#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnsNumber.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{


template <typename T>
struct AggregateFunctionAvgData
{
    T sum = 0;
    UInt64 count = 0;
};


/// Calculates arithmetic mean of numbers.
template <typename T, typename Data>
class AggregateFunctionAvg final : public IAggregateFunctionDataHelper<Data, AggregateFunctionAvg<T, Data>>
{
public:
    using ResultType = std::conditional_t<IsDecimalNumber<T>, Decimal128, Float64>;
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<Decimal128>, DataTypeNumber<Float64>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<Decimal128>, ColumnVector<Float64>>;

    /// ctor for native types
    AggregateFunctionAvg()
        : scale(0)
    {}

    /// ctor for Decimals
    AggregateFunctionAvg(const IDataType & data_type)
        : scale(getDecimalScale(data_type))
    {}

    String getName() const override { return "avg"; }

    DataTypePtr getReturnType() const override
    {
        if constexpr (IsDecimalNumber<T>)
            return std::make_shared<ResultDataType>(ResultDataType::maxPrecision(), scale);
        else
            return std::make_shared<ResultDataType>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        const auto & column = static_cast<const ColVecType &>(*columns[0]);
        this->data(place).sum += column.getData()[row_num];
        ++this->data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).sum += this->data(rhs).sum;
        this->data(place).count += this->data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).sum, buf);
        writeVarUInt(this->data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).sum, buf);
        readVarUInt(this->data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(static_cast<ResultType>(this->data(place).sum) / this->data(place).count);
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 scale;
};


}
