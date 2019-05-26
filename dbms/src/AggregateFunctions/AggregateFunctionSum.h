#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

template <typename T>
struct AggregateFunctionSumData
{
    T sum{};

    void add(T value)
    {
        sum += value;
    }

    void merge(const AggregateFunctionSumData & rhs)
    {
        sum += rhs.sum;
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
    }

    T get() const
    {
        return sum;
    }
};

template <typename T>
struct AggregateFunctionSumKahanData
{
    static_assert(std::is_floating_point_v<T>,
        "It doesn't make sense to use Kahan Summation algorithm for non floating point types");

    T sum{};
    T compensation{};

    void add(T value)
    {
        auto compensated_value = value - compensation;
        auto new_sum = sum + compensated_value;
        compensation = (new_sum - sum) - compensated_value;
        sum = new_sum;
    }

    void merge(const AggregateFunctionSumKahanData & rhs)
    {
        auto raw_sum = sum + rhs.sum;
        auto rhs_compensated = raw_sum - sum;
        auto compensations = ((rhs.sum - rhs_compensated) + (sum - (raw_sum - rhs_compensated))) + compensation + rhs.compensation;
        sum = raw_sum + compensations;
        compensation = compensations - (sum - raw_sum);
    }

    void write(WriteBuffer & buf) const
    {
        writeBinary(sum, buf);
        writeBinary(compensation, buf);
    }

    void read(ReadBuffer & buf)
    {
        readBinary(sum, buf);
        readBinary(compensation, buf);
    }

    T get() const
    {
        return sum;
    }
};


/// Counts the sum of the numbers.
template <typename T, typename TResult, typename Data>
class AggregateFunctionSum final : public IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>
{
public:
    using ResultDataType = std::conditional_t<IsDecimalNumber<T>, DataTypeDecimal<TResult>, DataTypeNumber<TResult>>;
    using ColVecType = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<T>, ColumnVector<T>>;
    using ColVecResult = std::conditional_t<IsDecimalNumber<T>, ColumnDecimal<TResult>, ColumnVector<TResult>>;

    String getName() const override { return "sum"; }

    AggregateFunctionSum(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(argument_types_, {})
        , scale(0)
    {}

    AggregateFunctionSum(const IDataType & data_type, const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionSum<T, TResult, Data>>(argument_types_, {})
        , scale(getDecimalScale(data_type))
    {}

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
        this->data(place).add(column.getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).merge(this->data(rhs));
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        this->data(place).write(buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        this->data(place).read(buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        auto & column = static_cast<ColVecResult &>(to);
        column.getData().push_back(this->data(place).get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    UInt32 scale;
};

}
