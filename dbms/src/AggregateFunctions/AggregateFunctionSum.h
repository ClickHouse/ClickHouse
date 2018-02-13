#pragma once

#include <type_traits>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
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
    String getName() const override { return "sum"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<TResult>>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).add(static_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
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
        static_cast<ColumnVector<TResult> &>(to).getData().push_back(this->data(place).get());
    }

    const char * getHeaderFilePath() const override { return __FILE__; }
};


}
