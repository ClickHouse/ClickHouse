#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;


template <typename T>
struct AggregateFunctionGroupBitOrData
{
    T value = 0;
    static const char * name() { return "groupBitOr"; }
    void update(T x) { value |= x; }
};

template <typename T>
struct AggregateFunctionGroupBitAndData
{
    T value = -1; /// Two's complement arithmetic, sign extension.
    static const char * name() { return "groupBitAnd"; }
    void update(T x) { value &= x; }
};

template <typename T>
struct AggregateFunctionGroupBitXorData
{
    T value = 0;
    static const char * name() { return "groupBitXor"; }
    void update(T x) { value ^= x; }
};


/// Counts bitwise operation on numbers.
template <typename T, typename Data>
class AggregateFunctionBitwise final : public IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>
{
public:
    AggregateFunctionBitwise(const DataTypePtr & type)
        : IAggregateFunctionDataHelper<Data, AggregateFunctionBitwise<T, Data>>({type}, {}) {}

    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        this->data(place).update(assert_cast<const ColumnVector<T> &>(*columns[0]).getData()[row_num]);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        this->data(place).update(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).value);
    }
};


}
