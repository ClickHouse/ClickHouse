#pragma once

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnVector.h>

#include <AggregateFunctions/IUnaryAggregateFunction.h>


namespace DB
{


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
class AggregateFunctionBitwise final : public IUnaryAggregateFunction<Data, AggregateFunctionBitwise<T, Data>>
{
public:
    String getName() const override { return Data::name(); }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeNumber<T>>();
    }

    void setArgument(const DataTypePtr & argument)
    {
        if (!argument->behavesAsNumber())
            throw Exception("Illegal type " + argument->getName() + " of argument for aggregate function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena *) const
    {
        this->data(place).update(static_cast<const ColumnVector<T> &>(column).getData()[row_num]);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        this->data(place).update(this->data(rhs).value);
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeBinary(this->data(place).value, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readBinary(this->data(place).value, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnVector<T> &>(to).getData().push_back(this->data(place).value);
    }
};


}
