#pragma once

#include <IO/VarInt.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <AggregateFunctions/INullaryAggregateFunction.h>
#include <AggregateFunctions/IUnaryAggregateFunction.h>
#include <Columns/ColumnNullable.h>


namespace DB
{

struct AggregateFunctionCountData
{
    UInt64 count = 0;
};


/// Simply count number of calls.
class AggregateFunctionCount final : public INullaryAggregateFunction<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    String getName() const override { return "count"; }

    void setArguments(const DataTypes & arguments) override
    {
        /// You may pass some arguments. All of them are ignored.
    }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void addImpl(AggregateDataPtr place) const
    {
        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

    /// May be used for optimization.
    void addDelta(AggregateDataPtr place, UInt64 x) const
    {
        data(place).count += x;
    }
};


/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final : public IUnaryAggregateFunction<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>
{
public:
    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void addImpl(AggregateDataPtr place, const IColumn & column, size_t row_num, Arena * arena) const
    {
        data(place).count += !static_cast<const ColumnNullable &>(column).isNullAt(row_num);
    }

    void setArgument(const DataTypePtr & argument)
    {
        if (!argument->isNullable() && !argument->isNull())
            throw Exception("Not Nullable argument passed to aggregate function count", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }
};


/// Count number of calls where all arguments are not NULL.
class AggregateFunctionCountNotNullVariadic final : public IAggregateFunctionHelper<AggregateFunctionCountData>
{
public:
    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void setArguments(const DataTypes & arguments) override
    {
        number_of_arguments = arguments.size();

        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionCountNotNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable() || arguments[i]->isNull();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        for (size_t i = 0; i < number_of_arguments; ++i)
            if (is_nullable[i] && static_cast<const ColumnNullable &>(*columns[i]).isNullAt(row_num))
                return;

        ++data(place).count;
    }

    static void addFree(const IAggregateFunction * that, AggregateDataPtr place,
        const IColumn ** columns, size_t row_num, Arena * arena)
    {
        return static_cast<const AggregateFunctionCountNotNullVariadic &>(*that).add(place, columns, row_num, arena);
    }

    AddFunc getAddressOfAddFunction() const override
    {
        return &addFree;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr place, ReadBuffer & buf, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(ConstAggregateDataPtr place, IColumn & to) const override
    {
        static_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

}
