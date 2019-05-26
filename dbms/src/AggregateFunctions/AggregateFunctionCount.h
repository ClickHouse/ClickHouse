#pragma once

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <array>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnNullable.h>
#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{

struct AggregateFunctionCountData
{
    UInt64 count = 0;
};

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// Simply count number of calls.
class AggregateFunctionCount final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    AggregateFunctionCount(const DataTypes & argument_types_) : IAggregateFunctionDataHelper(argument_types_, {}) {}

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn **, size_t, Arena *) const override
    {
        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
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

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Simply count number of not-NULL values.
class AggregateFunctionCountNotNullUnary final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>
{
public:
    AggregateFunctionCountNotNullUnary(const DataTypePtr & argument, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullUnary>({argument}, params)
    {
        if (!argument->isNullable())
            throw Exception("Logical error: not Nullable data type passed to AggregateFunctionCountNotNullUnary", ErrorCodes::LOGICAL_ERROR);
    }

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        data(place).count += !static_cast<const ColumnNullable &>(*columns[0]).isNullAt(row_num);
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
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

    const char * getHeaderFilePath() const override { return __FILE__; }
};


/// Count number of calls where all arguments are not NULL.
class AggregateFunctionCountNotNullVariadic final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullVariadic>
{
public:
    AggregateFunctionCountNotNullVariadic(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCountNotNullVariadic>(arguments, params)
    {
        number_of_arguments = arguments.size();

        if (number_of_arguments == 1)
            throw Exception("Logical error: single argument is passed to AggregateFunctionCountNotNullVariadic", ErrorCodes::LOGICAL_ERROR);

        if (number_of_arguments > MAX_ARGS)
            throw Exception("Maximum number of arguments for aggregate function with Nullable types is " + toString(size_t(MAX_ARGS)),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        for (size_t i = 0; i < number_of_arguments; ++i)
            is_nullable[i] = arguments[i]->isNullable();
    }

    String getName() const override { return "count"; }

    DataTypePtr getReturnType() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        for (size_t i = 0; i < number_of_arguments; ++i)
            if (is_nullable[i] && static_cast<const ColumnNullable &>(*columns[i]).isNullAt(row_num))
                return;

        ++data(place).count;
    }

    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena *) const override
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

    const char * getHeaderFilePath() const override { return __FILE__; }

private:
    enum { MAX_ARGS = 8 };
    size_t number_of_arguments = 0;
    std::array<char, MAX_ARGS> is_nullable;    /// Plain array is better than std::vector due to one indirection less.
};

}
