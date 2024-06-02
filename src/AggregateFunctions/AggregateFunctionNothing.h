#pragma once

#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <Columns/IColumn.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

/// Returns the same type as the first argument
struct NameAggregateFunctionNothing { static constexpr auto name = "nothing"; };
/// Always returns Nullable(Nothing)
struct NameAggregateFunctionNothingNull { static constexpr auto name = "nothingNull"; };
/// Always returns UInt64
struct NameAggregateFunctionNothingUInt64 { static constexpr auto name = "nothingUInt64"; };

template <typename Name> class AggregateFunctionNothingImpl;

using AggregateFunctionNothing = AggregateFunctionNothingImpl<NameAggregateFunctionNothing>;
using AggregateFunctionNothingNull = AggregateFunctionNothingImpl<NameAggregateFunctionNothingNull>;
using AggregateFunctionNothingUInt64 = AggregateFunctionNothingImpl<NameAggregateFunctionNothingUInt64>;


/** Aggregate function that takes arbitrary number of arbitrary arguments and does nothing.
  */
template <typename Name>
class AggregateFunctionNothingImpl final : public IAggregateFunctionHelper<AggregateFunctionNothingImpl<Name>>
{
    static DataTypePtr getReturnType(const DataTypes & arguments [[maybe_unused]])
    {
        if constexpr (std::is_same_v<Name, NameAggregateFunctionNothingUInt64>)
            return std::make_shared<DataTypeUInt64>();
        else if constexpr (std::is_same_v<Name, NameAggregateFunctionNothingNull>)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
        return arguments.empty() ? std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>()) : arguments.front();
    }

public:
    AggregateFunctionNothingImpl(const DataTypes & arguments, const Array & params)
        : IAggregateFunctionHelper<AggregateFunctionNothingImpl<Name>>(arguments, params, getReturnType(arguments))
    {
    }

    String getName() const override { return Name::name; }

    bool allocatesMemoryInArena() const override { return false; }

    void create(AggregateDataPtr __restrict) const override
    {
    }

    void destroy(AggregateDataPtr __restrict) const noexcept override
    {
    }

    bool hasTrivialDestructor() const override
    {
        return true;
    }

    size_t sizeOfData() const override
    {
        return 0;
    }

    size_t alignOfData() const override
    {
        return 1;
    }

    void add(AggregateDataPtr __restrict, const IColumn **, size_t, Arena *) const override
    {
    }

    void merge(AggregateDataPtr __restrict, ConstAggregateDataPtr, Arena *) const override
    {
    }

    void serialize(ConstAggregateDataPtr __restrict, WriteBuffer & buf, std::optional<size_t>) const override
    {
        writeChar('\0', buf);
    }

    void deserialize(AggregateDataPtr __restrict, ReadBuffer & buf, std::optional<size_t>, Arena *) const override
    {
        [[maybe_unused]] char symbol;
        readChar(symbol, buf);
        if (symbol != '\0')
            throw Exception(ErrorCodes::INCORRECT_DATA, "Incorrect state of aggregate function '{}', it should contain exactly one zero byte, while it is {}",
                getName(), static_cast<UInt32>(symbol));
    }

    void insertResultInto(AggregateDataPtr __restrict, IColumn & to, Arena *) const override
    {
        to.insertDefault();
    }
};

}
