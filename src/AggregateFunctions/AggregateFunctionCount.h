#pragma once

#include "config.h"

#include <IO/VarInt.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsCommon.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/assert_cast.h>


namespace DB
{
struct Settings;

struct AggregateFunctionCountData
{
    UInt64 count = 0;
};


/// Simply count number of calls.
class AggregateFunctionCount final : public IAggregateFunctionDataHelper<AggregateFunctionCountData, AggregateFunctionCount>
{
public:
    explicit AggregateFunctionCount(const DataTypes & argument_types_)
        : IAggregateFunctionDataHelper(argument_types_, {}, createResultType())
    {}

    String getName() const override { return "count"; }

    static DataTypePtr createResultType()
    {
        return std::make_shared<DataTypeUInt64>();
    }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn **, size_t, Arena *) const override
    {
        ++data(place).count;
    }

    void addManyDefaults(
        AggregateDataPtr __restrict place,
        const IColumn ** /*columns*/,
        size_t length,
        Arena * /*arena*/) const override
    {
        data(place).count += length;
    }

    void addBatchSinglePlace(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilter(flags.data(), row_begin, row_end);
        }
        else
        {
            data(place).count += row_end - row_begin;
        }
    }

    void addBatchSinglePlaceNotNull(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        const UInt8 * null_map,
        Arena *,
        ssize_t if_argument_pos) const override
    {
        if (if_argument_pos >= 0)
        {
            const auto & flags = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]).getData();
            data(place).count += countBytesInFilterWithNull(flags, null_map, row_begin, row_end);
        }
        else
        {
            size_t rows = row_end - row_begin;
            data(place).count += rows - countBytesInFilter(null_map, row_begin, row_end);
        }
    }

    bool haveSameStateRepresentationImpl(const IAggregateFunction & rhs) const override
    {
        return this->getName() == rhs.getName();
    }

    DataTypePtr getNormalizedStateType() const override
    {
        /// Return normalized state type: count()
        AggregateFunctionProperties properties;
        return std::make_shared<DataTypeAggregateFunction>(
            AggregateFunctionFactory::instance().get(getName(), NullsAction::EMPTY, {}, {}, properties), DataTypes{}, Array{});
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        data(place).count += data(rhs).count;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        writeVarUInt(data(place).count, buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readVarUInt(data(place).count, buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        assert_cast<ColumnUInt64 &>(to).getData().push_back(data(place).count);
    }

    /// Reset the state to specified value. This function is not the part of common interface.
    static void set(AggregateDataPtr __restrict place, UInt64 new_count)
    {
        data(place).count = new_count;
    }

    AggregateFunctionPtr getOwnNullAdapter(
        const AggregateFunctionPtr &, const DataTypes & types, const Array & params, const AggregateFunctionProperties & /*properties*/) const override;

#if USE_EMBEDDED_COMPILER

    bool isCompilable() const override;
    void compileCreate(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override;
    void compileAdd(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr, const ValuesWithType &) const override;
    void compileMerge(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_dst_ptr, llvm::Value * aggregate_data_src_ptr) const override;
    llvm::Value * compileGetResult(llvm::IRBuilderBase & builder, llvm::Value * aggregate_data_ptr) const override;

#endif
};

}
