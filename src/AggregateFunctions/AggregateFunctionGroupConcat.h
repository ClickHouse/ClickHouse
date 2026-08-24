#pragma once

#ifndef DB_GROUP_CONCAT_H
#define DB_GROUP_CONCAT_H

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Core/ServerSettings.h>
#include <Common/ArenaAllocator.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{
struct Settings;

struct GroupConcatDataBase
{
    UInt64 data_size = 0;
    UInt64 allocated_size = 0;
    char * data = nullptr;

    void checkAndUpdateSize(UInt64 add, Arena * arena);
    void insertChar(const char * str, UInt64 str_size, Arena * arena);
    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena);
};

struct GroupConcatData : public GroupConcatDataBase
{
    using Offset = UInt64;
    using Allocator = MixedAlignedArenaAllocator<alignof(Offset), 4096>;
    using Offsets = PODArray<Offset, 32, Allocator>;

    Offsets offsets;
    UInt64 num_rows = 0;

    UInt64 getSize(size_t i) const;
    UInt64 getString(size_t i) const;

    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena);
};

template <bool has_limit>
class GroupConcatImpl : public IAggregateFunctionDataHelper<GroupConcatData, GroupConcatImpl<has_limit>>
{
    static constexpr auto name = "groupConcat";

    SerializationPtr serialization;
    UInt64 limit;
    const String delimiter;
    const DataTypePtr type;

public:
    GroupConcatImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 limit_, const String & delimiter_);

    String getName() const override;

    static const std::vector<std::string>& getNameAndAliases()
    {
        static const std::vector<std::string> aliases = {"groupConcat", "group_concat"};
        return aliases;
    }

    void add(AggregateDataPtr place, const IColumn ** columns, size_t row_num, Arena * arena) const override;
    void merge(AggregateDataPtr place, ConstAggregateDataPtr rhs, Arena * arena) const override;
    void serialize(ConstAggregateDataPtr place, WriteBuffer & buf, std::optional<size_t> version) const override;
    void deserialize(AggregateDataPtr place, ReadBuffer & buf, std::optional<size_t> version, Arena * arena) const override;
    void insertResultInto(AggregateDataPtr place, IColumn & to, Arena * arena) const override;

    bool allocatesMemoryInArena() const override;
};

}

#endif
