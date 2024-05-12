#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <Interpreters/castColumn.h>
#include <Core/ServerSettings.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <Columns/ColumnNothing.h>
#include <Columns/ColumnString.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include "Core/ColumnWithTypeAndName.h"
#include <Columns/IColumn.h>
#include <DataTypes/IDataType.h>

#include <AggregateFunctions/IAggregateFunction.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct GroupConcatDataBase
{
    UInt64 size = 0;
    UInt64 allocated_size = 0;
    bool is_empty = true;
    char * data = nullptr;

    void checkAndUpdateSize(UInt64 add, Arena * arena)
    {
        if (size + add >= allocated_size)
        {
            auto old_size = allocated_size;
            allocated_size = std::max(2 * allocated_size, size + add);
            data = arena->realloc(data, old_size, allocated_size);
        }
    }

    void insert(const IColumn * column, const DataTypePtr & data_type, size_t row_num, Arena * arena)
    {
        auto casted_column = castColumn({column->getPtr(), data_type, "tmp"}, std::make_shared<DataTypeString>());
        StringRef string = assert_cast<const ColumnString &>(*casted_column).getDataAt(row_num);

        checkAndUpdateSize(string.size, arena);
        memcpy(data + size, string.data, string.size);
        size += string.size;
    }

    void insertStr(const String & str, Arena * arena)
    {
        checkAndUpdateSize(str.size(), arena);
        memcpy(data + size, str.c_str(), str.size());
        size += str.size();
    }

    void insertChar(const char * str, UInt64 cur_size, Arena * arena)
    {
        checkAndUpdateSize(cur_size, arena);
        memcpy(data + size, str, cur_size);
        size += cur_size;
    }
};

template <bool has_limit>
struct GroupConcatData;

template<>
struct GroupConcatData<false> final : public GroupConcatDataBase
{
};

template<>
struct GroupConcatData<true> final : public GroupConcatDataBase
{
    using Offset = UInt64;
    using Allocator = MixedAlignedArenaAllocator<alignof(Offset), 4096>;
    using Offsets = PODArray<Offset, 32, Allocator>;

    Offsets offsets;
    UInt64 count = 0;

    UInt64 getSize(size_t i) const { return offsets[i * 2 + 1] - offsets[i * 2]; }

    UInt64 getString(size_t i) const { return offsets[i * 2]; }

    void insert(const IColumn * column, const DataTypePtr & data_type, size_t row_num, Arena * arena)
    {
        auto casted_column = castColumn({column->getPtr(), data_type, "tmp"}, std::make_shared<DataTypeString>());
        StringRef string = assert_cast<const ColumnString &>(*casted_column).getDataAt(row_num);

        checkAndUpdateSize(string.size, arena);
        memcpy(data + size, string.data, string.size);
        offsets.push_back(size, arena);
        size += string.size;
        offsets.push_back(size, arena);
        count++;
    }
};

template <bool has_limit>
class GroupConcatImpl final
    : public IAggregateFunctionDataHelper<GroupConcatData<has_limit>, GroupConcatImpl<has_limit>>
{
    static constexpr auto name = "groupConcat";
    using Data = GroupConcatData<has_limit>;

    DataTypePtr & data_type;
    UInt64 max_elems;
    const String & delimiter;

public:
    GroupConcatImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 max_elems_, const String & delimiter_)
        : IAggregateFunctionDataHelper<GroupConcatData<has_limit>, GroupConcatImpl<has_limit>>(
            {data_type_}, parameters_, std::make_shared<DataTypeString>())
        , data_type(this->argument_types[0])
        , max_elems(max_elems_)
        , delimiter(delimiter_)
    {
    }

    String getName() const override { return name; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        if constexpr (has_limit)
            if (cur_data.count >= max_elems)
                return;

        if (!cur_data.is_empty)
            cur_data.insertStr(delimiter, arena);

        cur_data.is_empty = false;
        cur_data.insert(columns[0], data_type, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_data = this->data(place);
        auto & rhs_data = this->data(rhs);

        if (rhs_data.is_empty)
            return;

        if constexpr (has_limit)
        {
            UInt64 new_elems_count = std::min(rhs_data.count, max_elems - cur_data.count);
            for (UInt64 i = 0; i < new_elems_count; ++i)
            {
                if (!cur_data.is_empty)
                    cur_data.insertStr(delimiter, arena);

                cur_data.is_empty = false;
                cur_data.offsets.push_back(cur_data.size, arena);
                cur_data.insertChar(rhs_data.data + rhs_data.getString(i), rhs_data.getSize(i), arena);
                cur_data.count++;
                cur_data.offsets.push_back(cur_data.size, arena);
            }
        }
        else
        {
            if (!cur_data.is_empty)
                cur_data.insertStr(delimiter, arena);

            cur_data.is_empty = false;
            cur_data.insertChar(rhs_data.data, rhs_data.size, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & cur_data = this->data(place);

        writeVarUInt(cur_data.size, buf);
        writeVarUInt(cur_data.allocated_size, buf);

        writeBinary(cur_data.is_empty, buf);

        buf.write(cur_data.data, cur_data.size);

        if constexpr (has_limit)
        {
            writeVarUInt(cur_data.count, buf);
            for (const auto & offset : cur_data.offsets)
                writeVarUInt(offset, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        readVarUInt(cur_data.size, buf);
        readVarUInt(cur_data.allocated_size, buf);

        readBinary(cur_data.is_empty, buf);

        buf.readStrict(cur_data.data, cur_data.size);

        if constexpr (has_limit)
        {
            readVarUInt(cur_data.count, buf);
            cur_data.offsets.resize_exact(cur_data.count * 2, arena);
            for (auto & offset : cur_data.offsets)
                readVarUInt(offset, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & cur_data = this->data(place);

        if (cur_data.is_empty)
        {
            auto & column_string = assert_cast<ColumnString &>(to);
            column_string.insert(NULL);
            return;
        }

        auto & column_string = assert_cast<ColumnString &>(to);
        column_string.insertData(cur_data.data, cur_data.size);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

AggregateFunctionPtr createAggregateFunctionGroupConcat(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool has_limit = false;
    UInt64 max_elems = 0;
    String delimiter;

    if (parameters.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of parameters for aggregate function {}, should be 0, 1 or 2", name);

    if (parameters.size() >= 1)
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::String)
               throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First parameter for aggregate function {} should be string", name);

        delimiter = parameters[0].get<String>();
    }
    if (parameters.size() == 2)
    {
        auto type = parameters[1].getType();
        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
               throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Second parameter for aggregate function {} should be positive number", name);

        if ((type == Field::Types::Int64 && parameters[1].get<Int64>() <= 0) ||
            (type == Field::Types::UInt64 && parameters[1].get<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter for aggregate function {} should be positive number", name);

        has_limit = true;
        max_elems = parameters[1].get<UInt64>();
    }

    if (has_limit)
        return std::make_shared<GroupConcatImpl</* has_limit= */ true>>(argument_types[0], parameters, max_elems, delimiter);
    else
        return std::make_shared<GroupConcatImpl</* has_limit= */ false>>(argument_types[0], parameters, max_elems, delimiter);
}

}

void registerAggregateFunctionGroupConcat(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupConcat", { createAggregateFunctionGroupConcat, properties });
    factory.registerAlias("group_concat", "groupConcat", AggregateFunctionFactory::CaseInsensitive);
    factory.registerAlias("string_agg", "groupConcat", AggregateFunctionFactory::CaseInsensitive);
}

}
