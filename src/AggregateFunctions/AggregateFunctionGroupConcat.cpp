#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>

#include <Columns/IColumn.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>

#include <Core/ServerSettings.h>
#include <Core/ColumnWithTypeAndName.h>

#include <Common/ArenaAllocator.h>
#include <Common/assert_cast.h>
#include <Interpreters/castColumn.h>

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

struct GroupConcatDataBase
{
    UInt64 data_size = 0;
    UInt64 allocated_size = 0;
    char * data = nullptr;

    void checkAndUpdateSize(UInt64 add, Arena * arena)
    {
        if (data_size + add >= allocated_size)
        {
            auto old_size = allocated_size;
            allocated_size = std::max(2 * allocated_size, data_size + add);
            data = arena->realloc(data, old_size, allocated_size);
        }
    }

    void insertChar(const char * str, UInt64 str_size, Arena * arena)
    {
        checkAndUpdateSize(str_size, arena);
        memcpy(data + data_size, str, str_size);
        data_size += str_size;
    }

    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena)
    {
        WriteBufferFromOwnString buff;
        serialization->serializeText(*column, row_num, buff, FormatSettings{});
        auto string = buff.stringView();
        insertChar(string.data(), string.size(), arena);
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

    /// offset[i * 2] - beginning of the i-th row, offset[i * 2 + 1] - end of the i-th row
    Offsets offsets;
    UInt64 num_rows = 0;

    UInt64 getSize(size_t i) const { return offsets[i * 2 + 1] - offsets[i * 2]; }

    UInt64 getString(size_t i) const { return offsets[i * 2]; }

    void insert(const IColumn * column, const SerializationPtr & serialization, size_t row_num, Arena * arena)
    {
        WriteBufferFromOwnString buff;
        serialization->serializeText(*column, row_num, buff, {});
        auto string = buff.stringView();

        checkAndUpdateSize(string.size(), arena);
        memcpy(data + data_size, string.data(), string.size());
        offsets.push_back(data_size, arena);
        data_size += string.size();
        offsets.push_back(data_size, arena);
        num_rows++;
    }
};

template <bool has_limit>
class GroupConcatImpl final
    : public IAggregateFunctionDataHelper<GroupConcatData<has_limit>, GroupConcatImpl<has_limit>>
{
    static constexpr auto name = "groupConcat";

    SerializationPtr serialization;
    UInt64 limit;
    const String delimiter;
    const DataTypePtr type;

public:
    GroupConcatImpl(const DataTypePtr & data_type_, const Array & parameters_, UInt64 limit_, const String & delimiter_)
        : IAggregateFunctionDataHelper<GroupConcatData<has_limit>, GroupConcatImpl<has_limit>>(
            {data_type_}, parameters_, std::make_shared<DataTypeString>())
        , limit(limit_)
        , delimiter(delimiter_)
        , type(data_type_)
    {
        serialization = isFixedString(type) ? std::make_shared<DataTypeString>()->getDefaultSerialization() : this->argument_types[0]->getDefaultSerialization();
    }

    String getName() const override { return name; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        if constexpr (has_limit)
            if (cur_data.num_rows >= limit)
                return;

        if (cur_data.data_size != 0)
            cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

        if (isFixedString(type))
        {
            ColumnWithTypeAndName col = {columns[0]->getPtr(), type, "column"};
            const auto & col_str = castColumn(col, std::make_shared<DataTypeString>());
            cur_data.insert(col_str.get(), serialization, row_num, arena);
        }
        else
            cur_data.insert(columns[0], serialization, row_num, arena);
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & cur_data = this->data(place);
        auto & rhs_data = this->data(rhs);

        if (rhs_data.data_size == 0)
            return;

        if constexpr (has_limit)
        {
            UInt64 new_elems_count = std::min(rhs_data.num_rows, limit - cur_data.num_rows);
            for (UInt64 i = 0; i < new_elems_count; ++i)
            {
                if (cur_data.data_size != 0)
                    cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

                cur_data.offsets.push_back(cur_data.data_size, arena);
                cur_data.insertChar(rhs_data.data + rhs_data.getString(i), rhs_data.getSize(i), arena);
                cur_data.num_rows++;
                cur_data.offsets.push_back(cur_data.data_size, arena);
            }
        }
        else
        {
            if (cur_data.data_size != 0)
                cur_data.insertChar(delimiter.c_str(), delimiter.size(), arena);

            cur_data.insertChar(rhs_data.data, rhs_data.data_size, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & cur_data = this->data(place);

        writeVarUInt(cur_data.data_size, buf);

        buf.write(cur_data.data, cur_data.data_size);

        if constexpr (has_limit)
        {
            writeVarUInt(cur_data.num_rows, buf);
            for (const auto & offset : cur_data.offsets)
                writeVarUInt(offset, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & cur_data = this->data(place);

        UInt64 temp_size = 0;
        readVarUInt(temp_size, buf);

        cur_data.checkAndUpdateSize(temp_size, arena);

        buf.readStrict(cur_data.data + cur_data.data_size, temp_size);
        cur_data.data_size = temp_size;

        if constexpr (has_limit)
        {
            readVarUInt(cur_data.num_rows, buf);
            cur_data.offsets.resize_exact(cur_data.num_rows * 2, arena);
            for (auto & offset : cur_data.offsets)
                readVarUInt(offset, buf);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & cur_data = this->data(place);

        if (cur_data.data_size == 0)
        {
            to.insertDefault();
            return;
        }

        auto & column_string = assert_cast<ColumnString &>(to);
        column_string.insertData(cur_data.data, cur_data.data_size);
    }

    bool allocatesMemoryInArena() const override { return true; }
};

AggregateFunctionPtr createAggregateFunctionGroupConcat(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    bool has_limit = false;
    UInt64 limit = 0;
    String delimiter;

    if (parameters.size() > 2)
        throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
            "Incorrect number of parameters for aggregate function {}, should be 0, 1 or 2, got: {}", name, parameters.size());

    if (!parameters.empty())
    {
        auto type = parameters[0].getType();
        if (type != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "First parameter for aggregate function {} should be string", name);

        delimiter = parameters[0].safeGet<String>();
    }
    if (parameters.size() == 2)
    {
        auto type = parameters[1].getType();

        if (type != Field::Types::Int64 && type != Field::Types::UInt64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter for aggregate function {} should be a positive number", name);

        if ((type == Field::Types::Int64 && parameters[1].safeGet<Int64>() <= 0) ||
            (type == Field::Types::UInt64 && parameters[1].safeGet<UInt64>() == 0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Second parameter for aggregate function {} should be a positive number, got: {}", name, parameters[1].safeGet<Int64>());

        has_limit = true;
        limit = parameters[1].safeGet<UInt64>();
    }

    if (has_limit)
        return std::make_shared<GroupConcatImpl</* has_limit= */ true>>(argument_types[0], parameters, limit, delimiter);
    return std::make_shared<GroupConcatImpl</* has_limit= */ false>>(argument_types[0], parameters, limit, delimiter);
}

}

void registerAggregateFunctionGroupConcat(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupConcat", { createAggregateFunctionGroupConcat, properties });
    factory.registerAlias("group_concat", "groupConcat", AggregateFunctionFactory::Case::Insensitive);
}

}
