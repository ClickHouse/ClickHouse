#include <cassert>
#include <memory>

#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashMap.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/KeyHolderHelpers.h>

#include <Core/Field.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}
struct Settings;


template <typename T>
struct AggregateFunctionGroupArrayIntersectData
{
    using Map = HashMap<T, UInt64>;
    Map value;
    UInt64 version = 0;
};


/// Puts all values to the hash set. Returns an array of unique values. Implemented for numeric types.
template <typename T>
class AggregateFunctionGroupArrayIntersect
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>, AggregateFunctionGroupArrayIntersect<T>>
{

private:
    using State = AggregateFunctionGroupArrayIntersectData<T>;

public:
    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, std::make_shared<DataTypeArray>(argument_type)) {}

    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, result_type_) {}

    String getName() const override { return "GroupArrayIntersect"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & version = this->data(place).version;
        auto & map = this->data(place).value;

        const auto data_column = assert_cast<const ColumnArray &>(*columns[0]).getDataPtr();
        const auto & offsets = assert_cast<const ColumnArray &>(*columns[0]).getOffsets();
        const size_t offset = offsets[static_cast<ssize_t>(row_num) - 1];
        const auto arr_size = offsets[row_num] - offset;

        ++version;
        if (version == 1)
        {
            for (size_t i = 0; i < arr_size; ++i)
                map[static_cast<T>((*data_column)[offset + i].get<T>())] = version;
        }
        else if (map.size() > 0)
        {
            typename State::Map new_map;
            for (size_t i = 0; i < arr_size; ++i)
            {
                typename State::Map::LookupResult value = map.find(static_cast<T>((*data_column)[offset + i].get<T>()));
                if (value != nullptr && value->getMapped() == version - 1)
                    new_map[static_cast<T>((*data_column)[offset + i].get<T>())] = version;
            }
            map = std::move(new_map);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & map = this->data(place).value;
        const auto & rhs_map = this->data(rhs).value;

        if (this->data(rhs).version == 0)
            return;

        UInt64 version = this->data(place).version++;
        if (version == 0)
        {
            for (auto & rhs_elem : rhs_map)
                map[rhs_elem.getKey()] = 1;
            return;
        }

        if (map.size() > 0)
        {
            typename State::Map new_map;
            if (rhs_map.size() < map.size())
            {
                for (auto & rhs_elem : rhs_map)
                {
                    auto value = map.find(rhs_elem.getKey());
                    if (value != nullptr)
                        new_map[rhs_elem.getKey()] = version;
                }
            }
            else
            {
                for (auto & elem : map)
                {
                    auto value = rhs_map.find(elem.getKey());
                    if (value != nullptr)
                        new_map[elem.getKey()] = version;
                }
            }
            map = std::move(new_map);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).value;
        auto version = this->data(place).version;

        writeVarUInt(version, buf);
        writeVarUInt(map.size(), buf);

        for (const auto & elem : map)
        {
            writeIntBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        readVarUInt(this->data(place).version, buf);
        this->data(place).value.read(buf);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & map = this->data(place).value;

        offsets_to.push_back(offsets_to.back() + map.size());

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + map.size());

        size_t i = 0;
        for (auto it = map.begin(); it != map.end(); ++it)
            data_to[old_size + i++] = it->getKey();
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupArrayIntersectGenericData
{
    using Map = HashMap<StringRef, UInt64>;

    Map value;
    UInt64 version = 0;
};

/** Template parameter with true value should be used for columns that store their elements in memory continuously.
 *  For such columns GroupArrayIntersect() can be implemented more efficiently (especially for small numeric arrays).
 */
template <bool is_plain_column = false>
class AggregateFunctionGroupArrayIntersectGeneric
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData,
        AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>
{
    const DataTypePtr & input_data_type;

    using State = AggregateFunctionGroupArrayIntersectGenericData;

public:
    AggregateFunctionGroupArrayIntersectGeneric(const DataTypePtr & input_data_type_, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData, AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>({input_data_type_}, parameters_, std::make_shared<DataTypeArray>(input_data_type_))
        , input_data_type(this->argument_types[0]) {}

    AggregateFunctionGroupArrayIntersectGeneric(const DataTypePtr & input_data_type_, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData, AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>({input_data_type_}, parameters_, result_type_)
        , input_data_type(result_type_) {}

    String getName() const override { return "GroupArrayIntersect"; }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        bool inserted;
        State::Map::LookupResult it;

        const auto data_column = assert_cast<const ColumnArray &>(*columns[0]).getDataPtr();
        const auto & offsets = assert_cast<const ColumnArray &>(*columns[0]).getOffsets();
        const size_t offset = offsets[static_cast<ssize_t>(row_num) - 1];
        const auto arr_size = offsets[row_num] - offset;

        ++version;
        if (version == 1)
        {
            for (size_t i = 0; i < arr_size; ++i)
            {
                if constexpr (is_plain_column)
                    map.emplace(ArenaKeyHolder{data_column->getDataAt(offset + i), *arena}, it, inserted);
                else
                {
                    const char * begin = nullptr;
                    StringRef serialized = data_column->serializeValueIntoArena(offset + i, *arena, begin);
                    assert(serialized.data != nullptr);
                    map.emplace(SerializedKeyHolder{serialized, *arena}, it, inserted);
                }

                if (inserted)
                    new (&it->getMapped()) UInt64(version);
            }
        }
        else if (map.size() > 0)
        {
            typename State::Map new_map;
            for (size_t i = 0; i < arr_size; ++i)
            {
                if constexpr (is_plain_column)
                {
                    it = map.find(data_column->getDataAt(offset + i));
                    if (it != nullptr)
                        new_map.emplace(ArenaKeyHolder{data_column->getDataAt(offset + i), *arena}, it, inserted);
                }
                else
                {
                    const char * begin = nullptr;
                    StringRef serialized = data_column->serializeValueIntoArena(offset + i, *arena, begin);
                    assert(serialized.data != nullptr);
                    it = map.find(serialized);

                    if (it != nullptr)
                        new_map.emplace(SerializedKeyHolder{serialized, *arena}, it, inserted);
                }
            }
            map = std::move(new_map);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & map = this->data(place).value;
        const auto & rhs_map = this->data(rhs).value;

        if (this->data(rhs).version == 0)
            return;

        UInt64 version = this->data(place).version++;
        if (version == 0)
        {
            bool inserted;
            State::Map::LookupResult it;
            for (auto & rhs_elem : rhs_map)
            {
                map[rhs_elem.getKey()] = 1;
                map.emplace(ArenaKeyHolder{rhs_elem.getKey(), *arena}, it, inserted);
                if (inserted)
                    new (&it->getMapped()) UInt64(version);
            }
        }
        else if (map.size() > 0)
        {
            typename State::Map new_map;
            if (rhs_map.size() < map.size())
            {
                for (auto & rhs_elem : rhs_map)
                {
                    auto value = map.find(rhs_elem.getKey());
                    if (value != nullptr)
                        new_map[rhs_elem.getKey()] = version;
                }
            }
            else
            {
                for (auto & elem : map)
                {
                    auto value = rhs_map.find(elem.getKey());
                    if (value != nullptr)
                        new_map[elem.getKey()] = version;
                }
            }
            map = std::move(new_map);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        writeVarUInt(version, buf);
        writeVarUInt(map.size(), buf);

        for (const auto & elem : map)
        {
            writeStringBinary(elem.getKey(), buf);
            writeVarUInt(elem.getMapped(), buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & map = this->data(place).value;
        auto & version = this->data(place).version;
        size_t size;
        readVarUInt(version, buf);
        readVarUInt(size, buf);
        map.reserve(size);
        UInt64 elem_version;
        for (size_t i = 0; i < size; ++i)
        {
            auto key = readStringBinaryInto(*arena, buf);
            readVarUInt(elem_version, buf);
            if (elem_version == version)
                map[key] = version;
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & map = this->data(place).value;

        offsets_to.push_back(offsets_to.back() + map.size());

        for (auto & elem : map)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.getKey().data, elem.getKey().size);
            else
                std::ignore = data_to.deserializeAndInsertFromArena(elem.getKey().data);
        }
    }
};

namespace
{

/// Substitute return type for Date and DateTime
class AggregateFunctionGroupArrayIntersectDate : public AggregateFunctionGroupArrayIntersect<DataTypeDate::FieldType>
{
public:
    explicit AggregateFunctionGroupArrayIntersectDate(const DataTypePtr & argument_type, const Array & parameters_)
        : AggregateFunctionGroupArrayIntersect<DataTypeDate::FieldType>(argument_type, parameters_, createResultType()) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate>()); }
};

class AggregateFunctionGroupArrayIntersectDateTime : public AggregateFunctionGroupArrayIntersect<DataTypeDateTime::FieldType>
{
public:
    explicit AggregateFunctionGroupArrayIntersectDateTime(const DataTypePtr & argument_type, const Array & parameters_)
        : AggregateFunctionGroupArrayIntersect<DataTypeDateTime::FieldType>(argument_type, parameters_, createResultType()) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime>()); }
};

class AggregateFunctionGroupArrayIntersectDate32 : public AggregateFunctionGroupArrayIntersect<DataTypeDate32::FieldType>
{
public:
    explicit AggregateFunctionGroupArrayIntersectDate32(const DataTypePtr & argument_type, const Array & parameters_)
        : AggregateFunctionGroupArrayIntersect<DataTypeDate32::FieldType>(argument_type, parameters_, createResultType()) {}
    static DataTypePtr createResultType() { return std::make_shared<DataTypeArray>(std::make_shared<DataTypeDate32>()); }
};

IAggregateFunction * createWithExtraTypes(const DataTypePtr & argument_type, const Array & parameters)
{
    WhichDataType which(argument_type);
    if (which.idx == TypeIndex::Date) return new AggregateFunctionGroupArrayIntersectDate(argument_type, parameters);
    else if (which.idx == TypeIndex::DateTime) return new AggregateFunctionGroupArrayIntersectDateTime(argument_type, parameters);
    else if (which.idx == TypeIndex::Date32) return new AggregateFunctionGroupArrayIntersectDate32(argument_type, parameters);
    else if (which.idx == TypeIndex::DateTime64)
    {
        const auto * datetime64_type = dynamic_cast<const DataTypeDateTime64 *>(argument_type.get());
        const auto return_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime64>(datetime64_type->getScale()));

        return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_type, parameters, return_type);
    }
    else
    {
        /// Check that we can use plain version of AggregateFunctionGroupArrayIntersectGeneric
        if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
            return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_type, parameters);
        else
            return new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_type, parameters);
    }
}

inline AggregateFunctionPtr createAggregateFunctionGroupArrayIntersectImpl(const std::string & name, const DataTypePtr & argument_type, const Array & parameters)
{
    const auto & nested_type = dynamic_cast<const DataTypeArray &>(*argument_type).getNestedType();
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupArrayIntersect, const DataTypePtr &>(*nested_type, nested_type, parameters));
    if (!res)
    {
        res = AggregateFunctionPtr(createWithExtraTypes(nested_type, parameters));
    }

    if (!res)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument for aggregate function {}",
                        argument_type->getName(), name);

    return res;
}

AggregateFunctionPtr createAggregateFunctionGroupArrayIntersect(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    assertUnary(name, argument_types);

    if (!WhichDataType(argument_types.at(0)).isArray())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Aggregate function groupArrayIntersect accepts only array type argument.");

    if (!parameters.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Incorrect number of parameters for aggregate function {}, should be 0", name);

    return createAggregateFunctionGroupArrayIntersectImpl(name, argument_types[0], parameters);
}

}

void registerAggregateFunctionGroupArrayIntersect(AggregateFunctionFactory & factory)
{
    AggregateFunctionProperties properties = { .returns_default_when_only_null = false, .is_order_dependent = true };

    factory.registerFunction("groupArrayIntersect", { createAggregateFunctionGroupArrayIntersect, properties });
}

}
