#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadHelpersArena.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDate32.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeString.h>

#include <Columns/ColumnArray.h>

#include <Common/HashTable/HashSet.h>
#include <Common/HashTable/HashTableKeyHolder.h>
#include <Common/assert_cast.h>

#include <Core/Field.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/FactoryHelpers.h>
#include <AggregateFunctions/Helpers.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <memory>


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
    using Set = HashSet<T>;

    Set value;
    UInt64 version = 0;
};


/// Puts all values to the hash set. Returns an array of unique values present in all inputs. Implemented for numeric types.
template <typename T>
class AggregateFunctionGroupArrayIntersect
    : public IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>, AggregateFunctionGroupArrayIntersect<T>>
{

private:
    using State = AggregateFunctionGroupArrayIntersectData<T>;

public:
    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, argument_type) {}

    AggregateFunctionGroupArrayIntersect(const DataTypePtr & argument_type, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectData<T>,
          AggregateFunctionGroupArrayIntersect<T>>({argument_type}, parameters_, result_type_) {}

    String getName() const override { return "groupArrayIntersect"; }

    bool allocatesMemoryInArena() const override { return false; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena *) const override
    {
        auto & version = this->data(place).version;
        auto & set = this->data(place).value;

        const auto data_column = assert_cast<const ColumnArray &>(*columns[0]).getDataPtr();
        const auto & offsets = assert_cast<const ColumnArray &>(*columns[0]).getOffsets();
        const size_t offset = offsets[static_cast<ssize_t>(row_num) - 1];
        const auto arr_size = offsets[row_num] - offset;

        ++version;
        if (version == 1)
        {
            for (size_t i = 0; i < arr_size; ++i)
                set.insert(static_cast<T>((*data_column)[offset + i].safeGet<T>()));
        }
        else if (!set.empty())
        {
            typename State::Set new_set;
            for (size_t i = 0; i < arr_size; ++i)
            {
                typename State::Set::LookupResult set_value = set.find(static_cast<T>((*data_column)[offset + i].safeGet<T>()));
                if (set_value != nullptr)
                    new_set.insert(static_cast<T>((*data_column)[offset + i].safeGet<T>()));
            }
            set = std::move(new_set);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena *) const override
    {
        auto & set = this->data(place).value;
        const auto & rhs_set = this->data(rhs).value;

        if (this->data(rhs).version == 0)
            return;

        UInt64 version = this->data(place).version++;
        if (version == 0)
        {
            for (auto & rhs_elem : rhs_set)
                set.insert(rhs_elem.getValue());
            return;
        }

        if (!set.empty())
        {
            auto create_new_set = [](auto & lhs_val, auto & rhs_val)
            {
                typename State::Set new_set;
                for (auto & lhs_elem : lhs_val)
                {
                    auto res = rhs_val.find(lhs_elem.getValue());
                    if (res != nullptr)
                        new_set.insert(lhs_elem.getValue());
                }
                return new_set;
            };
            auto new_set = rhs_set.size() < set.size() ? create_new_set(rhs_set, set) : create_new_set(set, rhs_set);
            set = std::move(new_set);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        auto version = this->data(place).version;

        writeVarUInt(version, buf);
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
            writeIntBinary(elem.getValue(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena *) const override
    {
        auto & set = this->data(place).value;
        auto & version = this->data(place).version;
        size_t size;
        readVarUInt(version, buf);
        readVarUInt(size, buf);
        set.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            T key;
            readIntBinary(key, buf);
            set.insert(key);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();

        const auto & set = this->data(place).value;
        offsets_to.push_back(offsets_to.back() + set.size());

        typename ColumnVector<T>::Container & data_to = assert_cast<ColumnVector<T> &>(arr_to.getData()).getData();
        size_t old_size = data_to.size();
        data_to.resize(old_size + set.size());

        size_t i = 0;
        for (auto it = set.begin(); it != set.end(); ++it, ++i)
            data_to[old_size + i] = it->getValue();
    }
};


/// Generic implementation, it uses serialized representation as object descriptor.
struct AggregateFunctionGroupArrayIntersectGenericData
{
    using Set = HashSet<StringRef>;

    Set value;
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
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData, AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>({input_data_type_}, parameters_, input_data_type_)
        , input_data_type(this->argument_types[0]) {}

    AggregateFunctionGroupArrayIntersectGeneric(const DataTypePtr & input_data_type_, const Array & parameters_, const DataTypePtr & result_type_)
        : IAggregateFunctionDataHelper<AggregateFunctionGroupArrayIntersectGenericData, AggregateFunctionGroupArrayIntersectGeneric<is_plain_column>>({input_data_type_}, parameters_, result_type_)
        , input_data_type(result_type_) {}

    String getName() const override { return "groupArrayIntersect"; }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        auto & version = this->data(place).version;
        bool inserted;
        State::Set::LookupResult it;

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
                    set.emplace(ArenaKeyHolder{data_column->getDataAt(offset + i), *arena}, it, inserted);
                else
                {
                    const char * begin = nullptr;
                    StringRef serialized = data_column->serializeValueIntoArena(offset + i, *arena, begin);
                    chassert(serialized.data != nullptr);
                    set.emplace(SerializedKeyHolder{serialized, *arena}, it, inserted);
                }
            }
        }
        else if (!set.empty())
        {
            typename State::Set new_set;
            for (size_t i = 0; i < arr_size; ++i)
            {
                if constexpr (is_plain_column)
                {
                    it = set.find(data_column->getDataAt(offset + i));
                    if (it != nullptr)
                        new_set.emplace(ArenaKeyHolder{data_column->getDataAt(offset + i), *arena}, it, inserted);
                }
                else
                {
                    const char * begin = nullptr;
                    StringRef serialized = data_column->serializeValueIntoArena(offset + i, *arena, begin);
                    chassert(serialized.data != nullptr);
                    it = set.find(serialized);

                    if (it != nullptr)
                        new_set.emplace(SerializedKeyHolder{serialized, *arena}, it, inserted);
                }
            }
            set = std::move(new_set);
        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        const auto & rhs_value = this->data(rhs).value;

        if (this->data(rhs).version == 0)
            return;

        UInt64 version = this->data(place).version++;
        if (version == 0)
        {
            bool inserted;
            State::Set::LookupResult it;
            for (auto & rhs_elem : rhs_value)
            {
                set.emplace(ArenaKeyHolder{rhs_elem.getValue(), *arena}, it, inserted);
            }
        }
        else if (!set.empty())
        {
            auto create_new_map = [](auto & lhs_val, auto & rhs_val)
            {
                typename State::Set new_map;
                for (auto & lhs_elem : lhs_val)
                {
                    auto val = rhs_val.find(lhs_elem.getValue());
                    if (val != nullptr)
                        new_map.insert(lhs_elem.getValue());
                }
                return new_map;
            };
            auto new_map = create_new_map(set, rhs_value);
            set = std::move(new_map);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & set = this->data(place).value;
        auto & version = this->data(place).version;
        writeVarUInt(version, buf);
        writeVarUInt(set.size(), buf);

        for (const auto & elem : set)
            writeStringBinary(elem.getValue(), buf);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & set = this->data(place).value;
        auto & version = this->data(place).version;
        size_t size;
        readVarUInt(version, buf);
        readVarUInt(size, buf);
        set.reserve(size);
        for (size_t i = 0; i < size; ++i)
        {
            auto key = readStringBinaryInto(*arena, buf);
            set.insert(key);
        }
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        ColumnArray & arr_to = assert_cast<ColumnArray &>(to);
        ColumnArray::Offsets & offsets_to = arr_to.getOffsets();
        IColumn & data_to = arr_to.getData();

        auto & set = this->data(place).value;

        offsets_to.push_back(offsets_to.back() + set.size());

        for (auto & elem : set)
        {
            if constexpr (is_plain_column)
                data_to.insertData(elem.getValue().data, elem.getValue().size);
            else
                std::ignore = data_to.deserializeAndInsertFromArena(elem.getValue().data);
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
    if (which.idx == TypeIndex::DateTime)
        return new AggregateFunctionGroupArrayIntersectDateTime(argument_type, parameters);
    if (which.idx == TypeIndex::Date32)
        return new AggregateFunctionGroupArrayIntersectDate32(argument_type, parameters);
    if (which.idx == TypeIndex::DateTime64)
    {
        const auto * datetime64_type = dynamic_cast<const DataTypeDateTime64 *>(argument_type.get());
        const auto return_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeDateTime64>(datetime64_type->getScale()));

        return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_type, parameters, return_type);
    }

    /// Check that we can use plain version of AggregateFunctionGroupArrayIntersectGeneric
    if (argument_type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
        return new AggregateFunctionGroupArrayIntersectGeneric<true>(argument_type, parameters);
    return new AggregateFunctionGroupArrayIntersectGeneric<false>(argument_type, parameters);
}

inline AggregateFunctionPtr createAggregateFunctionGroupArrayIntersectImpl(const std::string & name, const DataTypePtr & argument_type, const Array & parameters)
{
    const auto & nested_type = dynamic_cast<const DataTypeArray &>(*argument_type).getNestedType();
    AggregateFunctionPtr res(createWithNumericType<AggregateFunctionGroupArrayIntersect, const DataTypePtr &>(*nested_type, argument_type, parameters));
    if (!res)
    {
        res = AggregateFunctionPtr(createWithExtraTypes(argument_type, parameters));
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
