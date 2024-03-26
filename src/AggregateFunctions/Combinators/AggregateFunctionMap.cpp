#include <unordered_map>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/Helpers.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include "AggregateFunctionCombinatorFactory.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

template <typename KeyType>
struct AggregateFunctionMapCombinatorData
{
    using SearchType = KeyType;
    std::unordered_map<KeyType, AggregateDataPtr> merged_maps;

    static void writeKey(KeyType key, WriteBuffer & buf) { writeBinaryLittleEndian(key, buf); }
    static void readKey(KeyType & key, ReadBuffer & buf) { readBinaryLittleEndian(key, buf); }
};

template <>
struct AggregateFunctionMapCombinatorData<String>
{
    struct StringHash
    {
        using hash_type = std::hash<std::string_view>;
        using is_transparent = void;

        size_t operator()(std::string_view str) const { return hash_type{}(str); }
    };

    using SearchType = std::string_view;
    std::unordered_map<String, AggregateDataPtr, StringHash, std::equal_to<>> merged_maps;

    static void writeKey(String key, WriteBuffer & buf)
    {
        writeStringBinary(key, buf);
    }
    static void readKey(String & key, ReadBuffer & buf)
    {
        readStringBinary(key, buf);
    }
};

/// Specialization for IPv6 - for historical reasons it should be stored as FixedString(16)
template <>
struct AggregateFunctionMapCombinatorData<IPv6>
{
    struct IPv6Hash
    {
        using hash_type = std::hash<IPv6>;
        using is_transparent = void;

        size_t operator()(const IPv6 & ip) const { return hash_type{}(ip); }
    };

    using SearchType = IPv6;
    std::unordered_map<IPv6, AggregateDataPtr, IPv6Hash, std::equal_to<>> merged_maps;

    static void writeKey(const IPv6 & key, WriteBuffer & buf)
    {
        writeIPv6Binary(key, buf);
    }
    static void readKey(IPv6 & key, ReadBuffer & buf)
    {
        readIPv6Binary(key, buf);
    }
};

template <typename KeyType, bool Plain>
class AggregateFunctionMap final
    : public IAggregateFunctionDataHelper<AggregateFunctionMapCombinatorData<KeyType>, AggregateFunctionMap<KeyType, Plain>>
{
private:
    DataTypePtr key_type;
    AggregateFunctionPtr nested_func;
    size_t arguments_num;

    using Data = AggregateFunctionMapCombinatorData<KeyType>;
    using Base = IAggregateFunctionDataHelper<Data, AggregateFunctionMap<KeyType, Plain>>;

public:
    bool isState() const override
    {
        return nested_func->isState();
    }

    bool isVersioned() const override
    {
        return nested_func->isVersioned();
    }

    size_t getVersionFromRevision(size_t revision) const override
    {
        return nested_func->getVersionFromRevision(revision);
    }

    size_t getDefaultVersion() const override
    {
        return nested_func->getDefaultVersion();
    }

    AggregateFunctionMap(AggregateFunctionPtr nested, const DataTypes & types)
        : Base(types, nested->getParameters(), std::make_shared<DataTypeMap>(DataTypes{getKeyType(types, nested), nested->getResultType()}))
        , nested_func(nested), arguments_num(types.size())
    {
        key_type = getKeyType(types, nested_func);
    }

    String getName() const override { return nested_func->getName() + "Map"; }

    static DataTypePtr getKeyType(const DataTypes & types, const AggregateFunctionPtr & nested)
    {
        if (types.size() == 1)
        {
            const auto * map_type = checkAndGetDataType<DataTypeMap>(types[0].get());
            if (!map_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Aggregate function {}Map requires map as argument", nested->getName());

            return map_type->getKeyType();
        }
        return types.back();
    }

    void addSingleKey(AggregateDataPtr __restrict place, const IColumn & key_column, const IColumn ** value_column, size_t position, Arena * arena) const
    {
        typename Data::SearchType key;
        if constexpr (std::is_same_v<KeyType, String>)
        {
            StringRef key_ref;
            if (key_type->getTypeId() == TypeIndex::FixedString)
                key_ref = assert_cast<const ColumnFixedString &>(key_column).getDataAt(position);
            else if (key_type->getTypeId() == TypeIndex::IPv6)
                key_ref = assert_cast<const ColumnIPv6 &>(key_column).getDataAt(position);
            else
                key_ref = assert_cast<const ColumnString &>(key_column).getDataAt(position);

            key = key_ref.toView();
        }
        else
        {
            key = assert_cast<const ColumnVector<KeyType> &>(key_column).getData()[position];
        }

        AggregateDataPtr nested_place;
        auto it = this->data(place).merged_maps.find(key);
        if (it == this->data(place).merged_maps.end())
        {
            // create a new place for each key
            nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
            nested_func->create(nested_place);
            this->data(place).merged_maps.emplace(key, nested_place);
        } else
            nested_place = it->second;

        nested_func->add(nested_place, value_column, position, arena);
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {

        if constexpr (Plain)
        {
            const auto & key_column = *columns[arguments_num - 1];

            addSingleKey(place, key_column, columns, row_num, arena);

        }
        else
        {

            const auto & map_column = assert_cast<const ColumnMap &>(*columns[0]);
            const auto & map_nested_tuple = map_column.getNestedData();
            const IColumn::Offsets & map_array_offsets = map_column.getNestedColumn().getOffsets();

            const size_t offset = map_array_offsets[row_num - 1];
            const size_t size = (map_array_offsets[row_num] - offset);

            const auto & key_column = map_nested_tuple.getColumn(0);
            const auto & val_column = map_nested_tuple.getColumn(1);

            const IColumn * nested_columns[1] = {&val_column};

            for (size_t i = 0; i < size; ++i)
            {
                addSingleKey(place, key_column, nested_columns, offset + i, arena);
            }

        }
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        const auto & rhs_maps = this->data(rhs).merged_maps;

        for (const auto & elem : rhs_maps)
        {
            const auto & it = merged_maps.find(elem.first);

            AggregateDataPtr nested_place;
            if (it == merged_maps.end())
            {
                // elem.second cannot be copied since this it will be destroyed after merging,
                // and lead to use-after-free.
                nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
                nested_func->create(nested_place);
                merged_maps.emplace(elem.first, nested_place);
            }
            else
            {
                nested_place = it->second;
            }

            nested_func->merge(nested_place, elem.second, arena);
        }
    }

    template <bool up_to_state>
    void destroyImpl(AggregateDataPtr __restrict place) const noexcept
    {
        AggregateFunctionMapCombinatorData<KeyType> & state = Base::data(place);

        for (const auto & [key, nested_place] : state.merged_maps)
        {
            if constexpr (up_to_state)
                nested_func->destroyUpToState(nested_place);
            else
                nested_func->destroy(nested_place);
        }

        state.~Data();
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        destroyImpl<false>(place);
    }

    bool hasTrivialDestructor() const override
    {
        return std::is_trivially_destructible_v<Data> && nested_func->hasTrivialDestructor();
    }

    void destroyUpToState(AggregateDataPtr __restrict place) const noexcept override
    {
        destroyImpl<true>(place);
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /* version */) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        writeVarUInt(merged_maps.size(), buf);

        for (const auto & elem : merged_maps)
        {
            this->data(place).writeKey(elem.first, buf);
            nested_func->serialize(elem.second, buf);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /* version */, Arena * arena) const override
    {
        auto & merged_maps = this->data(place).merged_maps;
        UInt64 size;

        readVarUInt(size, buf);
        for (UInt64 i = 0; i < size; ++i)
        {
            KeyType key;
            AggregateDataPtr nested_place;

            this->data(place).readKey(key, buf);
            nested_place = arena->alignedAlloc(nested_func->sizeOfData(), nested_func->alignOfData());
            nested_func->create(nested_place);
            merged_maps.emplace(key, nested_place);
            nested_func->deserialize(nested_place, buf, std::nullopt, arena);
        }
    }

    template <bool merge>
    void insertResultIntoImpl(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const
    {
        auto & map_column = assert_cast<ColumnMap &>(to);
        auto & nested_column = map_column.getNestedColumn();
        auto & nested_data_column = map_column.getNestedData();

        auto & key_column = nested_data_column.getColumn(0);
        auto & val_column = nested_data_column.getColumn(1);

        auto & merged_maps = this->data(place).merged_maps;

        // sort the keys
        std::vector<KeyType> keys;
        keys.reserve(merged_maps.size());
        for (auto & it : merged_maps)
        {
            keys.push_back(it.first);
        }
        ::sort(keys.begin(), keys.end());

        // insert using sorted keys to result column
        for (auto & key : keys)
        {
            key_column.insert(key);
            if constexpr (merge)
                nested_func->insertMergeResultInto(merged_maps[key], val_column, arena);
            else
                nested_func->insertResultInto(merged_maps[key], val_column, arena);
        }

        IColumn::Offsets & res_offsets = nested_column.getOffsets();
        res_offsets.push_back(val_column.size());
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<false>(place, to, arena);
    }

    void insertMergeResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena * arena) const override
    {
        insertResultIntoImpl<true>(place, to, arena);
    }

    bool allocatesMemoryInArena() const override { return true; }

    AggregateFunctionPtr getNestedFunction() const override { return nested_func; }
};


class AggregateFunctionCombinatorMap final : public IAggregateFunctionCombinator
{
public:
    String getName() const override { return "Map"; }

    DataTypes transformArguments(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Incorrect number of arguments for aggregate function with {} suffix", getName());

        const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get());
        if (map_type)
        {
            if (arguments.size() > 1)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "{} combinator takes only one map argument", getName());

            return DataTypes({map_type->getValueType()});
        }


        // we need this part just to pass to redirection for mapped arrays
        auto check_func = [](DataTypePtr t) { return t->getTypeId() == TypeIndex::Array; };

        const auto * tup_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());
        if (tup_type)
        {
            const auto & types = tup_type->getElements();
            bool arrays_match = arguments.size() == 1 && types.size() >= 2 && std::all_of(types.begin(), types.end(), check_func);
            if (arrays_match)
            {
                const auto * val_array_type = assert_cast<const DataTypeArray *>(types[1].get());
                return DataTypes({val_array_type->getNestedType()});
            }
        }
        else
        {
            bool arrays_match = arguments.size() >= 2 && std::all_of(arguments.begin(), arguments.end(), check_func);
            if (arrays_match)
            {
                const auto * val_array_type = assert_cast<const DataTypeArray *>(arguments[1].get());
                return DataTypes({val_array_type->getNestedType()});
            }
        }

        return DataTypes(arguments.begin(), arguments.end() - 1);
    }

    AggregateFunctionPtr transformAggregateFunction(
        const AggregateFunctionPtr & nested_function,
        const AggregateFunctionProperties &,
        const DataTypes & arguments,
        const Array & params) const override
    {


        auto nested_func_name = nested_function->getName();

        if (nested_func_name == "sum" || nested_func_name == "min" || nested_func_name == "max")
        {
            const auto * tup_type = checkAndGetDataType<DataTypeTuple>(arguments[0].get());

            auto check_func = [](DataTypePtr t) { return t->getTypeId() == TypeIndex::Array; };

            AggregateFunctionProperties out_properties;
            auto & aggr_func_factory = AggregateFunctionFactory::instance();
            auto action = NullsAction::EMPTY;

            if (tup_type)
            {
                const auto & types = tup_type->getElements();
                bool arrays_match = arguments.size() == 1 && types.size() >= 2 && std::all_of(types.begin(), types.end(), check_func);
                if (arrays_match)
                {
                    return aggr_func_factory.get(nested_func_name + "MappedArrays", action, arguments, params, out_properties);
                }
            }
            else
            {
                bool arrays_match = arguments.size() >= 2 && std::all_of(arguments.begin(), arguments.end(), check_func);
                if (arrays_match)
                {
                    return aggr_func_factory.get(nested_func_name + "MappedArrays", action, arguments, params, out_properties);
                }
            }

        }

        const auto * map_type = checkAndGetDataType<DataTypeMap>(arguments[0].get());

        AggregateFunctionPtr res;

        if (map_type)
        {
            const DataTypePtr & key_type = map_type->getKeyType();

            res.reset(createWithNumericBasedType<AggregateFunctionMap, false>(*key_type, nested_function, arguments));
            if (res)
                return res;

            if (key_type->getTypeId() == TypeIndex::FixedString || key_type->getTypeId() == TypeIndex::String)
                return std::make_shared<AggregateFunctionMap<String, false>>(nested_function, arguments);

            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Map key type {} is not is not supported by combinator {}", key_type->getName(), getName());
        }
        else
        {
            const DataTypePtr & key_type = arguments.back();

            res.reset(createWithNumericBasedType<AggregateFunctionMap, true>(*key_type, nested_function, arguments));
            if (res)
                return res;

            if (key_type->getTypeId() == TypeIndex::FixedString || key_type->getTypeId() == TypeIndex::String)
                return std::make_shared<AggregateFunctionMap<String, true>>(nested_function, arguments);

            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Map key type {} is not is not supported by combinator {}", key_type->getName(), getName());
        }
    }
};

}

void registerAggregateFunctionCombinatorMap(AggregateFunctionCombinatorFactory & factory)
{
    factory.registerCombinator(std::make_shared<AggregateFunctionCombinatorMap>());
}

}
